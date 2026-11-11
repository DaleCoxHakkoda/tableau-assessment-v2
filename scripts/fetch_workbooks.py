#!/usr/bin/env python3
"""Step 04: download every workbook's .twb (the XML that defines it), for complexity
scoring (step 05) and consolidation (step 12).

Usage:
    python scripts/fetch_workbooks.py --site <site> [--threads 8] [--limit N] [--force]
                                      [--exclude-projects "Personal Space"]

What it does:
  * Lists every workbook on the site (REST) and downloads each one WITHOUT extract data
    (includeExtract=false), so a site of thousands of workbooks stays small.
  * A .twbx (packaged workbook) is unzipped in memory and only its .twb is kept.
  * Resumable: a workbook already on disk and unchanged since (same updatedAt) is
    skipped; one republished since the last download is fetched again. --force
    refetches everything.
  * Threads share one PAT session (a PAT holds one session at a time); if it expires,
    one thread signs in again and the rest reuse it. 429/5xx and dropped connections
    back off and retry.

Writes to output/<site>/:
  twbs/<workbook luid>.twb      the workbook XML
  twb-fetch-results.json        per workbook: name, project, owner, updated_at, size, kind,
                                file, error (if any)
"""

import argparse
import io
import json
import queue
import sys
import threading
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib.tableau_client import (TableauClient, TableauError, add_site_args, log,  # noqa: E402
                                resolve_site_args)


def twb_from_bytes(data):
    """(xml bytes, kind) from a download that is either a .twb or a .twbx (zip)."""
    if data[:2] != b"PK":
        return data, "twb"
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        members = [n for n in zf.namelist() if n.lower().endswith(".twb") and not n.endswith("/")]
        if not members:
            raise ValueError("packaged workbook contains no .twb")
        # The workbook itself sits at the top level; nested .twb files are rare leftovers.
        members.sort(key=lambda n: (n.count("/"), len(n)))
        return zf.read(members[0]), "twbx"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    add_site_args(ap)
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--limit", type=int, default=None, help="only the first N workbooks (a sanity pass)")
    ap.add_argument("--force", action="store_true", help="download everything again")
    ap.add_argument("--exclude-projects", default="",
                    help="comma-separated project names to skip, e.g. \"Personal Space\" (default: none)")
    args = ap.parse_args()
    site, out = resolve_site_args(args)
    twb_dir = out / "twbs"
    twb_dir.mkdir(parents=True, exist_ok=True)
    results_path = out / "twb-fetch-results.json"
    previous = json.loads(results_path.read_text(encoding="utf-8")) if results_path.exists() else {}
    excluded = {p.strip() for p in args.exclude_projects.split(",") if p.strip()}

    with TableauClient(site) as client:
        log("Listing workbooks")
        wbs = list(client.rest_paged("/workbooks", "workbooks", "workbook", page_size=100))
        if excluded:
            before = len(wbs)
            wbs = [w for w in wbs if (w.get("project") or {}).get("name") not in excluded]
            log(f"Excluded {before - len(wbs)} workbooks in {', '.join(sorted(excluded))}")
        if args.limit:
            wbs = wbs[:args.limit]

        results, todo = {}, []
        for w in wbs:
            luid = w["id"]
            rec = {"name": w.get("name"), "project": (w.get("project") or {}).get("name"),
                   "owner_id": (w.get("owner") or {}).get("id"), "updated_at": w.get("updatedAt"),
                   "content_url": w.get("contentUrl")}
            prev = previous.get(luid) or {}
            f = twb_dir / f"{luid}.twb"
            if (not args.force and f.exists() and f.stat().st_size > 0 and not prev.get("error")
                    and prev.get("updated_at") == rec["updated_at"]):
                results[luid] = {**prev, **rec, "cached": True}
            else:
                results[luid] = rec
                todo.append(luid)
        cached = len(wbs) - len(todo)
        log(f"{len(wbs)} workbooks: {cached} unchanged on disk, {len(todo)} to download "
            f"({args.threads} threads)")

        q = queue.Queue()
        for luid in todo:
            q.put(luid)
        lock = threading.Lock()
        done = [0]
        start = time.time()

        def worker():
            while True:
                try:
                    luid = q.get_nowait()
                except queue.Empty:
                    return
                rec = results[luid]
                try:
                    data = client.request_bytes("GET", f"/api/{client.version}/sites/{client.site_id}"
                                                       f"/workbooks/{luid}/content?includeExtract=false")
                    xml, kind = twb_from_bytes(data)
                    (twb_dir / f"{luid}.twb").write_bytes(xml)
                    upd = {"size": len(xml), "download_size": len(data), "kind": kind,
                           "file": f"twbs/{luid}.twb", "error": None, "cached": False}
                except (TableauError, ValueError, zipfile.BadZipFile, OSError) as e:
                    upd = {"error": str(e)[:300], "cached": False}
                with lock:
                    rec.update(upd)
                    done[0] += 1
                    n = done[0]
                    if upd.get("error"):
                        log(f"  ERROR {rec.get('name')}: {upd['error'][:150]}")
                    if n % 25 == 0 or n == len(todo):
                        rate = n / max(time.time() - start, 0.01)
                        eta = (len(todo) - n) / max(rate, 0.01) / 60
                        log(f"  {n}/{len(todo)}  {rate:.1f}/s  eta {eta:.1f} min")

        threads = [threading.Thread(target=worker, daemon=True) for _ in range(max(1, args.threads))]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    meta_errors = sum(1 for r in results.values() if r.get("error"))
    results_path.write_text(json.dumps(results, indent=2))
    log(f"Wrote {len(results) - meta_errors} workbooks to {twb_dir} "
        f"({meta_errors} errors, {cached} unchanged); results: {results_path}")
    (out / "twb-fetch-results.meta.json").write_text(json.dumps(
        {"fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
         "workbooks": len(results), "errors": meta_errors, "excluded_projects": sorted(excluded)}, indent=2))
    sys.exit(1 if results and meta_errors == len(results) else 0)


if __name__ == "__main__":
    try:
        main()
    except TableauError as e:
        sys.exit(f"ERROR: {e}")
