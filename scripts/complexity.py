#!/usr/bin/env python3
"""Step 05: how hard each workbook is to convert to Sigma (complexity and predicted parity).

Usage:
    python scripts/complexity.py --site <site> [--workers N] [--force] [--timeout 300]

Runs Sigma's own gap scanner (vendor/tableau-to-sigma, Apache 2.0) on every workbook
downloaded by step 04. The scanner finds each Tableau feature a workbook uses and its
conversion tier:

    auto       converted automatically                          weight 1.0
    hint       converted, with a formula to verify              weight 0.85
    manual     reachable, but needs hand work after publishing  weight 0.5
    unhandled  likely lost: escalation or silent gap            weight 0.0

Predicted parity = the occurrence-weighted share of features that convert, 100% for a
workbook with no complex features. Bands: A >= 90 (drop-in), B 75-90 (light touch-up),
C 50-75 (meaningful manual work), D < 50 (hard, re-author risk). It is a prediction from
the workbook file, not a measured result.

Needs Node.js on PATH (or NODE_BIN) for the scanner's formula audit. Without it the
scanner counts every formula as unhandled, so parity would be far too low: this step
refuses to run unless --allow-no-node is passed, and then marks every result
formula_audit=unavailable.

Resumable: a workbook whose .twb has not changed since its last scan is skipped.

Writes to output/<site>/:
  gaps/<luid>/                  the scanner's own reports per workbook
  complexity.json / .csv        per workbook: feature counts by tier, predicted parity and
                                band, field statistics, top features
"""

import argparse
import csv
import json
import os
import shutil
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib.tableau_client import REPO_ROOT, add_site_args, log, resolve_site_args  # noqa: E402

SCANNER = REPO_ROOT / "vendor" / "tableau-to-sigma" / "scripts" / "scan-workbook-gaps.py"
WEIGHTS = {"auto": 1.0, "hint": 0.85, "manual": 0.5, "unhandled": 0.0}
AUDIT_UNAVAILABLE = "Converter-backed formula audit unavailable"


def predicted_parity(features):
    total = weighted = 0
    for f in features:
        n = int(f.get("count") or 1)
        total += n
        weighted += n * WEIGHTS.get(f.get("status"), 0.0)
    if not total:
        return 100.0, "A"
    pct = round(weighted / total * 100, 1)
    band = "A" if pct >= 90 else "B" if pct >= 75 else "C" if pct >= 50 else "D"
    return pct, band


def node_available():
    node = os.environ.get("NODE_BIN") or shutil.which("node")
    if not node:
        return None
    try:
        v = subprocess.run([node, "--version"], capture_output=True, encoding="utf-8", errors="replace",
                           timeout=20)
        return v.stdout.strip() if v.returncode == 0 else None
    except (OSError, subprocess.TimeoutExpired):
        return None


def scan_one(luid, twb, report_dir, timeout):
    report_dir.mkdir(parents=True, exist_ok=True)
    md = report_dir / f"{luid}-gaps-report.md"
    # -X utf8: on Windows the scanner would otherwise decode the Node helper's UTF-8 output
    # with the ANSI code page and fail on formulas with characters outside it.
    proc = subprocess.run([sys.executable, "-I", "-X", "utf8", str(SCANNER), str(twb), str(md)],
                          capture_output=True, encoding="utf-8", errors="replace", timeout=timeout)
    js = md.with_suffix(".json")
    if proc.returncode or not js.exists():
        detail = (proc.stderr or proc.stdout or "").strip().splitlines()
        raise RuntimeError(f"scanner exit {proc.returncode}: {detail[-1] if detail else 'no output'}"[:300])
    return json.loads(js.read_text(encoding="utf-8"))


def summarize(info, gaps, twb):
    feats = gaps.get("detected_features") or []
    audit_down = any(f.get("name") == AUDIT_UNAVAILABLE for f in feats)
    fs = gaps.get("field_statistics") or {}
    by = {k: sum(1 for f in feats if f.get("status") == k) for k in WEIGHTS}
    occ = {k: sum(int(f.get("count") or 1) for f in feats if f.get("status") == k) for k in WEIGHTS}
    pct, band = predicted_parity(feats)
    rank = {"unhandled": 0, "manual": 1}
    top = sorted(feats, key=lambda f: (rank.get(f.get("status"), 2), -int(f.get("count") or 1)))
    return {
        "name": info.get("name"), "project": info.get("project"), "twb_size_kb": round(twb.stat().st_size / 1024),
        "n_features": len(feats), **{f"n_{k}": v for k, v in by.items()},
        **{f"occ_{k}": v for k, v in occ.items()},
        "predicted_parity_pct": pct, "parity_band": band,
        "formula_audit": "unavailable" if audit_down else "ok",
        "total_fields": fs.get("total_fields"), "unused_fields": fs.get("unused_fields"),
        "pct_used": fs.get("pct_used"), "calc_nested": fs.get("calc_nested"), "calc_lod": fs.get("calc_lod"),
        "orphan_worksheets": len(fs.get("orphan_worksheets") or []),
        "top_gaps": "; ".join(f"{f['name']} ({f['status']}, {f.get('count')})" for f in top
                               if f.get("status") in ("unhandled", "manual"))[:500],
        "features": [{"name": f.get("name"), "status": f.get("status"), "count": f.get("count")} for f in feats],
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    add_site_args(ap)
    ap.add_argument("--workers", type=int, default=max(1, min(8, (os.cpu_count() or 2) - 1)))
    ap.add_argument("--force", action="store_true", help="rescan every workbook")
    ap.add_argument("--timeout", type=int, default=300, help="seconds per workbook")
    ap.add_argument("--allow-no-node", action="store_true",
                    help="run without Node.js; formula features then count as unhandled (parity too low)")
    args = ap.parse_args()
    site, out = resolve_site_args(args)

    if not SCANNER.exists():
        sys.exit(f"Scanner not found at {SCANNER}; the vendor/ folder is part of the repo")
    fetched = out / "twb-fetch-results.json"
    if not fetched.exists():
        sys.exit(f"missing {fetched}; run step 04 (fetch_workbooks.py) first")
    node = node_available()
    if node:
        log(f"Node.js {node}: formula audit enabled")
    elif not args.allow_no_node:
        sys.exit("Node.js not found (set NODE_BIN or put node on PATH). Without it every formula counts as "
                 "unhandled and predicted parity is far too low. Install Node, or pass --allow-no-node.")
    else:
        log("WARNING: running without Node.js; results are marked formula_audit=unavailable")

    results_in = json.loads(fetched.read_text(encoding="utf-8"))
    prev_path = out / "complexity.json"
    prev = json.loads(prev_path.read_text(encoding="utf-8")) if prev_path.exists() else {}
    gaps_root = out / "gaps"
    jobs, results, failed = [], {}, {}
    for luid, info in results_in.items():
        twb = out / "twbs" / f"{luid}.twb"
        if info.get("error") or not twb.exists():
            continue
        js = gaps_root / luid / f"{luid}-gaps-report.json"
        fresh = js.exists() and js.stat().st_mtime >= twb.stat().st_mtime
        if fresh and not args.force and luid in prev and (prev[luid].get("formula_audit") == "ok" or not node):
            results[luid] = prev[luid]
        else:
            jobs.append((luid, info, twb))
    log(f"{len(results) + len(jobs)} workbooks: {len(results)} unchanged since last scan, {len(jobs)} to scan "
        f"({args.workers} workers)")

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(scan_one, luid, twb, gaps_root / luid, args.timeout): (luid, info, twb)
                for luid, info, twb in jobs}
        for i, fut in enumerate(as_completed(futs), 1):
            luid, info, twb = futs[fut]
            try:
                results[luid] = summarize(info, fut.result(), twb)
            except (RuntimeError, subprocess.TimeoutExpired, ValueError, OSError) as e:
                failed[luid] = str(e)[:300]
                log(f"  scan failed for {info.get('name')}: {failed[luid]}")
            if i % 50 == 0 or i == len(jobs):
                log(f"  scanned {i}/{len(jobs)}")

    prev_path.write_text(json.dumps(results, indent=2))
    cols = ["luid", "name", "project", "parity_band", "predicted_parity_pct", "n_features", "n_auto", "n_hint",
            "n_manual", "n_unhandled", "occ_unhandled", "occ_manual", "formula_audit", "total_fields",
            "unused_fields", "pct_used", "calc_nested", "calc_lod", "orphan_worksheets", "twb_size_kb", "top_gaps"]
    with open(out / "complexity.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore", lineterminator="\n")
        w.writeheader()
        for luid, r in sorted(results.items(), key=lambda x: x[1]["predicted_parity_pct"]):
            w.writerow({"luid": luid, **r})
    (out / "complexity-failures.json").write_text(json.dumps(failed, indent=2))
    bands = {b: sum(1 for r in results.values() if r["parity_band"] == b) for b in "ABCD"}
    log(f"Wrote complexity.json / .csv: {len(results)} workbooks; bands A={bands['A']} B={bands['B']} "
        f"C={bands['C']} D={bands['D']}; {len(failed)} failed (complexity-failures.json)")


if __name__ == "__main__":
    main()
