#!/usr/bin/env python3
"""Step 02b: import the Traffic to Views admin-view export (dated, per-user history).

Usage:
    python scripts/import_traffic_to_views.py --site <site>              # match views via saved snapshots
    python scripts/import_traffic_to_views.py --site <site> --fetch-map  # one sign-in for exact matching

Input: every CSV in output/<site>/traffic-to-views/ (split by month if the
download is large; overlapping rows across files are de-duplicated).

Export it from Tableau, NOT through Excel: Excel writes '########' for any date
column too narrow to display and saves that text into the CSV, which destroys
the timestamps. Download Full Data -> CSV and copy the file in as-is. This script
stops with an explanation if it finds those placeholders.

Matching to views (reported in the output):
  1. url      Repository Url (e.g. Workbook/sheets/View) = the REST view contentUrl.
              Exact. Needs content_url in the latest usage snapshot (step 02 adds it
              from now on) or --fetch-map.
  2. names    project + workbook + view name, then workbook + view name. Names are
              as of the event, so renamed content can fail to match.
Unmatched views (typically deleted since) are listed in traffic-unmatched.csv; that
content had users, which is worth knowing even though it is gone.

Writes to output/<site>/:
  traffic-usage.json / .csv   per view LUID: accesses, distinct users, first/last access,
                              active weeks, monthly counts, top users
  traffic-unmatched.csv       views in the export with no current match
build_report_inventory.py (step 10) joins traffic-usage.json into reports.csv as traffic_* columns.
"""

import argparse
import csv
import io
import json
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib.tableau_client import (TableauClient, TableauError, add_site_args, log,  # noqa: E402
                                resolve_site_args, site_folder)

# Candidate column names, matched case-insensitively, first found wins.
COLS = {
    "url": ["Repository Url (Hist Views)", "Repository Url", "repository_url"],
    "time": ["Created At", "created_at", "created_at_local", "Event Date"],
    "action": ["Action", "action_views", "Action Type", "Event Type"],
    "user_id": ["User Id", "user_id", "action_users"],
    "user": ["Email", "User Name", "user_name", "Display Name"],
    "workbook": ["Name (Hist Workbooks)", "Workbook Name", "Get Names"],
    "view": ["Name (Hist Views)", "View Name", "Item Name"],
    "project": ["name (hist_projects)", "Project Name", "project_name"],
    "site": ["url_namespace (site_restricted_sites)", "Url Namespace", "url_namespace"],
    "count": ["Number of Records", "number_of_records", "Count"],
}
DATE_FORMATS = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d",
                "%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y %I:%M %p", "%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M",
                "%m/%d/%Y", "%m/%d/%y %I:%M:%S %p", "%m/%d/%y %I:%M %p", "%m/%d/%y %H:%M", "%m/%d/%y"]


def read_text(path):
    b = path.read_bytes()
    for enc in ("utf-8-sig", "cp1252", "latin-1"):
        try:
            return b.decode(enc), enc
        except UnicodeDecodeError:
            continue
    return b.decode("latin-1", errors="replace"), "latin-1"


def norm(s):
    return re.sub(r"\s+", " ", (s or "").replace("\xa0", " ")).strip()


def pick(header):
    low = {h.strip().lower(): h for h in header}
    return {k: next((low[c.lower()] for c in cands if c.lower() in low), None) for k, cands in COLS.items()}


def parse_time(v):
    v = norm(v)
    if not v:
        return None
    v2 = re.sub(r"\.\d+", "", v.replace("Z", ""))
    for f in DATE_FORMATS:
        try:
            return datetime.strptime(v2, f)
        except ValueError:
            continue
    return None


def is_access(row, cols):
    vals = [norm(row.get(cols[k])).lower() for k in ("action",) if cols[k]]
    if not vals:
        return True
    return any("access" in v for v in vals)


def load_view_map(out, site, fetch):
    """Return (by_url, by_names3, by_names2) mapping to view LUID."""
    views = {}
    if fetch:
        with TableauClient(site) as client:
            wbs = {w["id"]: w for w in client.rest_paged("/workbooks", "workbooks", "workbook", page_size=100)}
            for v in client.rest_paged("/views", "views", "view", page_size=100):
                wb = wbs.get((v.get("workbook") or {}).get("id")) or {}
                views[v["id"]] = {"name": v.get("name"), "workbook": wb.get("name"),
                                  "project": (wb.get("project") or {}).get("name"),
                                  "content_url": v.get("contentUrl")}
        log(f"Fetched {len(views)} views for matching")
    else:
        snaps = sorted((out / "usage-snapshots").glob("*.json"))
        if not snaps:
            sys.exit("No usage snapshots found; run step 02 first, or use --fetch-map")
        views = json.loads(snaps[-1].read_text(encoding="utf-8"))["views"]
    by_url, by3, by2 = {}, {}, defaultdict(list)
    for luid, v in views.items():
        if v.get("content_url"):
            by_url[v["content_url"].lower()] = luid
        key3 = (norm(v.get("project")).lower(), norm(v.get("workbook")).lower(), norm(v.get("name")).lower())
        by3[key3] = luid
        by2[key3[1:]].append(luid)
    if not by_url:
        log("Latest snapshot has no content URLs (it predates this importer): matching by names only. "
            "Use --fetch-map for exact matching, or it will be exact after the next step 02 snapshot.")
    return by_url, by3, {k: v[0] for k, v in by2.items() if len(v) == 1}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    add_site_args(ap)
    ap.add_argument("--fetch-map", action="store_true",
                    help="sign in once and list views for exact URL matching (otherwise uses the latest snapshot)")
    args = ap.parse_args()
    site, out = resolve_site_args(args)
    src = out / "traffic-to-views"
    files = sorted(src.glob("*.csv")) if src.exists() else []
    if not files:
        sys.exit(f"No CSV files in {src}. Save the Traffic to Views full-data export(s) there.")

    by_url, by3, by2 = load_view_map(out, site, args.fetch_map)

    events = []   # (luid_or_None, time, user_key, user_label, count, key_for_unmatched)
    seen_other_files, skipped_dupes, skipped_site, skipped_action, bad_time = set(), 0, 0, 0, 0
    placeholder = 0
    for f in files:
        text, enc = read_text(f)
        rdr = csv.DictReader(io.StringIO(text))
        cols = pick(rdr.fieldnames or [])
        missing = [k for k in ("time",) if not cols[k]]
        if missing or not (cols["url"] or (cols["workbook"] and cols["view"])):
            sys.exit(f"{f.name}: missing required columns (need a date/time column and either Repository Url "
                     f"or workbook + view names). Found: {', '.join(rdr.fieldnames or [])}")
        log(f"{f.name}: encoding {enc}; using " + ", ".join(f"{k}='{v}'" for k, v in cols.items() if v))
        this_file = set()
        for row in rdr:
            raw_key = tuple(row.values())
            if raw_key in seen_other_files:
                skipped_dupes += 1
                continue
            this_file.add(raw_key)
            if cols["site"] and norm(row.get(cols["site"])) not in (site, "") and site:
                skipped_site += 1
                continue
            if not is_access(row, cols):
                skipped_action += 1
                continue
            tv = row.get(cols["time"]) or ""
            if "#" in tv:
                placeholder += 1
                continue
            t = parse_time(tv)
            if t is None:
                bad_time += 1
                continue
            url = norm(row.get(cols["url"])).lower() if cols["url"] else ""
            proj = norm(row.get(cols["project"])).lower() if cols["project"] else ""
            wbn = norm(row.get(cols["workbook"])).lower() if cols["workbook"] else ""
            vn = norm(row.get(cols["view"])).lower() if cols["view"] else ""
            luid, how = by_url.get(url), "url"
            if not luid:
                luid, how = by3.get((proj, wbn, vn)), "names"
            if not luid:
                luid, how = by2.get((wbn, vn)), "names"
            uid = norm(row.get(cols["user_id"])) if cols["user_id"] else ""
            ulabel = norm(row.get(cols["user"])) if cols["user"] else uid
            try:
                cnt = int(float(row.get(cols["count"]) or 1)) if cols["count"] else 1
            except ValueError:
                cnt = 1
            events.append((luid, how if luid else None, t, uid or ulabel, ulabel, cnt,
                           (norm(row.get(cols["url"])) if cols["url"] else "", norm(row.get(cols["workbook"]) or ""),
                            norm(row.get(cols["view"]) or ""))))
        seen_other_files |= this_file

    if placeholder and not events:
        sys.exit(f"All {placeholder} date values are '########'. The file was opened and saved in Excel, which "
                 "writes that placeholder when a date column is too narrow. Re-export from Tableau and copy the "
                 "CSV in directly, without opening it in Excel.")
    if placeholder:
        log(f"WARNING: {placeholder} rows had '########' dates (Excel artifact) and were skipped")
    if not events:
        sys.exit("No usable access events found. Check the export's date column and Action filter.")

    start = min(e[2] for e in events)
    end = max(e[2] for e in events)
    span_weeks = max(1, -(-((end - start).days + 1) // 7))
    per_view = defaultdict(lambda: {"accesses": 0, "users": Counter(), "labels": {}, "weeks": set(),
                                    "months": Counter(), "first": None, "last": None, "how": None})
    unmatched = Counter()
    for luid, how, t, ukey, ulabel, cnt, ukey_view in events:
        if not luid:
            unmatched[ukey_view] += cnt
            continue
        v = per_view[luid]
        v["accesses"] += cnt
        v["how"] = v["how"] or how
        if ukey:
            v["users"][ukey] += cnt
            v["labels"][ukey] = ulabel
        v["weeks"].add(t.strftime("%G-W%V"))
        v["months"][t.strftime("%Y-%m")] += cnt
        v["first"] = t if v["first"] is None or t < v["first"] else v["first"]
        v["last"] = t if v["last"] is None or t > v["last"] else v["last"]

    today = datetime.now()
    out_views = {}
    for luid, v in per_view.items():
        top = v["users"].most_common(5)
        out_views[luid] = {
            "view_luid": luid, "match": v["how"], "accesses": v["accesses"], "users": len(v["users"]),
            # Kept so step 10 can combine a dashboard with its member sheets without double counting.
            "user_counts": {k: [v["labels"].get(k, k), n] for k, n in v["users"].items()},
            "week_keys": sorted(v["weeks"]),
            "first_accessed": v["first"].date().isoformat(), "last_accessed": v["last"].date().isoformat(),
            "days_since_last_access": (today - v["last"]).days,
            "active_weeks": len(v["weeks"]), "active_week_pct": round(100 * len(v["weeks"]) / span_weeks),
            "months": dict(sorted(v["months"].items())),
            "top_users": "; ".join(f"{v['labels'].get(k, k)} ({n})" for k, n in top),
        }
    meta = {"format": 2, "imported_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "site": site_folder(site),
            "files": [f.name for f in files], "window_start": start.date().isoformat(),
            "window_end": end.date().isoformat(), "window_days": (end - start).days + 1,
            "window_weeks": span_weeks, "events": sum(e[5] for e in events),
            "matched_views": len(out_views), "unmatched_views": len(unmatched),
            "skipped": {"duplicate_across_files": skipped_dupes, "other_site": skipped_site,
                        "non_access": skipped_action, "unparsed_time": bad_time, "excel_placeholder": placeholder}}
    (out / "traffic-usage.json").write_text(json.dumps({"window": meta, "views": out_views}, indent=2))

    cols_out = ["view_luid", "match", "accesses", "users", "first_accessed", "last_accessed",
                "days_since_last_access", "active_weeks", "active_week_pct", "top_users"]
    with open(out / "traffic-usage.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols_out, extrasaction="ignore", lineterminator="\n")
        w.writeheader()
        for r in sorted(out_views.values(), key=lambda r: -r["accesses"]):
            w.writerow(r)
    with open(out / "traffic-unmatched.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(["repository_url", "workbook", "view", "accesses"])
        for (u, wbn, vn), n in unmatched.most_common():
            w.writerow([u, wbn, vn, n])

    hows = Counter(v["match"] for v in out_views.values())
    log(f"Window {meta['window_start']} to {meta['window_end']} ({meta['window_days']} days), "
        f"{meta['events']} access events")
    log(f"Views matched: {len(out_views)} ({hows.get('url', 0)} by URL, {hows.get('names', 0)} by names); "
        f"unmatched (likely deleted): {len(unmatched)}")
    sk = {k: v for k, v in meta["skipped"].items() if v}
    if sk:
        log("Skipped rows: " + ", ".join(f"{k}={v}" for k, v in sk.items()))
    log(f"Wrote traffic-usage.json / .csv and traffic-unmatched.csv to {out}/")


if __name__ == "__main__":
    try:
        main()
    except TableauError as e:
        sys.exit(f"ERROR: {e}")
