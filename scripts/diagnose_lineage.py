#!/usr/bin/env python3
"""Explain reports whose lineage_status is no-source-info or no-datasource.

Usage:
    python scripts/diagnose_lineage.py --site <site>

Reads output/<site>/report-lineage-raw.json and report-inventory/reports.csv only
(no Tableau calls). Groups the unexplained reports by the pattern of what Tableau
returned for their datasources, so you can tell an extraction gap (fixable in
code) from a real data-source gap (migration risk).

Prints a pattern summary and writes report-inventory/lineage-diagnosis.csv
(one row per report x datasource, with the pattern for each). Output contains
datasource and workbook names only; no SQL text, no user names.
"""

import argparse
import csv
import json
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib.tableau_client import add_site_args, resolve_site_args  # noqa: E402

TARGET = ("no-source-info", "no-datasource")


def n(x):
    return len(x or [])


def ds_pattern(ds, kind, pub_by_luid, pub_by_id):
    """Describe one datasource the way it came back from the Metadata API."""
    if ds is None:
        return "datasource id not found in workbook or published list"
    name = (ds.get("name") or "")
    if name == "Parameters":
        return "Tableau 'Parameters' pseudo-datasource"
    parts = [kind, "extract" if ds.get("hasExtracts") else "live"]
    ups = ds.get("upstreamDatasources") or []
    if ups:
        resolved = [pub_by_luid.get(u.get("luid")) or pub_by_id.get(u.get("id")) for u in ups]
        if not all(resolved):
            parts.append("proxy to UNRESOLVED published datasource")
        else:
            p = resolved[0]
            parts.append(f"proxy to published ({n(p.get('upstreamDatabases'))} dbs, "
                         f"{n(p.get('upstreamTables'))} tables)")
    parts.append(f"{n(ds.get('upstreamDatabases'))} dbs")
    parts.append(f"{n(ds.get('upstreamTables'))} tables")
    types = sorted({(d.get("connectionType") or "?") for d in ds.get("upstreamDatabases") or []})
    if types:
        parts.append("types=" + "/".join(types))
    return ", ".join(parts)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    add_site_args(ap)
    ap.add_argument("--samples", type=int, default=3, help="example reports to print per pattern")
    _, out = resolve_site_args(ap.parse_args())

    raw_path, rep_path = out / "report-lineage-raw.json", out / "report-inventory" / "reports.csv"
    for p in (raw_path, rep_path):
        if not p.exists():
            sys.exit(f"missing {p}; run steps 03 and 10 first")
    raw = json.loads(raw_path.read_text(encoding="utf-8"))
    with open(rep_path, encoding="utf-8") as fh:
        reports = [r for r in csv.DictReader(fh) if r.get("lineage_status") in TARGET]
    if not reports:
        print("No reports with lineage_status no-source-info or no-datasource. Nothing to diagnose.")
        return

    pubs = raw.get("published_datasources") or []
    pub_by_luid = {p.get("luid"): p for p in pubs if p.get("luid")}
    pub_by_id = {p["id"]: p for p in pubs}
    wbs = {w.get("luid"): w for w in raw.get("workbooks") or []}

    rows, patterns = [], {t: Counter() for t in TARGET}
    examples = {t: {} for t in TARGET}
    for r in reports:
        status, wb = r["lineage_status"], wbs.get(r["workbook_luid"]) or {}
        sheets = {s["id"]: s for s in wb.get("sheets") or []}
        emb = {e["id"]: e for e in wb.get("embeddedDatasources") or []}
        if r["report_type"] == "dashboard":
            dash = next((d for d in wb.get("dashboards") or [] if d["id"] == r["report_id"]), {})
            members = [sheets.get(s["id"]) for s in dash.get("sheets") or []]
            listed = n(dash.get("sheets"))
        else:
            members, listed = [sheets.get(r["report_id"])], 1
        found = [m for m in members if m]

        if status == "no-datasource":
            if r["report_type"] == "dashboard" and listed == 0:
                pat = "dashboard lists no member sheets (text/image/web objects only, or not indexed)"
            elif len(found) < listed:
                pat = "member sheets listed on the dashboard but missing from the workbook's sheets"
            else:
                pat = f"{r['report_type']}: {len(found)} sheet(s), none with an upstream datasource"
            patterns[status][pat] += 1
            examples[status].setdefault(pat, []).append(f"{r['workbook']} / {r['report_name']}")
            rows.append({"lineage_status": status, "report_type": r["report_type"], "workbook": r["workbook"],
                         "report_name": r["report_name"], "datasource": "", "pattern": pat})
            continue

        ds_ids = list(dict.fromkeys(d["id"] for m in found for d in m.get("upstreamDatasources") or []))
        report_pats = set()
        for did in ds_ids:
            if did in emb:
                ds, kind = emb[did], "embedded"
            elif did in pub_by_id:
                ds, kind = pub_by_id[did], "published"
            else:
                ds, kind = None, "?"
            pat = ds_pattern(ds, kind, pub_by_luid, pub_by_id)
            report_pats.add(pat)
            rows.append({"lineage_status": status, "report_type": r["report_type"], "workbook": r["workbook"],
                         "report_name": r["report_name"], "datasource": (ds or {}).get("name") or did,
                         "pattern": pat})
        key = " | ".join(sorted(report_pats)) or "no datasource ids on member sheets"
        patterns[status][key] += 1
        examples[status].setdefault(key, []).append(f"{r['workbook']} / {r['report_name']}")

    dest = out / "report-inventory" / "lineage-diagnosis.csv"
    with open(dest, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]), lineterminator="\n")
        w.writeheader()
        w.writerows(rows)

    for status in TARGET:
        total = sum(patterns[status].values())
        if not total:
            continue
        print(f"\n== {status}: {total} reports")
        for pat, cnt in patterns[status].most_common():
            print(f"  {cnt:>6}  {pat}")
            for ex in examples[status][pat][:ap.parse_args().samples]:
                print(f"            e.g. {ex}")
    print(f"\nDetail per report x datasource: {dest}")


if __name__ == "__main__":
    main()
