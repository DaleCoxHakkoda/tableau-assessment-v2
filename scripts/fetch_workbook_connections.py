#!/usr/bin/env python3
"""Step 03b: fill in data sources the Metadata API could not describe, from REST connections.

Usage:
    python scripts/fetch_workbook_connections.py --site <site>          # only what step 03 left unknown
    python scripts/fetch_workbook_connections.py --site <site> --all    # every workbook and published datasource

Why: the Metadata API returns no connection or table info for some datasources,
most often extracts (embedded, extract, 0 dbs, 0 tables in diagnose_lineage.py).
The REST connections endpoints still report each connection's type (sqlserver,
excel-direct, textscan, hyper...) and server address, so most unknowns become
"Excel file", "SQL Server on host X", or "extract with no recorded origin".

Reads output/<site>/report-lineage-raw.json (step 03). One REST call per workbook
or published datasource that needs it; --all fetches everything, which also gives
server addresses for the whole site (useful when mapping sources to pipelines).

Writes output/<site>/workbook-connections.json:
    {"workbooks": {workbook_luid: [connection, ...]},
     "datasources": {published_datasource_luid: [connection, ...]},
     "errors": {luid: message}}
Each connection: type, server_address, server_port, datasource_name, datasource_id.
Usernames are not stored. Step 10 merges these into reports.csv (source_types,
source_hosts, lineage_status).
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib.tableau_client import (HTTPStatusError, TableauClient, TableauError,  # noqa: E402
                                add_site_args, log, resolve_site_args)


def unknown(ds):
    return not (ds.get("upstreamDatabases") or ds.get("upstreamTables"))


def connections(resp):
    items = (resp.get("connections") or {}).get("connection") or []
    if isinstance(items, dict):
        items = [items]
    return [{"type": c.get("type"), "server_address": c.get("serverAddress"),
             "server_port": c.get("serverPort"),
             "datasource_name": (c.get("datasource") or {}).get("name"),
             "datasource_id": (c.get("datasource") or {}).get("id")} for c in items]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    add_site_args(ap)
    ap.add_argument("--all", action="store_true", help="fetch every workbook and published datasource")
    args = ap.parse_args()
    site, out = resolve_site_args(args)
    raw_path = out / "report-lineage-raw.json"
    if not raw_path.exists():
        sys.exit(f"missing {raw_path}; run step 03 first")
    raw = json.loads(raw_path.read_text(encoding="utf-8"))

    pubs = raw.get("published_datasources") or []
    pub_by_luid = {p.get("luid"): p for p in pubs if p.get("luid")}
    pub_by_id = {p["id"]: p for p in pubs}

    def ds_unknown(ed):
        if not unknown(ed):
            return False
        ups = [pub_by_luid.get(u.get("luid")) or pub_by_id.get(u.get("id")) for u in ed.get("upstreamDatasources") or []]
        return not ups or any(u is None or unknown(u) for u in ups)

    wb_targets = [w["luid"] for w in raw.get("workbooks") or [] if w.get("luid") and w.get("name") is not None
                  and (args.all or any(ds_unknown(e) for e in w.get("embeddedDatasources") or []))]
    ds_targets = [p["luid"] for p in pubs if p.get("luid") and p.get("name") is not None
                  and (args.all or unknown(p))]
    log(f"Fetching REST connections for {len(wb_targets)} workbooks and {len(ds_targets)} published datasources"
        + ("" if args.all else " with unknown sources"))

    result = {"fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
              "scope": "all" if args.all else "unknown-only", "workbooks": {}, "datasources": {}, "errors": {}}
    with TableauClient(site) as client:
        for kind, path, targets in (("workbooks", "/workbooks/{}/connections", wb_targets),
                                    ("datasources", "/datasources/{}/connections", ds_targets)):
            for i, luid in enumerate(targets, 1):
                try:
                    result[kind][luid] = connections(client.rest("GET", path.format(luid)))
                except HTTPStatusError as e:
                    result["errors"][luid] = f"{kind[:-1]} HTTP {e.status}"
                except TableauError as e:
                    result["errors"][luid] = f"{kind[:-1]} {e}"[:300]
                if i % 50 == 0 or i == len(targets):
                    log(f"  {kind}: {i}/{len(targets)}")

    (out / "workbook-connections.json").write_text(json.dumps(result, indent=2))
    types = {}
    for conns in list(result["workbooks"].values()) + list(result["datasources"].values()):
        for c in conns:
            types[c["type"] or "?"] = types.get(c["type"] or "?", 0) + 1
    log("Connection types found: " + ", ".join(f"{t}={n}" for t, n in sorted(types.items(), key=lambda x: -x[1]))
        if types else "No connections returned")
    if result["errors"]:
        log(f"{len(result['errors'])} item(s) failed (see 'errors' in workbook-connections.json)")
    log(f"Wrote {out / 'workbook-connections.json'}")


if __name__ == "__main__":
    try:
        main()
    except TableauError as e:
        sys.exit(f"ERROR: {e}")
