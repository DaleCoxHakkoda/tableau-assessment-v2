#!/usr/bin/env python3
"""Trace warehouse tables with missing connection details back to their connections.

Usage:
    python scripts/diagnose_tables.py --site <site>                  # tables with no type, host or database
    python scripts/diagnose_tables.py --site <site> --schema EDW     # or every table in a schema
    python scripts/diagnose_tables.py --site <site> --table CLAIMS   # or tables whose key contains a text
    python scripts/diagnose_tables.py --all-sites [--schema EDW]     # every site, one combined file

tables.csv takes connection_type, host and database from the database object the Metadata
API links each table to. When all three are blank, that link was missing, and the answer is
in the connection behind the datasource. For each table this reads, without calling Tableau:

  report-inventory/   tables.csv, edges_datasource_table.csv (table -> datasource, and whether
                      it came from custom SQL), edges_report_datasource.csv, reports.csv
  workbook-connections.json (step 03b)   the REST connection type and server per datasource
  twbs/<luid>.twb (step 04)              the workbook file's own <connection> attributes:
                      class, server, dbname, schema, an ODBC DSN, and Initial SQL

Writes report-inventory/table-diagnosis.csv (one row per table and datasource) and prints
the connection classes found, which usually names the cause:
  genericodbc / odbc-dsn   server and database live in an ODBC DSN on the machine that ran it
  empty server and dbname  connection details not saved (often a locally built extract)
  sqlproxy only            a published datasource: the REST columns show its real connection
"""

import argparse
import csv
import html
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib.tableau_client import REPO_ROOT, add_site_args, log, resolve_site_args  # noqa: E402

CONN_ATTRS = ("class", "server", "dbname", "schema", "port", "odbc-dsn", "odbc-connect-string-extras", "username")
SKIP_CLASSES = {"federated"}  # the wrapper Tableau puts around a datasource's real connections


def read_csv(path):
    if not path.exists():
        return []
    with open(path, encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def attrs(tag):
    return {k: html.unescape(v) for k, v in re.findall(r"([\w:-]+)='([^']*)'", tag)}


def twb_connections(twb_text, ds_name):
    """Connection attributes of the datasource called ds_name (by caption or name) in a .twb."""
    out, initial_sql = [], []
    for m in re.finditer(r"<datasource\b([^>]*)>(.*?)</datasource>", twb_text, flags=re.S):
        a = attrs(m.group(1))
        if ds_name not in (a.get("caption"), a.get("name")):
            continue
        for tag in re.findall(r"<connection\b[^>]*>", m.group(2)):
            c = attrs(tag)
            if c.get("one-time-sql"):
                initial_sql.append(" ".join(c["one-time-sql"].split())[:200])
            if c.get("class") in SKIP_CLASSES:
                continue
            out.append({k: c.get(k, "") for k in CONN_ATTRS if k != "username"})
    return out, initial_sql


def write_rows(dest, rows):
    with open(dest, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]), lineterminator="\n")
        w.writeheader()
        w.writerows(rows)


def diagnose_site(out, args):
    """Rows of table-diagnosis.csv for one site folder (also written into it)."""
    inv = out / "report-inventory"
    tables = read_csv(inv / "tables.csv")
    if not tables:
        log(f"{out.name}: no tables.csv (run step 10 first)")
        return []
    if args.schema:
        pick = [t for t in tables if (t["schema"] or "").upper() == args.schema.upper()]
        what = f"schema {args.schema}"
    elif args.table:
        pick = [t for t in tables if args.table.upper() in t["table_key"].upper()]
        what = f"tables matching {args.table}"
    else:
        pick = [t for t in tables if not (t["connection_type"] or t["host"] or t["database"])]
        what = "tables with no connection type, host or database"
    log(f"{out.name}: {len(pick)} {what}")
    if not pick:
        return []

    ds_of = defaultdict(list)
    for e in read_csv(inv / "edges_datasource_table.csv"):
        ds_of[e["table_key"]].append(e)
    wbs_of_ds, wbs_of_pub, pubs_of_ds = defaultdict(set), defaultdict(set), defaultdict(set)
    for e in read_csv(inv / "edges_report_datasource.csv"):
        wbs_of_ds[e["datasource_id"]].add(e["workbook_luid"])
        for luid in (e.get("published_luids") or "").split(";"):
            if luid.strip():
                wbs_of_pub[luid.strip()].add(e["workbook_luid"])
                pubs_of_ds[e["datasource_id"]].add(luid.strip())
    wb_info = {}
    for r in read_csv(inv / "reports.csv"):
        wb_info.setdefault(r["workbook_luid"], (r["workbook"], r["project"], r["owner"]))
    wc_path = out / "workbook-connections.json"
    wc = json.loads(wc_path.read_text(encoding="utf-8")) if wc_path.exists() else {}
    rest_wb, rest_pds = wc.get("workbooks") or {}, wc.get("datasources") or {}
    twb_cache = {}

    def twb(luid):
        if luid not in twb_cache:
            p = out / "twbs" / f"{luid}.twb"
            twb_cache[luid] = p.read_text(encoding="utf-8", errors="replace") if p.exists() else None
        return twb_cache[luid]

    rows = []
    for t in pick:
        edges = ds_of.get(t["table_key"]) or [{}]
        for e in edges:
            ds_name, kind, luid = e.get("datasource_name") or "", e.get("datasource_kind") or "", e.get("datasource_luid")
            via_published = kind in ("published", "embedded-proxy")
            wb_luids = sorted(wbs_of_pub.get(luid, set()) if kind == "published" and luid
                              else wbs_of_ds.get(e.get("datasource_id"), set()))
            # A workbook's link to a published datasource (sqlproxy) points at Tableau Server; the
            # real connection is the published datasource's own, from step 03b.
            pub_luids = {luid} if kind == "published" and luid else pubs_of_ds.get(e.get("datasource_id"), set())
            rest = [c for p in pub_luids for c in rest_pds.get(p) or []] if via_published else []
            twb_conns, init_sql = [], []
            for w in wb_luids:
                if not via_published:
                    rest += [c for c in rest_wb.get(w) or [] if c.get("datasource_name") == ds_name]
                rest = [c for c in rest if (c.get("type") or "").lower() != "sqlproxy"]
                text = twb(w)
                if text and not via_published:
                    c, i = twb_connections(text, ds_name)
                    twb_conns += c
                    init_sql += i
            cls = sorted({c["class"] for c in twb_conns if c["class"]})
            names = [wb_info.get(w, ("", "", ""))[0] for w in wb_luids]

            def j(vals):
                return "; ".join(sorted({v for v in vals if v}))
            rows.append({
                "site": out.name, "table_key": t["table_key"], "schema": t["schema"], "table": t["table"],
                "found_via": e.get("via", ""), "datasource": ds_name, "datasource_kind": kind,
                "rest_types": j(c.get("type") for c in rest), "rest_servers": j(c.get("server_address") for c in rest),
                "twb_classes": "; ".join(cls), "twb_servers": j(c["server"] for c in twb_conns),
                "twb_dbnames": j(c["dbname"] for c in twb_conns), "twb_schemas": j(c["schema"] for c in twb_conns),
                "twb_odbc_dsn": j(c["odbc-dsn"] for c in twb_conns),
                "initial_sql": " | ".join(sorted(set(init_sql)))[:400],
                "workbooks": len(wb_luids), "example_workbooks": "; ".join(sorted(n for n in names if n)[:3]),
                "owners": j(wb_info.get(w, ("", "", ""))[2] for w in wb_luids),
                "workbook_luids": "; ".join(wb_luids[:10]),
            })
    write_rows(inv / "table-diagnosis.csv", rows)
    return rows


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    add_site_args(ap)
    ap.add_argument("--all-sites", action="store_true",
                    help="every site folder under output/; also writes output/_server/table-diagnosis-all-sites.csv")
    ap.add_argument("--schema", help="every table in this schema (case-insensitive)")
    ap.add_argument("--table", help="tables whose key contains this text (case-insensitive)")
    args = ap.parse_args()
    if args.all_sites:
        root = Path(args.out).parent if args.out else REPO_ROOT / "output"
        rows = []
        for inv in sorted(root.glob("*/report-inventory")):
            if not inv.parent.name.startswith("_"):
                rows += diagnose_site(inv.parent, args)
        if rows:
            (root / "_server").mkdir(parents=True, exist_ok=True)
            dest = root / "_server" / "table-diagnosis-all-sites.csv"
            write_rows(dest, rows)
    else:
        site, out = resolve_site_args(args)
        rows = diagnose_site(out, args)
        dest = out / "report-inventory" / "table-diagnosis.csv"
    if not rows:
        return
    classes = Counter(c for r in rows for c in (r["twb_classes"].split("; ") if r["twb_classes"] else
                      ["(published datasource: see rest_types)"] if r["datasource_kind"] in ("published", "embedded-proxy")
                      else ["(not found in the workbook file)"]))
    via = Counter((r["found_via"] or "no datasource edge").split(":")[-1] if r["found_via"].startswith("custom_sql")
                  else (r["found_via"] or "no datasource edge") for r in rows)
    log("Found via: " + ", ".join(f"{k} {n}" for k, n in via.most_common()))
    log("Connection class in the workbook file: " + ", ".join(f"{k} {n}" for k, n in classes.most_common()))
    log(f"REST server found for {sum(1 for r in rows if r['rest_servers'])} of {len(rows)}; "
        f"workbook-file server for {sum(1 for r in rows if r['twb_servers'])}; "
        f"ODBC DSN for {sum(1 for r in rows if r['twb_odbc_dsn'])}; "
        f"Initial SQL for {sum(1 for r in rows if r['initial_sql'])}")
    log(f"Wrote {dest}")


if __name__ == "__main__":
    main()
