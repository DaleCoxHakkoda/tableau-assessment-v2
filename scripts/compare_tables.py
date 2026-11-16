#!/usr/bin/env python3
"""Compare the tables Tableau uses with a migration master list (for example Greenplum).

Usage:
    python scripts/compare_tables.py --master greenplum_master.csv
    python scripts/compare_tables.py --master gp.csv --schema-col SRC_SCHEMA --table-col SRC_TABLE \\
                                     --status-col STATUS --target-col SNOWFLAKE_TABLE [--only-schemas EDW,STAGE]

Reads output/_server/tables-all-sites.csv (written by step 10: one row per distinct schema +
table across every site) and the master list (a CSV; save an Excel sheet as CSV first).
Names are compared without quotes or brackets, trimmed and upper-cased. No Tableau calls.

Each Tableau table gets a match:
  schema+table     found in the master list under the same schema and table
  table only (N)   no schema match, but the table name appears in the master list under N
                   schemas: an engineer confirms which (common for unqualified names in
                   custom SQL, which have no schema)
  none             not in the master list
plus the master list's own status and target columns when given, copied as they are.

Writes output/_server/tables-vs-master.csv, ordered by usage, and prints the counts.
--only-schemas limits the comparison to the schemas that belong to the master list's
platform (for example the Greenplum schemas), so tables from other platforms do not show
up as "none".

Column names are found automatically when they are common ones (schema, schema_name,
table_schema, source_schema / table, table_name, source_table, ...); otherwise pass them.
"""

import argparse
import csv
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib.tableau_client import REPO_ROOT, log  # noqa: E402

SCHEMA_GUESS = ["schema", "schema_name", "table_schema", "source_schema", "src_schema", "gp_schema"]
TABLE_GUESS = ["table", "table_name", "source_table", "src_table", "gp_table", "object_name"]


def norm(v):
    return re.sub(r'["`\[\]]', "", v or "").strip().upper()


def pick_col(cols, given, guesses, what):
    if given:
        hit = next((c for c in cols if c.strip().lower() == given.strip().lower()), None)
        if not hit:
            sys.exit(f"--{what}-col {given!r} not in the master list; its columns: {', '.join(cols)}")
        return hit
    hit = next((c for g in guesses for c in cols if c.strip().lower() == g), None)
    if not hit:
        sys.exit(f"Could not find the {what} column in the master list; pass --{what}-col. "
                 f"Its columns: {', '.join(cols)}")
    return hit


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--master", required=True, help="master list CSV")
    ap.add_argument("--schema-col")
    ap.add_argument("--table-col")
    ap.add_argument("--status-col", help="master list column copied as master_status (e.g. migrated Y/N)")
    ap.add_argument("--target-col", help="master list column copied as master_target (e.g. the Snowflake table)")
    ap.add_argument("--only-schemas", default="", help="comma-separated schemas to compare (default: all)")
    ap.add_argument("--output-dir", default=str(REPO_ROOT / "output"))
    args = ap.parse_args()

    server = Path(args.output_dir) / "_server"
    used_path = server / "tables-all-sites.csv"
    if not used_path.exists():
        sys.exit(f"missing {used_path}; run step 10 (build_report_inventory.py) first")
    with open(used_path, encoding="utf-8") as fh:
        used = list(csv.DictReader(fh))
    with open(args.master, encoding="utf-8-sig", newline="") as fh:  # utf-8-sig: Excel's CSV marker
        rd = csv.DictReader(fh)
        cols = rd.fieldnames or []
        master_rows = list(rd)
    sc = pick_col(cols, args.schema_col, SCHEMA_GUESS, "schema")
    tc = pick_col(cols, args.table_col, TABLE_GUESS, "table")
    for c in (args.status_col, args.target_col):
        if c and c not in cols:
            sys.exit(f"{c!r} not in the master list; its columns: {', '.join(cols)}")
    log(f"Master list: {len(master_rows)} rows; schema column {sc!r}, table column {tc!r}")

    exact, by_table = {}, defaultdict(set)
    for m in master_rows:
        key = (norm(m.get(sc)), norm(m.get(tc)))
        if key[1]:
            exact.setdefault(key, m)
            by_table[key[1]].add(key[0])

    only = {norm(x) for x in args.only_schemas.split(",") if x.strip()}
    if only:
        used = [u for u in used if norm(u["schema"]) in only or not u["schema"]]
        log(f"Comparing schemas {', '.join(sorted(only))} (plus tables with no schema): {len(used)} tables")

    out, counts = [], Counter()
    for u in used:
        key = (norm(u["schema"]), norm(u["table"]))
        m = exact.get(key)
        if m:
            match, cands = "schema+table", ""
        elif by_table.get(key[1]):
            schemas = sorted(by_table[key[1]])
            match, cands = f"table only ({len(schemas)})", "; ".join(schemas)
            m = exact[(schemas[0], key[1])] if len(schemas) == 1 else None
        else:
            match, cands = "none", ""
        counts[match.split(" (")[0]] += 1
        row = {"schema": u["schema"], "table": u["table"], "match": match, "master_schemas": cands}
        if args.status_col:
            row["master_status"] = (m or {}).get(args.status_col, "")
        if args.target_col:
            row["master_target"] = (m or {}).get(args.target_col, "")
        row.update({k: u[k] for k in ("n_sites", "n_workbooks", "n_reports", "sum_wb_accesses", "connection_types",
                                      "hosts", "databases", "sites", "only_on_partial_sites", "table_keys")})
        out.append(row)

    dest = server / "tables-vs-master.csv"
    with open(dest, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(out[0]) if out else ["schema", "table", "match"], lineterminator="\n")
        w.writeheader()
        w.writerows(out)
    log(f"{len(out)} Tableau tables: {counts.get('schema+table', 0)} matched on schema and table, "
        f"{counts.get('table only', 0)} on table name only (confirm the schema), {counts.get('none', 0)} not in the "
        f"master list")
    if args.status_col:
        st = Counter((r["master_status"] or "(blank)") for r in out if r["match"] == "schema+table")
        log("Master status of exact matches: " + ", ".join(f"{k} {n}" for k, n in st.most_common()))
    log(f"Wrote {dest}")


if __name__ == "__main__":
    main()
