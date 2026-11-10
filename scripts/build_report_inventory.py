#!/usr/bin/env python3
"""Turn report-lineage-raw.json into a migration inventory keyed by LUID.

Step 10 (run last; safe to re-run whenever an upstream output changes). Standard library only.

Usage:
    python scripts/build_report_inventory.py --site <site>      (reads and writes output/<site>/)

Inputs (in --out):
    report-lineage-raw.json   required (fetch_report_lineage.py)
    view-usage.json           optional (fetch_view_usage.py); joins per-view usage by view LUID
    traffic-usage.json        optional (import_traffic_to_views.py); joins dated history as traffic_* columns
    workbook-connections.json optional (fetch_workbook_connections.py); fills source types and hosts that
                              the Metadata API left blank
    shortlist.json            optional (assessment Phase 3); joins usage, tag, score by workbook LUID
    migration-plan.json       optional (Phase 6); joins recommended_path + DM cluster

Outputs (in --out/report-inventory/):
    reports.csv                   one row per report (dashboard, or standalone visible sheet)
    edges_report_datasource.csv   report -> datasource (embedded resolved to published where proxied)
    edges_datasource_table.csv    datasource -> warehouse table / custom SQL
    tables.csv                    one row per warehouse table with fan-in counts
    custom_sql.csv                every custom SQL block, parsed tables, parse confidence
    lineage-graph.json            nodes + edges; table nodes keyed DB.SCHEMA.TABLE for joining to pipeline DAGs

"Report" = a dashboard, or a visible worksheet not contained in any dashboard.
Sheets that only exist inside dashboards roll up into the dashboard. Hidden
sheets (no LUID) are never reports on their own.
"""

import argparse
import csv
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib.tableau_client import add_site_args, resolve_site_args  # noqa: E402


def log(msg):
    print(msg, file=sys.stderr)


def union(a, b):
    """Order-preserving union (Ruby Array#|)."""
    seen = set(a)
    return list(a) + [x for x in b if not (x in seen or seen.add(x))]


def uniq(xs):
    return union([], xs)


def dig(d, *keys):
    for k in keys:
        if not isinstance(d, dict):
            return None
        d = d.get(k)
    return d


def load_json(directory, name):
    p = Path(directory) / name
    return json.loads(p.read_text()) if p.exists() else None


# ---------------------------------------------------------------- tables
# Key = DATABASE.SCHEMA.TABLE, upper-cased, brackets/quotes stripped.
# Upper-casing matches Snowflake identifier folding; revisit for case-sensitive
# warehouses (quoted identifiers, Postgres).
def clean(s):
    return re.sub(r'[\[\]"`]', "", "" if s is None else str(s)).strip()


TABLES = {}


RESTRICTED = "(restricted)"

# Connection types for file and file-like sources. Lowercased Tableau class names.
FILE_TYPES = {"excel-direct", "excel", "textscan", "csv", "json", "pdf",
              "spatial", "stat-direct", "google-sheets", "googlesheets", "googledrive", "box", "onedrive",
              "dropbox", "webdata-direct", "webdata"}
# An extract whose only recorded connection is the extract itself: origin unknown.
EXTRACT_TYPES = {"hyper", "dataengine", "extract"}

LINEAGE_STATUS_HELP = {
    "tables": "warehouse tables found",
    "no-datasource": "uses no datasource (text, image, web or blank objects only)",
    "file-source": "datasources connect only to files (Excel, CSV, Google Sheets...)",
    "extract-only": "extract with no recorded origin: the data exists only in Tableau",
    "published-unresolved": "connects to a published datasource that could not be resolved",
    "unparsed-custom-sql": "custom SQL that could not be parsed into tables",
    "no-tables-indexed": "database source (type and host known), but no tables recorded",
    "no-source-info": "no tables or connection info, even from REST connections (run step 03b?)",
}


def table_key(t):
    # Custom SQL reaches sheets through upstreamTables too; its real tables come
    # from the custom_sql section, so the custom SQL node itself is not a table.
    if not t or t.get("__typename") == "CustomSQLTable":
        return None
    name, schema = clean(t.get("name")), clean(t.get("schema"))
    fn = clean(t.get("fullName"))
    if not schema and "." in fn:
        parts = fn.split(".")
        name, schema = parts[-1], parts[-2]
    db = clean(dig(t, "database", "name"))
    key = ".".join(x for x in (db, schema, name) if x).upper()
    if not key:
        return None
    TABLES.setdefault(key, {
        "table_key": key, "database": db, "schema": schema, "table": name,
        "connection_type": t.get("connectionType") or dig(t, "database", "connectionType"),
        "host": dig(t, "database", "hostName"),
    })
    return key


def table_keys(ts):
    return uniq(k for k in (table_key(t) for t in (ts or [])) if k)


# Best-effort FROM/JOIN extraction for custom SQL Tableau couldn't parse.
SQL_KW = {"SELECT", "LATERAL", "TABLE", "UNNEST", "VALUES"}


def regex_tables(sql):
    body = re.sub(r"--[^\n]*|/\*.*?\*/", " ", sql or "", flags=re.S)
    ctes = {m.upper() for m in re.findall(r"(?:WITH|,)\s*([A-Za-z_][\w$]*)\s+AS\s*\(", body, flags=re.I)}
    found = (clean(t).upper() for t in re.findall(r'\b(?:FROM|JOIN)\s+([\w$."\[\]`]+)', body, flags=re.I))
    return uniq(t for t in found if t and not t.startswith("(") and t not in SQL_KW and t not in ctes)


def traffic_cols(traffic, luid, window, present):
    t = traffic.get(luid) if luid else None
    if not present:
        return {}
    if t is None:
        # In the export window with no access events (the export lists every access).
        return {"traffic_window": window, "traffic_accesses": 0, "traffic_users": 0}
    return {"traffic_window": window, "traffic_accesses": t["accesses"], "traffic_users": t["users"],
            "traffic_first_accessed": t["first_accessed"], "traffic_last_accessed": t["last_accessed"],
            "traffic_days_since_access": t["days_since_last_access"],
            "traffic_active_weeks": t["active_weeks"], "traffic_active_week_pct": t["active_week_pct"],
            "traffic_top_users": t["top_users"]}


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    add_site_args(ap)
    _, out_dir = resolve_site_args(ap.parse_args())

    raw_path = Path(out_dir) / "report-lineage-raw.json"
    if not raw_path.exists():
        sys.exit(f"missing {raw_path}; run scripts/fetch_report_lineage.py first")
    raw = json.loads(raw_path.read_text())
    dest = Path(out_dir) / "report-inventory"
    dest.mkdir(parents=True, exist_ok=True)

    shortlist = {w.get("luid"): w for w in (load_json(out_dir, "shortlist.json") or [])}
    plan = load_json(out_dir, "migration-plan.json") or {}
    plan_wb = {w.get("workbookId"): w for w in plan.get("workbooks") or []}
    tu = load_json(out_dir, "traffic-usage.json")
    traffic = (tu or {}).get("views") or {}
    traffic_window = (f"{dig(tu, 'window', 'window_start')}..{dig(tu, 'window', 'window_end')}" if tu else None)
    vu = load_json(out_dir, "view-usage.json")
    view_usage = (vu or {}).get("views") or {}
    usage_days = dig(vu, "window", "usage_days")
    usage_source = dig(vu, "window", "source")
    # Fallback join when a LUID is missing on either side: workbook name + view name.
    view_by_name = {((v.get("workbook") or "").upper(), (v.get("view_name") or "").upper()): v
                    for v in view_usage.values()}
    wb_cluster = {}
    for c in plan.get("dm_clusters") or []:
        for wid in c.get("workbookIds") or []:
            wb_cluster[wid] = c.get("id")

    # ---- published datasources
    # Assets the PAT owner cannot open come back with null fields. They are kept
    # (named "(restricted)") so links from visible content still resolve, and counted.
    restricted = {"workbooks": 0, "published_datasources": 0, "custom_sql": 0}
    published = {}
    for pd in raw.get("published_datasources") or []:
        if pd.get("name") is None:
            restricted["published_datasources"] += 1
        published[pd["id"]] = {
            "id": pd["id"], "luid": pd.get("luid"), "name": pd.get("name") or RESTRICTED, "kind": "published",
            "project": pd.get("projectName"), "certified": pd.get("isCertified"),
            "extract": pd.get("hasExtracts"), "owner": dig(pd, "owner", "email"),
            "tables": table_keys(pd.get("upstreamTables")), "custom_sql": [], "published": [],
            "db_types": uniq((d.get("connectionType") or "").lower() for d in pd.get("upstreamDatabases") or []
                             if d.get("connectionType")),
            "hosts": uniq(d.get("hostName") for d in pd.get("upstreamDatabases") or [] if d.get("hostName")),
            "source_info": "metadata",
        }
    pub_by_luid = {p["luid"]: p for p in published.values()}

    # REST connections (step 03b) for datasources the Metadata API left blank.
    wc = load_json(out_dir, "workbook-connections.json") or {}
    wb_conns, pds_conns = wc.get("workbooks") or {}, wc.get("datasources") or {}
    filled = {"published": 0, "embedded": 0}
    for p in published.values():
        if not p["db_types"] and not p["tables"] and p["luid"] in pds_conns:
            conns = pds_conns[p["luid"]]
            p["db_types"] = uniq((c.get("type") or "").lower() for c in conns if c.get("type"))
            p["hosts"] = uniq(c.get("server_address") for c in conns if c.get("server_address"))
            p["source_info"] = "rest"
            filled["published"] += bool(p["db_types"])

    # ---- custom SQL
    custom_sql = {}
    ds_custom = defaultdict(list)  # datasource id -> custom sql ids
    for c in raw.get("custom_sql") or []:
        parsed, conf = table_keys(c.get("tables")), "tableau"
        if c.get("name") is None and c.get("query") is None:
            restricted["custom_sql"] += 1
            conf = "restricted"
        elif not parsed:
            db = clean(dig(c, "database", "name")).upper()
            parsed = [t if t.count(".") >= 2 or not db else f"{db}.{t}" for t in regex_tables(c.get("query"))]
            for k in parsed:
                parts = k.split(".")
                TABLES.setdefault(k, {
                    "table_key": k,
                    "database": parts[-3] if len(parts) >= 3 else None,
                    "schema": parts[-2] if len(parts) >= 2 else None,
                    "table": parts[-1],
                    "connection_type": dig(c, "database", "connectionType"),
                })
            conf = "regex" if parsed else "unparsed"
        custom_sql[c["id"]] = {
            "id": c["id"], "name": c.get("name"), "tables": parsed, "confidence": conf,
            "unsupported": c.get("isUnsupportedCustomSql"), "query": c.get("query"),
        }
        for d in c.get("downstreamDatasources") or []:
            ds_custom[d["id"]].append(c["id"])

    # ---- Prep flows
    flows = [{
        "id": f["id"], "luid": f.get("luid"), "name": f.get("name"),
        "tables": table_keys(f.get("upstreamTables")),
        "outputs": [d["luid"] for d in f.get("downstreamDatasources") or [] if d.get("luid")],
    } for f in raw.get("flows") or []]

    # ---- workbooks -> reports
    reports, edges_rep_ds, unpublished = [], [], []
    datasources = dict(published)  # id -> record; embedded added below

    for wb in raw.get("workbooks") or []:
        if wb.get("name") is None:
            restricted["workbooks"] += 1
            continue
        sl = shortlist.get(wb.get("luid")) or {}
        pw = plan_wb.get(wb.get("luid")) or {}

        # REST connections for this workbook, grouped by datasource name (the caption, same as
        # the Metadata API's embedded datasource name).
        conns = wb_conns.get(wb.get("luid")) or []
        conns_by_name = defaultdict(list)
        for c in conns:
            conns_by_name[clean(c.get("datasource_name")).lower()].append(c)
        matched_names = set()

        # Embedded datasources: resolve proxies to the published source.
        emb = {}
        for ed in wb.get("embeddedDatasources") or []:
            pub = [p for p in (pub_by_luid.get(u.get("luid")) or published.get(u.get("id"))
                               for u in ed.get("upstreamDatasources") or []) if p]
            tables = [k for k in (table_key(t) for t in ed.get("upstreamTables") or []) if k]
            for p in pub:
                tables = union(tables, p["tables"])
            for cid in ds_custom.get(ed["id"], []):
                tables = union(tables, custom_sql[cid]["tables"])
            emb[ed["id"]] = datasources[ed["id"]] = {
                "id": ed["id"], "luid": None, "name": ed.get("name"),
                "kind": "embedded-proxy" if pub else "embedded",
                "workbook_luid": wb.get("luid"), "extract": ed.get("hasExtracts"),
                "published": [p["luid"] for p in pub], "tables": uniq(tables),
                "custom_sql": list(ds_custom.get(ed["id"], [])),
                "db_types": uniq([(d.get("connectionType") or "").lower() for d in ed.get("upstreamDatabases") or []
                                  if d.get("connectionType")] + [t for p in pub for t in p["db_types"]]),
                "hosts": uniq([d.get("hostName") for d in ed.get("upstreamDatabases") or [] if d.get("hostName")]
                              + [h for p in pub for h in p.get("hosts") or []]),
                "source_info": "rest" if not ed.get("upstreamDatabases") and any(
                    p.get("source_info") == "rest" for p in pub) else "metadata",
            }
            rec = emb[ed["id"]]
            if not rec["db_types"] and not rec["tables"] and conns:
                key = clean(ed.get("name")).lower()
                hit = conns_by_name.get(key)
                if hit:
                    matched_names.add(key)
                    rec["db_types"] = uniq((c.get("type") or "").lower() for c in hit if c.get("type"))
                    rec["hosts"] = uniq(c.get("server_address") for c in hit if c.get("server_address"))
                    rec["source_info"] = "rest"
                    filled["embedded"] += bool(rec["db_types"])
        # A workbook with exactly one still-unknown datasource and leftover unmatched connections:
        # those connections can only belong to it.
        still = [r for r in emb.values() if not r["db_types"] and not r["tables"]]
        left = [c for k, cs in conns_by_name.items() if k not in matched_names for c in cs]
        if len(still) == 1 and left:
            r = still[0]
            r["db_types"] = uniq((c.get("type") or "").lower() for c in left if c.get("type"))
            r["hosts"] = uniq(c.get("server_address") for c in left if c.get("server_address"))
            r["source_info"] = "rest-inferred"
            filled["embedded"] += bool(r["db_types"])

        sheets = {s["id"]: s for s in wb.get("sheets") or []}

        def sheet_info(s):
            ds_ids = [d["id"] for d in s.get("upstreamDatasources") or []]
            t = [k for k in (table_key(x) for x in s.get("upstreamTables") or []) if k]
            # Sheet-level table lineage misses custom SQL; inherit it from the datasource.
            for d in ds_ids:
                for cid in ds_custom.get(d, []):
                    t = union(t, custom_sql[cid]["tables"])
            # Fall back to datasource tables if Tableau gave no sheet-level tables.
            if not t:
                t = [k for d in ds_ids for k in (emb.get(d) or published.get(d) or {}).get("tables", [])]
            return ds_ids, uniq(t)

        def build(rid, luid, name, rtype, members, target=None):
            target = reports if target is None else target
            in_scope = target is reports
            ds_ids, tbls = [], []
            for s in members:
                d, t = sheet_info(s)
                ds_ids, tbls = union(ds_ids, d), union(tbls, t)
            recs = [datasources[d] for d in ds_ids if d in datasources]
            pub_names = []
            for r in recs:
                if r["kind"] == "published":
                    pub_names.append(r["name"])
                else:
                    pub_names += [dig(pub_by_luid, l, "name") for l in r.get("published") or []]
            pub_names = uniq(n for n in pub_names if n)
            conn = uniq(c for c in (dig(TABLES, k, "connection_type") for k in tbls) if c)
            src_types = uniq(t for r in recs for t in r.get("db_types") or [])
            csql_conf = [custom_sql[c]["confidence"] for r in recs
                         for c in (r.get("custom_sql") or ds_custom.get(r["id"]) or []) if c in custom_sql]
            if tbls:
                status = "tables"
            elif not ds_ids:
                status = "no-datasource"
            elif any(c in ("unparsed", "restricted") for c in csql_conf):
                status = "unparsed-custom-sql"
            elif src_types and all(t in EXTRACT_TYPES for t in src_types):
                status = "extract-only"
            elif src_types and all(t in FILE_TYPES | EXTRACT_TYPES for t in src_types):
                status = "file-source"
            elif src_types and all(t == "sqlproxy" for t in src_types):
                status = "published-unresolved"
            elif src_types:
                status = "no-tables-indexed"
            else:
                status = "no-source-info"
            for d in ds_ids if in_scope else []:
                r = datasources.get(d) or {}
                luids = [r.get("luid")] if r.get("kind") == "published" else (r.get("published") or [])
                edges_rep_ds.append({
                    "report_id": rid, "report_luid": luid, "workbook_luid": wb.get("luid"),
                    "datasource_id": d, "datasource_name": r.get("name"), "datasource_kind": r.get("kind"),
                    "published_luids": ";".join(x for x in luids if x),
                })
            usage, join = view_usage.get(luid), "luid"
            if usage is None:
                usage, join = view_by_name.get(((wb.get("name") or "").upper(), (name or "").upper())), "name"
            if usage is None:
                if not vu:
                    usage, join = {}, None
                elif usage_source == "admin-insights":
                    # Event log: absent from the window means not opened in the window.
                    usage, join = {"accesses": 0, "actors": 0}, "no-events"
                else:
                    # Snapshots list every view, so absent means created after the latest snapshot.
                    usage, join = {}, "not-in-snapshot"
            target.append({
                "report_id": rid, "report_luid": luid, "report_name": name, "report_type": rtype,
                "workbook_luid": wb.get("luid"), "workbook": wb.get("name"), "project": wb.get("projectName"),
                "owner": dig(wb, "owner", "email"), "workbook_updated": wb.get("updatedAt"),
                "sheet_count": len(members),
                "datasources": "; ".join(uniq(r["name"] for r in recs if r.get("name"))),
                "published_datasources": "; ".join(pub_names),
                "uses_custom_sql": any(r.get("custom_sql") or ds_custom.get(r["id"]) for r in recs),
                "uses_extract": any(r.get("extract") for r in recs),
                "table_count": len(tbls), "tables": "; ".join(sorted(tbls)),
                "connection_types": "; ".join(conn),
                "source_types": "; ".join(src_types), "lineage_status": status,
                "source_hosts": "; ".join(uniq(h for r in recs for h in r.get("hosts") or [])),
                "source_info": "; ".join(uniq(r.get("source_info") or "" for r in recs if r.get("source_info"))),
                "wb_accesses": sl.get("accesses"), "wb_actors": sl.get("actors"), "wb_tag": sl.get("tag"),
                "wb_score": sl.get("score"), "wb_predicted_parity": sl.get("predicted_parity_pct"),
                "wb_recommended_path": pw.get("recommended_path"), "wb_dm_cluster": wb_cluster.get(wb.get("luid")),
                "usage_source": usage_source, "usage_window_days": usage_days if vu else None,
                "usage_join": join,
                "view_accesses": usage.get("accesses"), "view_actors": usage.get("actors"),
                "view_last_accessed": usage.get("last_accessed"),
                "view_days_since_access": usage.get("days_since_last_access"),
                "view_active_weeks": usage.get("active_weeks"), "view_active_week_pct": usage.get("active_week_pct"),
                "view_top_users": usage.get("top_users"),
                "view_tracked_days": usage.get("tracked_days"),
                "view_active_intervals": usage.get("active_intervals"), "view_intervals": usage.get("intervals"),
                "view_active_interval_pct": usage.get("active_interval_pct"),
                "view_last_active_between": usage.get("last_active_between"),
                "view_lifetime_views": usage.get("lifetime_views"), "view_lifetime_avg_per_30d": usage.get("lifetime_avg_views_per_30d", usage.get("views_per_30d")),
                "view_created_at": usage.get("created_at"), "view_counter_reset": usage.get("counter_reset"),
                **traffic_cols(traffic, luid, traffic_window, bool(tu)),
            })

        for db in wb.get("dashboards") or []:
            members = [sheets[s["id"]] for s in db.get("sheets") or [] if s["id"] in sheets]
            # No LUID = the author left this dashboard out when publishing: no URL, no REST
            # view, nothing a user can open. Kept aside for reference, not in the inventory.
            build(db["id"], db.get("luid"), db.get("name"), "dashboard", members,
                  target=None if db.get("luid") else unpublished)
        for s in sheets.values():
            if s.get("containedInDashboards"):
                continue
            if not s.get("luid"):  # hidden sheet, no REST view
                continue
            build(s["id"], s.get("luid"), s.get("name"), "sheet", [s])

    # ---- datasource -> table edges
    ds_tables_rows = []
    for d in datasources.values():
        csql = d.get("custom_sql") or ds_custom.get(d["id"], [])
        for k in d["tables"]:
            via = next((c for c in csql if k in custom_sql[c]["tables"]), None)
            ds_tables_rows.append({
                "datasource_id": d["id"], "datasource_luid": d.get("luid"), "datasource_name": d.get("name"),
                "datasource_kind": d["kind"], "table_key": k,
                "via": f"custom_sql:{custom_sql[via]['confidence']}" if via else "direct",
            })

    # ---- table fan-in
    fan = defaultdict(lambda: {"reports": set(), "wbs": set(), "ds": set(), "access": {}, "vaccess": {}})
    for r in reports:
        for k in filter(None, r["tables"].split("; ")):
            f = fan[k]
            f["reports"].add(r["report_id"])
            f["wbs"].add(r["workbook_luid"])
            f["access"][r["workbook_luid"]] = int(r["wb_accesses"] or 0)
            f["vaccess"][r["report_id"]] = int(r["view_accesses"] or 0)
    for e in ds_tables_rows:
        fan[e["table_key"]]["ds"].add(e["datasource_id"])
    flow_reads = defaultdict(list)
    for f in flows:
        for k in f["tables"]:
            flow_reads[k].append(f["name"])

    table_rows = []
    for t in TABLES.values():
        f = fan[t["table_key"]]
        table_rows.append({**t, "n_reports": len(f["reports"]), "n_workbooks": len(f["wbs"]),
                           "n_datasources": len(f["ds"]), "sum_wb_accesses": sum(f["access"].values()),
                           "sum_report_accesses": sum(f["vaccess"].values()),
                           "read_by_prep_flows": "; ".join(uniq(flow_reads[t["table_key"]]))})
    table_rows.sort(key=lambda t: (-t["n_reports"], t["table_key"]))

    # ---- write
    def fmt(v):
        if v is None:
            return ""
        if isinstance(v, bool):
            return "true" if v else "false"
        return v

    def write_csv(name, rows, cols=None):
        if cols is None:
            cols = uniq(k for r in rows for k in r)
        with open(dest / name, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh, lineterminator="\n")
            w.writerow(cols)
            for r in rows:
                w.writerow([fmt(r.get(c)) for c in cols])

    write_csv("reports.csv", reports)
    write_csv("unpublished_dashboards.csv", unpublished)
    write_csv("edges_report_datasource.csv", edges_rep_ds)
    write_csv("edges_datasource_table.csv", ds_tables_rows)
    write_csv("tables.csv", table_rows,
              "table_key database schema table connection_type host n_reports n_workbooks "
              "n_datasources sum_wb_accesses sum_report_accesses read_by_prep_flows".split())
    write_csv("custom_sql.csv", [{**c, "tables": "; ".join(c["tables"]),
                                  "query": re.sub(r"\s+", " ", c["query"] or "")} for c in custom_sql.values()])

    nodes, edges = [], []
    for r in reports:
        nodes.append({"id": f"report:{r['report_id']}", "type": "report", "name": r["report_name"],
                      "luid": r["report_luid"], "workbook_luid": r["workbook_luid"]})
    for d in datasources.values():
        nodes.append({"id": f"ds:{d['id']}", "type": f"datasource_{d['kind']}", "name": d.get("name"),
                      "luid": d.get("luid")})
    for k in TABLES:
        nodes.append({"id": f"table:{k}", "type": "table", "name": k})
    for f in flows:
        nodes.append({"id": f"flow:{f['id']}", "type": "prep_flow", "name": f["name"], "luid": f["luid"]})
    for e in edges_rep_ds:
        edges.append({"from": f"ds:{e['datasource_id']}", "to": f"report:{e['report_id']}"})
    for d in datasources.values():
        for l in d.get("published") or []:
            if l in pub_by_luid:
                edges.append({"from": f"ds:{pub_by_luid[l]['id']}", "to": f"ds:{d['id']}"})
    for e in ds_tables_rows:
        edges.append({"from": f"table:{e['table_key']}", "to": f"ds:{e['datasource_id']}", "via": e["via"]})
    for f in flows:
        for k in f["tables"]:
            edges.append({"from": f"table:{k}", "to": f"flow:{f['id']}"})
        for l in f["outputs"]:
            if l in pub_by_luid:
                edges.append({"from": f"flow:{f['id']}", "to": f"ds:{pub_by_luid[l]['id']}"})
    edges = uniq(json.dumps(e, sort_keys=True) for e in edges)
    (dest / "lineage-graph.json").write_text(
        json.dumps({"nodes": nodes, "edges": [json.loads(e) for e in edges]}, indent=2))

    n_dash = sum(r["report_type"] == "dashboard" for r in reports)
    log(f"reports: {len(reports)} ({n_dash} dashboards, {len(reports) - n_dash} standalone sheets)")
    if not reports:
        log("WARNING: no reports found. Check 'errors' in report-lineage-raw.json and the step 03 output.")
    if any(restricted.values()):
        log(f"restricted (no permission, excluded or unnamed): {restricted['workbooks']} workbooks, "
            f"{restricted['published_datasources']} published datasources, {restricted['custom_sql']} custom SQL. "
            "A Site Administrator role on this site removes these.")
    confs = [c["confidence"] for c in custom_sql.values()]
    log(f"datasources: {len(datasources)}, tables: {len(TABLES)}, custom SQL: {len(custom_sql)} "
        f"({confs.count('regex')} regex-parsed, {confs.count('unparsed')} unparsed, {confs.count('restricted')} restricted)")
    if wc:
        log(f"REST connections filled sources for {filled['embedded']} embedded and "
            f"{filled['published']} published datasources")
    if tu:
        hit = sum(1 for r in reports if (r.get("traffic_accesses") or 0) > 0)
        log(f"traffic history ({traffic_window}): {hit} of {len(reports)} reports accessed in the window")
    if unpublished:
        log(f"unpublished dashboards (no LUID, excluded; see unpublished_dashboards.csv): {len(unpublished)}")
    log(f"reports with no table lineage: {sum(r['table_count'] == 0 for r in reports)}")
    counts = defaultdict(int)
    for r in reports:
        counts[r["lineage_status"]] += 1
    for k in LINEAGE_STATUS_HELP:
        if counts.get(k):
            log(f"  lineage_status {k:<20} {counts[k]:>6}  {LINEAGE_STATUS_HELP[k]}")
    if vu:
        joins = [r["usage_join"] for r in reports]
        log(f"view usage ({usage_source}, window {usage_days}): {joins.count('luid')} by LUID, {joins.count('name')} by name, "
            f"{joins.count('no-events')} with no access events, {joins.count('not-in-snapshot')} not in snapshot")
    if raw.get("errors"):
        log(f"fetch errors: {', '.join(raw['errors'])}")
    log(f"Wrote {dest}/")


if __name__ == "__main__":
    main()
