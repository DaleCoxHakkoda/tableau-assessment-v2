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
    summary.csv                   the run's headline numbers, one metric per row (site, section, metric,
                                  value, description); paste or import into a workbook

Also rebuilds, across every site folder next to this one (output/_server/):
    summary-all-sites.csv         every site's summary.csv stacked (long format, for import / pivots)
    summary-all-sites-wide.csv    one row per metric, one column per site (for copy-paste)

"Report" = a dashboard, or a visible worksheet not contained in any dashboard.
Sheets that only exist inside dashboards roll up into the dashboard. Hidden
sheets (no LUID) are never reports on their own.
"""

import argparse
import csv
import json
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib.hosts import normalize_host  # noqa: E402
from lib.tableau_client import add_site_args, cfg, code_version, resolve_site_args  # noqa: E402

HOST_ALIASES = defaultdict(int)  # (raw, normalized) -> times seen


def nh(raw):
    """Normalized host name (see lib/hosts.py); records the raw spelling for host-aliases.csv."""
    norm = normalize_host(raw)
    if raw and norm:
        HOST_ALIASES[(str(raw).strip(), norm)] += 1
    return norm


def hosts_of(items, key):
    return uniq(h for h in (nh(i.get(key)) for i in items or []) if h)


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
        "host": nh(dig(t, "database", "hostName")),
    })
    return key


def table_keys(ts):
    return uniq(k for k in (table_key(t) for t in (ts or [])) if k)


# Best-effort FROM/JOIN extraction for custom SQL Tableau couldn't parse.
# Words that can follow FROM/JOIN without being a table.
SQL_KW = {"SELECT", "LATERAL", "TABLE", "UNNEST", "VALUES", "CASE", "CAST", "WHEN", "THEN", "ELSE", "END",
          "NULL", "ONLY", "DUAL", "CURRENT_DATE", "CURRENT_TIMESTAMP", "NOW", "INTERVAL", "DATE", "TIMESTAMP"}
# A parenthesis opened right after one of these starts a subquery, not a function call.
CLAUSE_KW = {"FROM", "JOIN", "IN", "EXISTS", "AS", "ON", "WHERE", "AND", "OR", "NOT", "SELECT", "UNION", "ALL",
             "ANY", "SOME", "LATERAL", "WHEN", "THEN", "ELSE", "HAVING", "BY", "USING", "WITH", "INTERSECT",
             "EXCEPT", "MINUS", "VALUES", "RETURN"}


def _inside_function(body, pos):
    """True when pos sits inside a function call's own arguments, e.g. the FROM in
    EXTRACT(YEAR FROM dt), SUBSTRING(s FROM 1 FOR 3) or TRIM(BOTH ' ' FROM x): there FROM is
    the function's grammar and what follows is a column or value, never a data source.
    A parenthesis that opens a subquery (its content starts with SELECT or WITH, as in
    ARRAY(SELECT code FROM ref.dx_codes)) is not a function's arguments: its tables count."""
    depth = 0
    for i in range(pos - 1, -1, -1):
        ch = body[i]
        if ch == ")":
            depth += 1
        elif ch == "(":
            if depth == 0:
                if re.match(r"\s*(SELECT|WITH)\b", body[i + 1:], flags=re.I):
                    return False
                m = re.search(r"([A-Za-z_][\w$]*)\s*$", body[:i])
                return bool(m) and m.group(1).upper() not in CLAUSE_KW
            depth -= 1
    return False


def regex_tables(sql):
    return regex_sources(sql)[0]


def regex_sources(sql):
    """(tables, functions) read FROM/JOIN in custom SQL. A name followed by "(" there is a
    table function (a UDF such as edw.fn_member_months(...), or generate_series(...)): it is
    reported as a function to rebuild in Snowflake, not as a table to land."""
    body = re.sub(r"--[^\n]*|/\*.*?\*/", " ", sql or "", flags=re.S)
    body = re.sub(r"'(?:[^']|'')*'", "''", body)  # string literals cannot hold table names
    ctes = {m.upper() for m in re.findall(r"(?:WITH|,)\s*([A-Za-z_][\w$]*)\s+AS\s*\(", body, flags=re.I)}
    found, funcs = [], []
    for m in re.finditer(r'\b(?:FROM|JOIN)\s+([\w$."\[\]`]+)(\s*\()?', body, flags=re.I):
        t = clean(m.group(1)).upper()
        last = t.split(".")[-1]
        if (not t or t.startswith("(") or t in ctes or last in SQL_KW or re.fullmatch(r"[\d.]+", t)
                or _inside_function(body, m.start())):
            continue
        (funcs if m.group(2) else found).append(t)
    return uniq(found), uniq(funcs)


# Usage columns that exist only for one usage source; dropped from the CSVs for the other.
CLOUD_ONLY_COLS = ["view_actors", "view_last_accessed", "view_days_since_access", "view_active_weeks",
                   "view_active_week_pct", "view_top_users"]
SNAPSHOT_ONLY_COLS = ["view_tracked_days", "view_active_intervals", "view_intervals", "view_active_interval_pct",
                      "view_last_active_between", "view_lifetime_views", "view_lifetime_avg_per_30d",
                      "view_created_at", "view_counter_reset"]


def _sum(recs, k):
    vals = [r.get(k) for r in recs if r.get(k) is not None]
    return sum(vals) if vals else None


def _minmax(recs, k, fn):
    vals = [r.get(k) for r in recs if r.get(k) is not None and r.get(k) != ""]
    return fn(vals) if vals else None


def combine_view_usage(recs, source):
    """Usage for a report: its own view plus any member sheets opened on their own tab."""
    if len(recs) == 1:
        return dict(recs[0])
    out = {"accesses": _sum(recs, "accesses")}
    if source == "admin-insights":
        # Event-level detail is not kept for Cloud; distinct users and weeks are lower bounds.
        top = max(recs, key=lambda r: r.get("accesses") or 0)
        out.update({"actors": _minmax(recs, "actors", max), "first_accessed": _minmax(recs, "first_accessed", min),
                    "last_accessed": _minmax(recs, "last_accessed", max),
                    "days_since_last_access": _minmax(recs, "days_since_last_access", min),
                    "active_weeks": _minmax(recs, "active_weeks", max),
                    "active_week_pct": _minmax(recs, "active_week_pct", max), "top_users": top.get("top_users")})
        return out
    with_iv = [r for r in recs if r.get("intervals") is not None]
    intervals = _minmax(recs, "intervals", max)
    if with_iv and all(r.get("active_interval_keys") is not None for r in with_iv):
        active = len(set().union(*(set(r["active_interval_keys"]) for r in with_iv)))
    else:
        active = _minmax(recs, "active_intervals", max)  # older view-usage.json: lower bound
    rate = _sum(recs, "lifetime_avg_views_per_30d")
    out.update({"lifetime_views": _sum(recs, "lifetime_views"),
                "lifetime_avg_views_per_30d": round(rate, 2) if rate is not None else None,
                "tracked_days": _minmax(recs, "tracked_days", max), "intervals": intervals,
                "active_intervals": active,
                "active_interval_pct": round(100 * active / intervals) if active is not None and intervals else None,
                "last_active_between": _minmax(recs, "last_active_between", max),
                "created_at": recs[0].get("created_at"),
                "counter_reset": any(r.get("counter_reset") for r in recs) if with_iv else None})
    return out


def traffic_cols(traffic, luids, window, window_weeks, present):
    """Traffic to Views history for a report's own view plus its member sheets' views."""
    if not present:
        return {}
    recs = [traffic[l] for l in luids if l and l in traffic]
    if not recs:
        # In the export window with no access events (the export lists every access).
        return {"traffic_window": window, "traffic_accesses": 0, "traffic_users": 0,
                "traffic_active_weeks": 0, "traffic_active_week_pct": 0}
    if all("user_counts" in r for r in recs):
        users, labels = defaultdict(int), {}
        for r in recs:
            for k, (label, n) in r["user_counts"].items():
                users[k] += n
                labels[k] = label
        n_users = len(users)
        top = "; ".join(f"{labels[k]} ({n})" for k, n in sorted(users.items(), key=lambda x: -x[1])[:5])
        n_weeks = len(set().union(*(set(r.get("week_keys") or []) for r in recs)))
    else:  # older traffic-usage.json without detail: lower bounds
        n_users = max(r["users"] for r in recs)
        top = max(recs, key=lambda r: r["accesses"])["top_users"]
        n_weeks = max(r["active_weeks"] for r in recs)
    return {"traffic_window": window, "traffic_accesses": sum(r["accesses"] for r in recs), "traffic_users": n_users,
            "traffic_first_accessed": min(r["first_accessed"] for r in recs),
            "traffic_last_accessed": max(r["last_accessed"] for r in recs),
            "traffic_days_since_access": min(r["days_since_last_access"] for r in recs),
            "traffic_active_weeks": n_weeks,
            "traffic_active_week_pct": round(100 * n_weeks / window_weeks) if window_weeks else None,
            "traffic_top_users": top}


def retire_assessment(r, traffic_window, traffic_start, snap_window, snap_days, min_days):
    """(retire_candidate, retire_reason). Candidates only; owners confirm before anything is retired."""
    if r["lineage_status"] == "no-datasource":
        return "n/a", "no-datasource page (cover, glossary, parameters): migrates with its workbook"
    t = r.get("traffic_accesses")
    s = r.get("view_accesses") if snap_window else None
    if t:
        return "no", f"opened {t} times in {traffic_window} (Traffic to Views)"
    if s:
        return "no", f"opened {s} times in {snap_window} (usage snapshots)"
    created = r.get("view_created_at")
    if t is not None:  # Traffic to Views present, 0 opens
        if created and traffic_start and created > traffic_start:
            return "review", f"not opened since it was created on {created}, inside the {traffic_window} window"
        why = f"not opened {traffic_window} (Traffic to Views)"
        if s == 0:
            why += f" or {snap_window} (usage snapshots)"
        return "yes", why + "; pending owner confirmation"
    if s == 0 and snap_days is not None:
        if snap_days < min_days:
            return "unknown", (f"not opened in {snap_window}, but that is only {snap_days} days "
                               f"(minimum {min_days}); import Traffic to Views or keep snapshotting")
        return "yes", f"not opened {snap_window} (usage snapshots); pending owner confirmation"
    return "unknown", "no windowed usage yet"


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
    traffic_weeks = dig(tu, "window", "window_weeks")
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
            "hosts": hosts_of(pd.get("upstreamDatabases"), "hostName"),
            "source_info": "metadata",
        }
    pub_by_luid = {p["luid"]: p for p in published.values()}

    # REST connections (step 03b) for datasources the Metadata API left blank.
    wc = load_json(out_dir, "workbook-connections.json") or {}
    wb_conns, pds_conns = wc.get("workbooks") or {}, wc.get("datasources") or {}
    filled = {"published": 0, "embedded": 0, "hosts": 0}
    for p in published.values():
        if not p["db_types"] and not p["tables"] and p["luid"] in pds_conns:
            conns = pds_conns[p["luid"]]
            p["db_types"] = uniq((c.get("type") or "").lower() for c in conns if c.get("type"))
            p["hosts"] = hosts_of(conns, "server_address")
            p["source_info"] = "rest"
            filled["published"] += bool(p["db_types"])
        elif not p["hosts"] and p["luid"] in pds_conns:
            # The Metadata API knew the database but not its server (seen with Greenplum, ODBC
            # and some extracts); REST connections often still carry the server address.
            p["hosts"] = hosts_of([c for c in pds_conns[p["luid"]]
                                   if (c.get("type") or "").lower() != "sqlproxy"], "server_address")
            filled["hosts"] += bool(p["hosts"])

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
            host = nh(dig(c, "database", "hostName"))
            parsed = []
            reg_tables, reg_funcs = regex_sources(c.get("query"))
            for t in reg_tables:
                parts = t.split(".")
                if len(parts) >= 3:
                    d, sch, name = parts[-3], parts[-2], parts[-1]
                elif len(parts) == 2:
                    d, sch, name = db or None, parts[0], parts[1]
                else:  # unqualified: the custom SQL's own database, default schema
                    d, sch, name = db or None, None, parts[0]
                k = ".".join(x for x in (d, sch, name) if x)
                parsed.append(k)
                TABLES.setdefault(k, {
                    "table_key": k, "database": d, "schema": sch, "table": name,
                    "connection_type": dig(c, "database", "connectionType"), "host": host,
                })
            parsed = uniq(parsed)
            conf = "regex" if parsed else "table-function" if reg_funcs else "unparsed"
        custom_sql[c["id"]] = {
            "id": c["id"], "name": c.get("name"), "tables": parsed, "confidence": conf,
            "table_functions": regex_sources(c.get("query"))[1],
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
                "hosts": uniq(hosts_of(ed.get("upstreamDatabases"), "hostName")
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
                    rec["hosts"] = hosts_of(hit, "server_address")
                    rec["source_info"] = "rest"
                    filled["embedded"] += bool(rec["db_types"])
            elif not rec["hosts"] and rec["db_types"] and conns and not rec["published"]:
                # Database known, server not: take it from REST, by datasource name, else from the
                # workbook's connections of the same type when they all point at one server.
                # sqlproxy connections point at Tableau Server (a published datasource), never
                # at the database, so they are not used; published datasources get their host
                # from their own connections above.
                hit = [c for c in conns_by_name.get(clean(ed.get("name")).lower()) or []
                       if (c.get("type") or "").lower() != "sqlproxy"]
                if not hit:
                    same = [c for cs in conns_by_name.values() for c in cs
                            if (c.get("type") or "").lower() in rec["db_types"]]
                    hit = same if len(set(hosts_of(same, "server_address"))) == 1 else None
                if hit:
                    rec["hosts"] = hosts_of(hit, "server_address")
                    filled["hosts"] += bool(rec["hosts"])
        # A workbook with exactly one still-unknown datasource and leftover unmatched connections:
        # those connections can only belong to it.
        still = [r for r in emb.values() if not r["db_types"] and not r["tables"]]
        left = [c for k, cs in conns_by_name.items() if k not in matched_names for c in cs]
        if len(still) == 1 and left:
            r = still[0]
            r["db_types"] = uniq((c.get("type") or "").lower() for c in left if c.get("type"))
            r["hosts"] = hosts_of(left, "server_address")
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
            # A dashboard's visible member sheets can also be opened on their own tab; those
            # opens count against the sheet's view, so they are added to the dashboard's usage.
            member_luids = (uniq(m.get("luid") for m in members if m.get("luid") and m.get("luid") != luid)
                            if rtype == "dashboard" else [])
            usage, join = view_usage.get(luid), "luid"
            if usage is None:
                usage, join = view_by_name.get(((wb.get("name") or "").upper(), (name or "").upper())), "name"
            member_recs = [view_usage[l] for l in member_luids if l in view_usage]
            if member_recs:
                if usage is None:
                    join = "members-only"
                usage = combine_view_usage(([usage] if usage else []) + member_recs, usage_source)
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
                "usage_member_views": len(member_recs) if vu else None,
                **traffic_cols(traffic, [luid] + member_luids, traffic_window, traffic_weeks, bool(tu)),
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

    # ---- retire candidates (candidates only: owners confirm before anything is retired)
    snap_window = snap_days = None
    vw = (vu or {}).get("window") or {}
    if vu and vw.get("usage_days") not in (None, "lifetime"):
        snap_days = vw["usage_days"]
        if usage_source == "admin-insights":
            snap_window = f"{vw.get('min_date')}..{str(vw.get('computed_at'))[:10]}"
        else:
            snap_window = f"{str(vw.get('first_snapshot'))[:10]}..{str(vw.get('latest_snapshot'))[:10]}"
    min_days = float(cfg("RETIRE_MIN_WINDOW_DAYS", None, "28"))
    traffic_start = dig(tu, "window", "window_start")
    for r in reports:
        r["retire_candidate"], r["retire_reason"] = retire_assessment(
            r, traffic_window, traffic_start, snap_window, snap_days, int(min_days) if min_days.is_integer() else min_days)

    # ---- columns that cannot have values for this deployment are left out
    drop = set()
    if usage_source == "rest-snapshots":
        drop |= set(CLOUD_ONLY_COLS)
    elif usage_source == "admin-insights":
        drop |= set(SNAPSHOT_ONLY_COLS)
    else:
        drop |= set(CLOUD_ONLY_COLS) | set(SNAPSHOT_ONLY_COLS) | {"usage_member_views"}
    if not shortlist and not plan:  # filled by future ports (shortlist, migration plan)
        drop |= {"wb_accesses", "wb_actors", "wb_tag", "wb_score", "wb_predicted_parity",
                 "wb_recommended_path", "wb_dm_cluster"}

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

    def report_cols(rows):
        return [c for c in uniq(k for r in rows for k in r) if c not in drop]

    write_csv("reports.csv", reports, report_cols(reports))
    write_csv("host-aliases.csv", [{"raw_host": r, "normalized_host": n, "times_seen": c}
                                   for (r, n), c in sorted(HOST_ALIASES.items(), key=lambda x: (x[0][1], x[0][0]))],
              ["raw_host", "normalized_host", "times_seen"])
    write_csv("unpublished_dashboards.csv", unpublished, report_cols(unpublished))
    write_csv("edges_report_datasource.csv", edges_rep_ds)
    write_csv("edges_datasource_table.csv", ds_tables_rows)
    write_csv("tables.csv", table_rows,
              "table_key database schema table connection_type host n_reports n_workbooks "
              "n_datasources sum_wb_accesses sum_report_accesses read_by_prep_flows".split())
    write_csv("custom_sql.csv", [{**c, "tables": "; ".join(c["tables"]),
                                  "table_functions": "; ".join(c.get("table_functions") or []),
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
    rc = Counter(r.get("retire_candidate") for r in reports)
    log(f"retire candidates (pending owner confirmation): {rc.get('yes', 0)}; review (new): {rc.get('review', 0)}; "
        f"keep: {rc.get('no', 0)}; unknown: {rc.get('unknown', 0)}; n/a (no-datasource pages): {rc.get('n/a', 0)}")
    if vu:
        joins = [r["usage_join"] for r in reports]
        log(f"view usage ({usage_source}, window {usage_days}): {joins.count('luid')} by LUID, {joins.count('name')} by name, "
            f"{joins.count('no-events')} with no access events, {joins.count('not-in-snapshot')} not in snapshot")
    if raw.get("errors"):
        log(f"fetch errors: {', '.join(raw['errors'])}")
    write_summary(Path(out_dir), dest, reports, unpublished, datasources, custom_sql, restricted, filled,
                  bool(wc), tu, vu, raw)
    log(f"Wrote {dest}/")


def write_summary(out_dir, dest, reports, unpublished, datasources, custom_sql, restricted, filled,
                  had_wc, tu, vu, raw):
    site = out_dir.name
    rows = []

    def add(section, metric, value, desc=""):
        rows.append({"site": site, "section": section, "metric": metric,
                     "value": "" if value is None else value, "description": desc})

    conn = load_json(out_dir, "connectivity.json") or {}
    add("site", "generated_at", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"), "when step 10 ran")
    add("site", "site_role", conn.get("site_role"), "PAT owner's role (full visibility needs Site Administrator)")
    add("site", "code_version", code_version(), "tableau-assessment-py version that produced these numbers")

    data = [r for r in reports if r["lineage_status"] != "no-datasource"]
    add("inventory", "workbooks", len({r["workbook_luid"] for r in reports}), "workbooks with at least one report")
    add("inventory", "reports", len(reports), "dashboards + standalone visible sheets")
    add("inventory", "dashboards", sum(r["report_type"] == "dashboard" for r in reports))
    add("inventory", "standalone_sheets", sum(r["report_type"] == "sheet" for r in reports))
    add("inventory", "data_bearing_reports", len(data), "reports that use a datasource (the migration units)")
    add("inventory", "no_datasource_pages", len(reports) - len(data), "cover, glossary and parameter pages")
    add("inventory", "unpublished_dashboards", len(unpublished), "no LUID; excluded from reports")
    for k, v in restricted.items():
        add("inventory", f"restricted_{k}", v, "not visible to the PAT owner")

    kinds = Counter(d["kind"] for d in datasources.values())
    add("datasources", "datasources", len(datasources))
    for k in ("published", "embedded", "embedded-proxy"):
        add("datasources", k, kinds.get(k, 0))
    add("datasources", "with_extracts", sum(1 for d in datasources.values() if d.get("extract")))
    if had_wc:
        add("datasources", "hosts_filled_by_rest", filled["hosts"],
            "database known from the Metadata API but not its server; server address taken from step 03b")
        add("datasources", "sources_filled_by_rest", filled["embedded"] + filled["published"],
            "unknown sources resolved by step 03b")

    st = Counter(r["lineage_status"] for r in reports)
    for k, desc in LINEAGE_STATUS_HELP.items():
        add("lineage", f"status_{k}", st.get(k, 0), desc)
    add("lineage", "warehouse_tables", len(TABLES))
    confs = Counter(c["confidence"] for c in custom_sql.values())
    add("lineage", "custom_sql_blocks", len(custom_sql))
    for k in ("tableau", "regex", "table-function", "unparsed", "restricted"):
        add("lineage", f"custom_sql_{k.replace('-', '_')}", confs.get(k, 0),
            {"tableau": "tables parsed by Tableau", "regex": "tables parsed by FROM/JOIN extraction",
             "table-function": "reads only table functions (UDFs) in FROM/JOIN: rebuild in Snowflake",
             "unparsed": "needs manual review", "restricted": "not visible to the PAT owner"}[k])
    add("lineage", "custom_sql_with_table_functions",
        sum(1 for c in custom_sql.values() if c.get("table_functions")),
        "custom SQL blocks calling table functions in FROM/JOIN (custom_sql.csv table_functions)")

    by_type, by_host = Counter(), Counter()
    for r in data:
        for t in filter(None, r["source_types"].split("; ")):
            by_type[t] += 1
        for h in filter(None, r["source_hosts"].split("; ")):
            by_host[h] += 1
    add("source_types", "distinct_types", len(by_type))
    for t, n in by_type.most_common():
        add("source_types", t, n, "data-bearing reports using this connection type")
    add("source_hosts", "distinct_hosts", len(by_host))
    for h, n in by_host.most_common():
        add("source_hosts", h, n, "data-bearing reports using this server")

    if vu:
        w = vu.get("window") or {}
        add("usage_snapshots", "source", w.get("source"))
        add("usage_snapshots", "snapshots", w.get("snapshots"))
        add("usage_snapshots", "window_days", w.get("usage_days"), "first to latest snapshot")
        if w.get("usage_days") != "lifetime":
            add("usage_snapshots", "reports_accessed", sum(1 for r in reports if (r.get("view_accesses") or 0) > 0),
                "reports opened between first and latest snapshot")
    if tu:
        w = tu.get("window") or {}
        add("usage_traffic", "window_start", w.get("window_start"))
        add("usage_traffic", "window_end", w.get("window_end"))
        add("usage_traffic", "window_days", w.get("window_days"))
        add("usage_traffic", "access_events", w.get("events"))
        add("usage_traffic", "views_matched", w.get("matched_views"))
        add("usage_traffic", "views_unmatched", w.get("unmatched_views"), "had traffic, no longer exist")
        hit = [r for r in reports if (r.get("traffic_accesses") or 0) > 0]
        add("usage_traffic", "reports_accessed", len(hit), "reports opened in the window")
        add("usage_traffic", "data_bearing_not_accessed", sum(1 for r in data if not (r.get("traffic_accesses") or 0)),
            "data-bearing reports with no opens in the traffic window")
    rc = Counter(r.get("retire_candidate") for r in reports)
    add("retire", "candidates_to_retire", rc.get("yes", 0), "retire_candidate = yes; pending owner confirmation")
    add("retire", "review", rc.get("review", 0), "created inside the usage window; too new to judge")
    add("retire", "keep", rc.get("no", 0), "opened in a usage window")
    add("retire", "unknown", rc.get("unknown", 0), "not enough windowed usage to judge")
    add("retire", "not_applicable", rc.get("n/a", 0), "no-datasource pages")
    if raw.get("errors"):
        add("site", "fetch_errors", ", ".join(raw["errors"]), "see report-lineage-raw.json")

    # Sections written by later steps (11, 12) survive a re-run of this step.
    later = []
    if (dest / "summary.csv").exists():
        with open(dest / "summary.csv", encoding="utf-8") as fh:
            later = [r for r in csv.DictReader(fh) if r["section"] in LATER_SECTIONS]
    write_summary_rows(dest, rows + later)
    rebuild_cross_site(out_dir)


SUMMARY_COLS = ["site", "section", "metric", "value", "description"]
SECTION_ORDER = ["site", "inventory", "datasources", "lineage", "source_types", "source_hosts",
                 "usage_snapshots", "usage_traffic", "retire", "workbooks", "pipelines", "gaps", "consolidation"]
LATER_SECTIONS = {"workbooks", "pipelines", "gaps", "consolidation"}


def write_summary_rows(dest, rows):
    rows = sorted(rows, key=lambda r: SECTION_ORDER.index(r["section"]) if r["section"] in SECTION_ORDER
                  else len(SECTION_ORDER))  # stable: keeps metric order within a section
    with open(dest / "summary.csv", "w", newline="", encoding="utf-8") as fh:
        wr = csv.DictWriter(fh, fieldnames=SUMMARY_COLS, lineterminator="\n")
        wr.writeheader()
        wr.writerows(rows)


def replace_summary_section(dest, site, section, metrics):
    """Replace one section of a site's summary.csv with (metric, value, description) rows."""
    path = dest / "summary.csv"
    if not path.exists():
        raise SystemExit(f"missing {path}; run step 10 first")
    with open(path, encoding="utf-8") as fh:
        rows = [r for r in csv.DictReader(fh) if r["section"] != section]
    rows += [{"site": site, "section": section, "metric": m, "value": "" if v is None else v, "description": d}
             for m, v, d in metrics]
    write_summary_rows(dest, rows)


def rebuild_cross_site(out_dir):
    """Rebuild output/_server/ rollups from every sibling site folder's summary.csv."""
    out_dir = Path(out_dir)
    cols = SUMMARY_COLS
    root = out_dir.parent
    stacked, order, sites = [], [], []
    for p in sorted(root.glob("*/report-inventory/summary.csv")):
        with open(p, encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                stacked.append(r)
                key = (r["section"], r["metric"])
                if key not in order:
                    order.append(key)
                if r["site"] not in sites:
                    sites.append(r["site"])
    if not stacked:
        return
    stacked, order = merge_host_rows(stacked)
    sections = SECTION_ORDER
    order.sort(key=lambda k: sections.index(k[0]) if k[0] in sections else len(sections))
    if ("source_types", "distinct_types") not in order and any(k[0] == "source_types" for k in order):
        order.insert(next(i for i, k in enumerate(order) if k[0] == "source_types"), ("source_types", "distinct_types"))
    val = {(r["section"], r["metric"], r["site"]): r["value"] for r in stacked}
    desc = {(r["section"], r["metric"]): r["description"] for r in stacked if r["description"]}
    desc.setdefault(("source_types", "distinct_types"), "distinct connection types; full list in summary-sources.csv")
    desc.setdefault(("source_hosts", "distinct_hosts"), "distinct database servers; full list in summary-sources.csv")
    access = site_access(root)
    # Sites below Site Administrator (PARTIAL) or without access stay out of the totals: their
    # inventory covers only what the PAT owner can open. Their size is reported under coverage.
    in_total = [s for s in sites if access.get(s, {}).get("access", "FULL") == "FULL"]
    excluded = [s for s in sites if s not in in_total]
    totals, how = rollup_totals(root, order, in_total, val)
    # Pipeline and gap counts share tables and features across sites, so step 11 computes them
    # over all full-access sites together.
    pt = root / "_server" / "step11-totals.json"
    if pt.exists():
        try:
            ptj = json.loads(pt.read_text(encoding="utf-8"))
        except ValueError:
            ptj = {}
        for k, v in (ptj.get("totals") or {}).items():
            key = tuple(k.split("|", 1))
            if key in totals:
                totals[key] = v
                how[key] = f"computed across {len(ptj.get('sites') or [])} site(s) together (step 11)"
    coverage = coverage_rows(access, in_total, excluded)

    server = root / "_server"
    server.mkdir(parents=True, exist_ok=True)
    with open(server / "summary-all-sites.csv", "w", newline="", encoding="utf-8") as fh:
        wr = csv.DictWriter(fh, fieldnames=cols, lineterminator="\n")
        wr.writeheader()
        wr.writerows(stacked)
        wr.writerows({"site": "ALL_SITES", "section": "coverage", "metric": m, "value": v, "description": d}
                     for m, v, _, d in coverage)
        wr.writerows({"site": "ALL_SITES", "section": sec, "metric": met, "value": totals[(sec, met)],
                      "description": desc.get((sec, met), "")} for sec, met in order)
    with open(server / "summary-all-sites-wide.csv", "w", newline="", encoding="utf-8") as fh:
        wr = csv.writer(fh, lineterminator="\n")
        wr.writerow(["section", "metric", "ALL_SITES", "rollup"]
                    + [s if s in in_total else f"{s} (PARTIAL, not in totals)" for s in sites] + ["description"])
        for m, v, h, d in coverage:
            wr.writerow(["coverage", m, v, h] + [""] * len(sites) + [d])
        for sec, met in order:
            if is_source_detail(sec, met):
                continue
            wr.writerow([sec, met, totals[(sec, met)], how[(sec, met)]]
                        + [val.get((sec, met, s), "") for s in sites] + [desc.get((sec, met), "")])
    with open(server / "summary-rollup.csv", "w", newline="", encoding="utf-8") as fh:
        wr = csv.writer(fh, lineterminator="\n")
        wr.writerow(["section", "metric", "value", "rollup", "description"])
        for m, v, h, d in coverage:
            wr.writerow(["coverage", m, v, h, d])
        wr.writerow(["site", "sites", len(in_total), "count", "sites in the totals below: " + ", ".join(in_total)])
        for sec, met in order:
            if how[(sec, met)] != "per-site only" and not is_source_detail(sec, met):
                wr.writerow([sec, met, totals[(sec, met)], how[(sec, met)], desc.get((sec, met), "")])
    # Source types and hosts: one row each, sites side by side, busiest first.
    with open(server / "summary-sources.csv", "w", newline="", encoding="utf-8") as fh:
        wr = csv.writer(fh, lineterminator="\n")
        wr.writerow(["kind", "name", "ALL_SITES"] + in_total)
        for sec, kind in (("source_types", "connection type"), ("source_hosts", "database host")):
            keys = [k for k in order if k[0] == sec and is_source_detail(*k)]
            keys.sort(key=lambda k: -(_num(totals[k]) or 0))
            for k in keys:
                wr.writerow([kind, k[1], totals[k]] + [val.get((k[0], k[1], s), "") for s in in_total])
    log(f"Summary: {out_dir / 'report-inventory' / 'summary.csv'}; rollup across {len(in_total)} site(s): {server / 'summary-rollup.csv'} "
        f"(source types and hosts: summary-sources.csv)")
    cov = {m: v for m, v, _, _ in coverage}
    if cov.get("sites_pending_access"):
        log(f"Coverage: {cov['workbooks_assessed']} workbooks assessed, {cov['workbooks_not_assessed']} not assessed "
            f"({cov['coverage_pct']}% coverage); pending access: {cov['sites_pending_access']}")


def merge_host_rows(stacked):
    """Normalize source_hosts names in every site's rows and add up spellings of one server."""
    merged, order = {}, []
    for r in stacked:
        r = dict(r)
        if r["section"] == "source_hosts" and not r["metric"].startswith("distinct_"):
            r["metric"] = normalize_host(r["metric"]) or r["metric"]
        k = (r["site"], r["section"], r["metric"])
        if k in merged and r["section"] == "source_hosts":
            merged[k]["value"] = (_num(merged[k]["value"]) or 0) + (_num(r["value"]) or 0)
        else:
            merged[k] = r
        if (r["section"], r["metric"]) not in order:
            order.append((r["section"], r["metric"]))
    # A site's own distinct_hosts may predate normalization: recount it from its merged rows.
    for site in {k[0] for k in merged}:
        n = sum(1 for (s, sec, met) in merged if s == site and sec == "source_hosts" and not met.startswith("distinct_"))
        if (site, "source_hosts", "distinct_hosts") in merged:
            merged[(site, "source_hosts", "distinct_hosts")]["value"] = n
    return list(merged.values()), order


def is_source_detail(sec, met):
    """Per-type and per-host rows: kept in summary-sources.csv, not in the main rollup."""
    return sec in ("source_types", "source_hosts") and not met.startswith("distinct_")


# How each metric combines across sites. Anything not listed is summed when every value is a number.
PER_SITE_ONLY = {("site", "generated_at"), ("site", "site_role"), ("site", "fetch_errors"),
                 ("usage_snapshots", "source"), ("usage_snapshots", "snapshots"),
                 ("usage_snapshots", "window_days"), ("usage_traffic", "window_days")}
EARLIEST = {("usage_traffic", "window_start")}
# Averages: combined across sites weighted by the count each one is taken over.
WEIGHTED_AVG = {("workbooks", "avg_predicted_parity_pct"): ("workbooks", "parity_scanned")}
LATEST = {("usage_traffic", "window_end"), ("site", "code_version")}


def _num(v):
    try:
        f = float(v)
        return int(f) if f.is_integer() else f
    except (TypeError, ValueError):
        return None


def site_access(root):
    """Per site folder: role, access level and workbook counts from its latest step 00 check."""
    out = {}
    for p in root.glob("*/connectivity.json"):
        if p.parent.name.startswith("_"):
            continue
        try:
            c = json.loads(p.read_text(encoding="utf-8"))
        except ValueError:
            continue
        role, signin = c.get("site_role") or "", c.get("sign_in")
        if signin == "failed":
            level = "NO ACCESS"
        elif role.startswith(("SiteAdministrator", "ServerAdministrator")):
            level = "FULL"
        elif role:
            level = "PARTIAL"
        else:
            level = "UNKNOWN"
        out[p.parent.name] = {"access": level, "role": role, "checked_at": c.get("checked_at"),
                              "site_workbooks": c.get("metadata_workbooks"),
                              "visible_workbooks": c.get("rest_workbooks")}
    return out


def coverage_rows(access, in_total, excluded):
    """(metric, value, rollup, description) rows describing how much of the estate is assessed."""
    known = dict(access)
    full = [s for s, a in known.items() if a["access"] == "FULL"]
    partial = [s for s, a in known.items() if a["access"] == "PARTIAL"]
    none = [s for s, a in known.items() if a["access"] == "NO ACCESS"]
    assessed = sum(known[s]["site_workbooks"] or 0 for s in in_total if s in known)
    not_assessed = sum(known[s]["site_workbooks"] or 0 for s in partial)
    total = assessed + not_assessed
    pending = sorted(set(partial) | set(none))
    checked = max((a["checked_at"] or "" for a in known.values()), default="")[:10]
    return [
        ("sites_full_access", len(full), "count", "Site/Server Administrator: complete inventory"),
        ("sites_partial_access", len(partial), "count", "lower role: inventory left out of the totals"),
        ("sites_no_access", len(none), "count", "sign-in refused"),
        ("workbooks_assessed", assessed, "sum", "workbooks on sites in the totals (Metadata API count)"),
        ("workbooks_not_assessed", not_assessed, "sum",
         "at least this many workbooks on PARTIAL sites, pending Site Administrator access"),
        ("coverage_pct", round(100 * assessed / total, 1) if total else "", "assessed / (assessed + not assessed)",
         "share of known workbooks covered; sites with no access are not counted"),
        ("sites_pending_access", ", ".join(f"{s} ({known[s]['role'] or known[s]['access']})" for s in pending),
         "list", "grant Site Administrator to complete the assessment"),
        ("sites_excluded_from_totals", ", ".join(excluded), "list", "ran step 10 but below Site Administrator"),
        ("access_checked_at", checked, "latest", "most recent step 00 check across sites"),
    ]


def rollup_totals(root, order, sites, val):
    """Combine each metric across the given sites. Returns ({key: value}, {key: how it was combined})."""
    totals, how = {}, {}
    # Distinct counts for things several sites can share (counted once, not per site).
    tables = set()
    for s in sites:
        p = root / s / "report-inventory" / "tables.csv"
        if p.exists():
            with open(p, encoding="utf-8") as fh:
                tables |= {r["table_key"] for r in csv.DictReader(fh) if r.get("table_key")}
    hosts = {met for sec, met in order if sec == "source_hosts" and met != "distinct_hosts"
             and any(val.get((sec, met, s)) not in (None, "") for s in sites)}
    for key in order:
        vals = [val.get((key[0], key[1], s), "") for s in sites]
        present = [v for v in vals if v not in ("", None)]
        if key == ("lineage", "warehouse_tables"):
            totals[key], how[key] = len(tables), "distinct across sites"
        elif key == ("source_hosts", "distinct_hosts"):
            totals[key], how[key] = len(hosts), "distinct across sites"
        elif key == ("source_types", "distinct_types"):
            types = {met for sec, met in order if sec == "source_types" and met != "distinct_types"
                     and any(val.get((sec, met, s)) not in (None, "") for s in sites)}
            totals[key], how[key] = len(types), "distinct across sites"
        elif key in WEIGHTED_AVG:
            wkey = WEIGHTED_AVG[key]
            pairs = [(_num(val.get((key[0], key[1], s))), _num(val.get((wkey[0], wkey[1], s)))) for s in sites]
            pairs = [(v, w) for v, w in pairs if v is not None and w]
            n = sum(w for _, w in pairs)
            totals[key] = round(sum(v * w for v, w in pairs) / n, 1) if n else ""
            how[key] = f"average weighted by {wkey[1]}" + (f" ({len(pairs)} of {len(sites)} sites)"
                                                           if len(pairs) < len(sites) else "")
        elif key in PER_SITE_ONLY:
            totals[key], how[key] = "", "per-site only"
        elif key in EARLIEST:
            totals[key], how[key] = (min(present) if present else ""), "earliest"
        elif key in LATEST:
            totals[key], how[key] = (max(present) if present else ""), "latest"
        else:
            nums = [_num(v) for v in present]
            if present and all(n is not None for n in nums):
                totals[key], how[key] = sum(nums), "sum"
                # A missing connection type or host just means zero on that site; elsewhere a
                # missing value means the site has not run that step, which the label says.
                if len(present) < len(sites) and key[0] not in ("source_types", "source_hosts"):
                    how[key] = f"sum of {len(present)} of {len(sites)} sites"
            else:
                totals[key], how[key] = "", "per-site only"
    return totals, how

if __name__ == "__main__":
    main()
