#!/usr/bin/env python3
"""Step 03: fetch report-level lineage (dashboards, sheets, datasources, tables,
custom SQL, Prep flows) from the Tableau Metadata API.

Usage:
    python scripts/fetch_report_lineage.py --site <site> [--page-size 20]

Writes <out>/report-lineage-raw.json (default output/<site>/):
    { fetched_at, workbooks, published_datasources, custom_sql, flows, errors }

Each section is fetched independently. If one errors (a field missing on this
Tableau version, permissions, node limits) the others still land and the
error is recorded under "errors" instead of aborting the run.
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib.tableau_client import TableauClient, TableauError, add_site_args, log, resolve_site_args  # noqa: E402

# ---------------------------------------------------------------- queries
# Some fields return the Table interface (e.g. Sheet.upstreamTables), whose concrete
# types are DatabaseTable and CustomSQLTable. Only id/name live on the interface, so
# database-table fields go in an inline fragment. The fragment is also valid where
# the field is already typed DatabaseTable, so one definition serves every place.
TABLE_FIELDS = """
  __typename id name
  ... on DatabaseTable {
    schema fullName connectionType
    database { id name connectionType ... on DatabaseServer { hostName } }
  }
"""

SECTIONS = {
    "workbooks": f"""
query($first: Int!, $after: String) {{
  workbooksConnection(first: $first, after: $after) {{
    pageInfo {{ hasNextPage endCursor }}
    nodes {{
      id luid name projectName updatedAt
      owner {{ email username }}
      sheets {{
        id luid name path
        containedInDashboards {{ id luid name }}
        upstreamDatasources {{ id name }}
        upstreamTables {{ {TABLE_FIELDS} }}
      }}
      dashboards {{
        id luid name path
        sheets {{ id luid name }}
      }}
      embeddedDatasources {{
        id name hasExtracts
        upstreamDatasources {{ id luid name projectName }}
        upstreamDatabases {{ id name connectionType ... on DatabaseServer {{ hostName }} }}
        upstreamTables {{ {TABLE_FIELDS} }}
      }}
    }}
  }}
}}""",
    "published_datasources": f"""
query($first: Int!, $after: String) {{
  publishedDatasourcesConnection(first: $first, after: $after) {{
    pageInfo {{ hasNextPage endCursor }}
    nodes {{
      id luid name projectName isCertified hasExtracts extractLastUpdateTime
      owner {{ email }}
      upstreamDatabases {{ id name connectionType ... on DatabaseServer {{ hostName }} }}
      upstreamTables {{ {TABLE_FIELDS} }}
    }}
  }}
}}""",
    "custom_sql": """
query($first: Int!, $after: String) {
  customSQLTablesConnection(first: $first, after: $after) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id name query isUnsupportedCustomSql
      database { id name connectionType }
      __CUSTOM_SQL_TABLES__
      downstreamDatasources { id }
      downstreamSheets { id }
    }
  }
}""",
    "flows": f"""
query($first: Int!, $after: String) {{
  flowsConnection(first: $first, after: $after) {{
    pageInfo {{ hasNextPage endCursor }}
    nodes {{
      id luid name projectName
      upstreamTables {{ {TABLE_FIELDS} }}
      downstreamDatasources {{ id luid name }}
    }}
  }}
}}""",
}

PLACEHOLDER = "__CUSTOM_SQL_TABLES__"


def primary(name, query):
    # CustomSQLTable.tables (Tableau's own parse of the SQL) is the field most
    # likely to be missing on older Server versions.
    if name == "custom_sql":
        return query.replace(PLACEHOLDER, f"tables {{ {TABLE_FIELDS} }}")
    return query


FALLBACKS = {"custom_sql": lambda q: q.replace(PLACEHOLDER, "")}


def fetch_section(client, name, query, page_size):
    """Page through one connection. Returns (nodes, errors, warnings).

    A response with errors and no data (e.g. a schema validation error) stops the
    section. A response with data AND errors (partial results) keeps the data and
    records the errors as warnings.
    """
    nodes, after, warnings, pages = [], None, [], 0
    while True:
        resp = client.graphql(query, {"first": page_size, "after": after})
        data = resp.get("data")
        conn = next(iter(data.values()), None) if data else None
        if resp.get("errors") and not conn:
            return nodes, resp["errors"], warnings
        if resp.get("errors"):
            warnings.extend(resp["errors"])
        nodes.extend(conn.get("nodes") or [])
        pages += 1
        page = conn.get("pageInfo") or {}
        more = page.get("hasNextPage")
        if pages % 10 == 0 or not more:
            log(f"  {name}: {len(nodes)}")
        if not more:
            return nodes, None, warnings
        after = page.get("endCursor")


def redacted(nodes):
    """Nodes the Metadata API returned with their identifying fields nulled.

    The Metadata API lists assets the signed-in user cannot open but returns
    their fields as null. A node with neither name nor query is one of those.
    """
    return sum(1 for n in nodes if n.get("name") is None and n.get("query") is None)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    add_site_args(ap)
    ap.add_argument("--page-size", type=int, default=20,
                    help="nodes per GraphQL page; lower it if you hit node-limit errors")
    args = ap.parse_args()
    site, out_dir = resolve_site_args(args)

    out = {"fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "errors": {},
           "warnings": {}, "redacted": {}}
    with TableauClient(site) as client:
        for name, query in SECTIONS.items():
            log(f"Fetching {name} ...")
            warns = []
            try:
                nodes, errs, warns = fetch_section(client, name, primary(name, query), args.page_size)
                if errs and name in FALLBACKS:
                    log(f"  {name}: GraphQL error, retrying without version-sensitive fields")
                    out["errors"][f"{name}_primary"] = errs
                    nodes, errs, warns = fetch_section(client, name, FALLBACKS[name](query), args.page_size)
            except TableauError as e:
                nodes, errs = [], [{"message": str(e)}]
            out[name] = nodes
            if errs:
                out["errors"][name] = errs
                log(f"  {name}: FAILED: {errs[0].get('message', errs[0])}"[:300])
            if warns:
                out["warnings"][name] = warns
            n_red = redacted(nodes)
            if n_red:
                out["redacted"][name] = n_red
                log(f"  {name}: {n_red} of {len(nodes)} returned without names (no permission to view)")

    out["site"] = site or "default"
    path = out_dir / "report-lineage-raw.json"
    path.write_text(json.dumps(out, indent=2))
    log(f"Wrote {path}")
    if out["errors"]:
        log(f"Sections with errors: {', '.join(out['errors'])} (see 'errors' in the JSON)")
    if out["redacted"]:
        log("Some assets came back without names: the PAT owner lacks permission on them. "
            "A Site Administrator role on this site gives complete results.")
    if "workbooks" in out["errors"]:
        log("ERROR: the workbooks section failed, so there are no reports to inventory. "
            "Fix the error above before building the inventory.")
        sys.exit(1)


if __name__ == "__main__":
    main()
