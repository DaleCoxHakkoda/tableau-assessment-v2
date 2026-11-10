#!/usr/bin/env python3
"""Step 00: check that everything the assessment needs is reachable, before a real run.

Usage:
    python scripts/check_connectivity.py --site <site>
    python scripts/check_connectivity.py --all-sites     # every site in TABLEAU_SITES (or, Server Admin, every site)
    python scripts/check_connectivity.py --all-sites --roles-only   # fast access audit: role + visibility per site

Checks, in order (each reports PASS / WARN / FAIL / SKIP with a fix):
  config         server URL and a PAT resolved for the site (incl. per-site overrides)
  reach          GET serverinfo: network, TLS, product and REST versions, Server vs Cloud
  sign-in        one PAT sign-in attempt (never retried; four failures lock a PAT)
  role           site role of the PAT owner; Server/Site Admin needed for a complete inventory
  sites          with --all-sites: the sites in TABLEAU_SITES, or (Server Admin only) every site on the server
  content        workbook and view counts on the site
  metadata-api   Metadata API enabled, its index covers every workbook REST sees, and whether
                 it is returning assets this PAT cannot open (their fields come back null)
  lineage-query  each step 03 query runs against this server's Metadata API schema (first: 1)
  rest-usage     REST view usage counters (Server's only usage source; step 02 snapshots them)
  snapshots      usage snapshots already saved for the site, and the window they cover
  admin-insights (Cloud) Admin Insights datasources visible

Writes output/<site>/connectivity.json. With more than one site, also prints an access
summary and writes output/_server/access-summary.csv and access-summary.md (ready to paste
into an access request). Exits 1 if any check FAILs.

Sites are checked one after another: a PAT holds one session at a time. Four consecutive
failed sign-ins lock a PAT, so a multi-site check stops signing in after two consecutive
failures and marks the remaining sites "not attempted".
"""

import argparse
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import fetch_report_lineage as lineage  # noqa: E402
from lib.tableau_client import (REPO_ROOT, HTTPStatusError, TableauClient, TableauError,  # noqa: E402
                                cfg, configured_sites, ensure_env, log, normalize_site, site_folder, site_key)

PASS, WARN, FAIL, SKIP = "PASS", "WARN", "FAIL", "SKIP"


class Report:
    def __init__(self, site):
        self.site, self.results = site, []

    def add(self, check, status, detail, fix=None):
        self.results.append({"check": check, "status": status, "detail": detail, "fix": fix})

    def failed(self):
        return any(r["status"] == FAIL for r in self.results)

    def print(self):
        print(f"\n== Site '{site_folder(self.site)}'")
        for r in self.results:
            print(f"  [{r['status']}] {r['check']:<15} {r['detail']}")
            if r["fix"] and r["status"] in (WARN, FAIL):
                print(f"  {'':<22}fix: {r['fix']}")

    def save(self, extra=None):
        out = REPO_ROOT / "output" / site_folder(self.site)
        out.mkdir(parents=True, exist_ok=True)
        doc = {"checked_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
               "site": site_folder(self.site), "results": self.results, **(extra or {})}
        (out / "connectivity.json").write_text(json.dumps(doc, indent=2, default=str))


def total_available(client, path):
    resp = client.rest("GET", f"{path}{'&' if '?' in path else '?'}pageSize=1&pageNumber=1")
    return int((resp.get("pagination") or {}).get("totalAvailable") or 0)


def snapshot_status(rep, site):
    snap_dir = REPO_ROOT / "output" / site_folder(site) / "usage-snapshots"
    snaps = sorted(snap_dir.glob("*.json")) if snap_dir.exists() else []
    if not snaps:
        rep.add("snapshots", WARN, "none yet; windowed usage starts with the second step 02 run",
                "Run step 02 now for a baseline, then schedule it weekly for the length of the assessment")
        return
    first, last = snaps[0].stem, snaps[-1].stem
    span = (datetime.strptime(last, "%Y%m%dT%H%M%SZ") - datetime.strptime(first, "%Y%m%dT%H%M%SZ")).days
    age = (datetime.now(timezone.utc).replace(tzinfo=None) - datetime.strptime(last, "%Y%m%dT%H%M%SZ")).days
    status = WARN if age > 14 else PASS
    rep.add("snapshots", status, f"{len(snaps)} snapshots covering {span} days; latest {age} days ago",
            "Latest snapshot is over two weeks old: run step 02 (this also keeps the PAT from expiring)"
            if status == WARN else None)


ADMIN_ROLES = ("SiteAdministrator", "ServerAdministrator")
MAX_CONSECUTIVE_SIGNIN_FAILURES = 2


def check_site(site, list_sites=False, roles_only=False):
    rep = Report(site)
    extra = {"sign_in": "not attempted"}
    server = cfg("TABLEAU_SERVER_URL", site)
    pat = cfg("TABLEAU_PAT_NAME", site)
    if not server or not pat or not cfg("TABLEAU_PAT_SECRET", site):
        rep.add("config", FAIL, f"server URL set: {bool(server)}, PAT name: {pat or 'missing'}",
                f"Set TABLEAU_SERVER_URL and TABLEAU_PAT_NAME / TABLEAU_PAT_SECRET (or __{site_key(site)} overrides)")
        return rep, extra
    rep.add("config", PASS, f"{server} | PAT '{pat}'")

    client = TableauClient(site)
    try:
        info = client.server_info()
        client.version = client.version or info.get("restApiVersion")
        extra["product_version"], extra["rest_api_version"] = client.product_version, client.version
        extra["deployment"] = client.deployment
        rep.add("reach", PASS, f"{client.deployment}, product {client.product_version}, REST {client.version}")
    except TableauError as e:
        rep.add("reach", FAIL, str(e), "Check VPN / firewall / proxy; set TABLEAU_CA_BUNDLE for an internal CA")
        return rep, extra

    try:
        client.sign_in()
        extra["sign_in"] = "ok"
        rep.add("sign-in", PASS, f"site id {client.site_id}")
    except HTTPStatusError as e:
        extra["sign_in"] = "failed"
        rep.add("sign-in", FAIL, f"HTTP {e.status}: {str(e)[-200:]}",
                "Wrong PAT name/secret, PAT expired (15 days unused or 1 year old), or the PAT owner is not a "
                "member of this site. Fix before re-running: four consecutive failures lock the PAT.")
        return rep, extra

    try:
        try:
            role = client.current_user().get("siteRole", "unknown")
        except TableauError as e:
            role = f"unknown ({e})"
        extra["site_role"] = role
        if role.startswith(ADMIN_ROLES):
            rep.add("role", PASS, f"{role}: full visibility of this site's content")
        else:
            rep.add("role", WARN, f"{role}: inventory will only include content this user can open, "
                                  "and the Metadata API will return the rest with null fields",
                    "Ask for Site Administrator on this site; otherwise the inventory is partial")

        if list_sites:
            try:
                sites = client.list_sites()
                extra["sites"] = [{"name": s.get("name"), "contentUrl": s.get("contentUrl") or ""} for s in sites]
                rep.add("sites", PASS, f"{len(sites)} sites: " +
                        ", ".join(site_folder(s.get('contentUrl') or '') for s in sites))
            except TableauError as e:
                rep.add("sites", WARN, "cannot list sites (Server Administrator only)",
                        "List the sites to assess in .env: TABLEAU_SITES=finance,ops,default")

        try:
            n_wb, n_views = total_available(client, "/workbooks"), total_available(client, "/views")
            extra["rest_workbooks"], extra["rest_views"] = n_wb, n_views
            rep.add("content", PASS if n_wb else WARN, f"{n_wb} workbooks, {n_views} views visible to this PAT")
        except TableauError as e:
            n_wb = None
            rep.add("content", FAIL, str(e))

        try:
            resp = client.graphql("{ workbooksConnection(first: 1) { totalCount } }")
            if resp.get("errors"):
                raise TableauError(json.dumps(resp["errors"])[:300])
            n_meta = resp["data"]["workbooksConnection"]["totalCount"]
            extra["metadata_workbooks"] = n_meta
            is_admin = extra.get("site_role", "").startswith(ADMIN_ROLES)
            if n_wb is not None and n_meta < n_wb:
                rep.add("metadata-api", WARN, f"enabled, but indexes {n_meta} of {n_wb} workbooks",
                        "Metadata API backfill may still be running after enablement; re-check later, or reindex "
                        "with `tsm maintenance metadata-services` per Tableau docs")
            elif n_wb is not None and n_meta > n_wb and not is_admin:
                rep.add("metadata-api", WARN,
                        f"lists {n_meta} workbooks but this PAT can open {n_wb}; the other {n_meta - n_wb} "
                        "will come back with null names and lineage",
                        "Get Site Administrator on this site for a complete inventory")
            else:
                rep.add("metadata-api", PASS, f"enabled, {n_meta} workbooks indexed")
            for sec, q in ({} if roles_only else lineage.SECTIONS).items():
                r = client.graphql(lineage.primary(sec, q), {"first": 1, "after": None})
                if r.get("errors") and not r.get("data"):
                    msg = r["errors"][0].get("message", str(r["errors"][0]))
                    if sec in lineage.FALLBACKS:
                        r2 = client.graphql(lineage.FALLBACKS[sec](q), {"first": 1, "after": None})
                        if not (r2.get("errors") and not r2.get("data")):
                            rep.add(f"lineage-{sec}"[:15], WARN, f"primary query failed, fallback works: {msg}"[:300])
                            continue
                    rep.add(f"lineage-{sec}"[:15], FAIL, f"{msg} ({len(r['errors'])} error(s))"[:300],
                            "Metadata API schema differs on this version; send this message so the query can be fixed")
                else:
                    rep.add(f"lineage-{sec}"[:15], PASS, "step 03 query validates against this server")
        except TableauError as e:
            rep.add("metadata-api", FAIL, f"{e}"[:300],
                    "Server: `tsm maintenance metadata-services enable` (2019.3+), then allow time for backfill")

        if roles_only:
            return rep, extra
        try:
            v = next(iter(client.rest_paged("/views?includeUsageStatistics=true", "views", "view", page_size=1)), None)
            if v is None:
                rep.add("rest-usage", WARN, "no views on this site to sample")
            elif (v.get("usage") or {}).get("totalViewCount") is None:
                rep.add("rest-usage", FAIL if client.deployment == "server" else WARN,
                        "views returned without usage statistics",
                        "Usage statistics may be disabled on this server; ask the admin to check")
            else:
                rep.add("rest-usage", PASS, f"view counters available (sample totalViewCount="
                                            f"{v['usage']['totalViewCount']}, createdAt={'yes' if v.get('createdAt') else 'no'})")
        except TableauError as e:
            rep.add("rest-usage", FAIL if client.deployment == "server" else WARN, str(e)[:200])

        if client.deployment == "cloud":
            try:
                ai = client.admin_insights()
                rep.add("admin-insights", PASS if "TS Events" in ai else WARN, ", ".join(sorted(ai)))
            except TableauError as e:
                rep.add("admin-insights", FAIL, str(e))
        else:
            rep.add("admin-insights", SKIP, "Tableau Cloud only; Server usage comes from REST snapshots (step 02)")
    finally:
        client.sign_out()

    if client.deployment == "server":
        snapshot_status(rep, site)
    return rep, extra


def access_row(site, extra, any_signin_ok):
    role = extra.get("site_role")
    n_rest, n_meta = extra.get("rest_workbooks"), extra.get("metadata_workbooks")
    pct = round(100 * n_rest / n_meta) if n_rest is not None and n_meta else None
    signin = extra.get("sign_in", "not attempted")
    if signin == "not attempted":
        access, action = "NOT CHECKED", extra.get("skip_reason") or "Not checked"
    elif signin == "failed":
        access = "NO ACCESS"
        action = ("Sign-in refused: add the account to this site, or fix the contentUrl spelling"
                  if any_signin_ok else
                  "Sign-in refused on every site: check PAT name/secret and expiry")
    elif role and role.startswith(ADMIN_ROLES):
        access, action = "FULL", ""
    else:
        access, action = "PARTIAL", "Grant Site Administrator on this site"
    return {"site": site_folder(site), "sign_in": signin, "site_role": role or "", "access": access,
            "rest_workbooks": "" if n_rest is None else n_rest,
            "metadata_workbooks": "" if n_meta is None else n_meta,
            "visible_pct": "" if pct is None else pct, "action": action}


def write_access_summary(rows):
    server_dir = REPO_ROOT / "output" / "_server"
    server_dir.mkdir(parents=True, exist_ok=True)
    cols = list(rows[0])
    with open(server_dir / "access-summary.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, lineterminator="\n")
        w.writeheader()
        w.writerows(rows)
    heads = ["Site", "Sign-in", "Role", "Access", "Workbooks visible", "Workbooks on site", "Visible %", "Action needed"]
    lines = ["| " + " | ".join(heads) + " |", "|" + "---|" * len(heads)]
    for r in rows:
        lines.append("| " + " | ".join(str(r[c]) for c in cols) + " |")
    need = [r for r in rows if r["access"] != "FULL"]
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    md = [f"# Tableau access summary ({stamp})", "",
          f"{len(rows) - len(need)} of {len(rows)} sites have full (Site Administrator) access.", ""] + lines
    if need:
        md += ["", "Sites needing action: " + ", ".join(r["site"] for r in need)]
    (server_dir / "access-summary.md").write_text("\n".join(md) + "\n", encoding="utf-8")

    print("\n======== access summary ========")
    hdr = ["site", "sign_in", "site_role", "access", "visible_wb", "site_wb", "visible%"]
    widths = [max([len(h)] + [len(str(r[c])) for r in rows]) for h, c in zip(hdr, cols[:7])]
    print("  " + "  ".join(h.ljust(w) for h, w in zip(hdr, widths)))
    for r in rows:
        print("  " + "  ".join(str(r[c]).ljust(w) for c, w in zip(cols[:7], widths))
              + (f"  -> {r['action']}" if r["action"] else ""))
    print(f"\n  {len(rows) - len(need)} of {len(rows)} sites FULL. Written to {server_dir / 'access-summary.md'} (and .csv)")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--site", default=None, help="site contentUrl; 'default' for the Server Default site")
    ap.add_argument("--all-sites", action="store_true",
                    help="check every site in TABLEAU_SITES; without it, list the server's sites (Server Admin only)")
    ap.add_argument("--roles-only", action="store_true",
                    help="fast access audit: sign-in, role and visibility only (skips lineage, usage, snapshot checks)")
    args = ap.parse_args()
    ensure_env()
    listed = configured_sites() if args.all_sites else None
    first = normalize_site(args.site) if args.site is not None or not listed else listed[0]

    rep, extra = check_site(first, list_sites=args.all_sites and not listed, roles_only=args.roles_only)
    rep.save(extra)
    rep.print()
    failed = rep.failed()
    results = [(first, extra)]
    if listed:
        extra["sites"] = [{"name": None, "contentUrl": s} for s in listed]

    if args.all_sites and extra.get("sites"):
        server_dir = REPO_ROOT / "output" / "_server"
        server_dir.mkdir(parents=True, exist_ok=True)
        (server_dir / "sites.json").write_text(json.dumps(extra["sites"], indent=2))
        streak = 1 if extra.get("sign_in") == "failed" else 0
        for s in extra["sites"]:
            other = s["contentUrl"]
            if other == first:
                continue
            if streak >= MAX_CONSECUTIVE_SIGNIN_FAILURES:
                x = {"sign_in": "not attempted",
                     "skip_reason": f"Skipped after {streak} consecutive sign-in failures (protects the PAT "
                                    "from lockout); fix those sites, then re-run"}
                results.append((other, x))
                failed = True
                continue
            r, x = check_site(other, roles_only=args.roles_only)
            r.save(x)
            r.print()
            failed = failed or r.failed()
            results.append((other, x))
            streak = streak + 1 if x.get("sign_in") == "failed" else 0
        if streak >= MAX_CONSECUTIVE_SIGNIN_FAILURES:
            print(f"\nSTOPPED signing in after {streak} consecutive failures to protect the PAT "
                  "(four in a row lock it). A successful sign-in resets Tableau's counter.")

    if len(results) > 1:
        any_ok = any(x.get("sign_in") == "ok" for _, x in results)
        write_access_summary([access_row(site, x, any_ok) for site, x in results])

    print("\nOverall:", "FAIL (see fixes above)" if failed else "OK")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
