#!/usr/bin/env python3
"""Step 11: workbook-level migration priority, Sigma data-model clusters and suggested waves.

Usage:
    python scripts/workbook_priority.py --site <site> [--similarity 0.5] [--hub-pct 5]

Runs after step 10. Rolls each workbook's reports up into one row and combines:
  * usage        Traffic to Views opens and users (else snapshot opens); retire-candidate
                 status is carried as information only (see "Population" below)
  * complexity   predicted parity, band and gap counts from step 05 (if it has run)
  * sources      how ready its data is for Sigma on Snowflake (worst of its reports):
                   in-snowflake       every source already in Snowflake
                   warehouse-mapped   traced to warehouse tables, but not in Snowflake yet
                   needs-mapping      database known, tables not recorded or source unresolved
                   needs-landing      files, extracts with no recorded origin, or localhost
  * clusters     workbooks that would share one Sigma data model: first those sharing a
                 published datasource, then those whose warehouse tables overlap by at
                 least --similarity (Jaccard), ignoring hub tables used by more than
                 --hub-pct % of workbooks (date and lookup tables would chain everything).

Score and tag follow the Sigma assessment skill's shortlist:
    value = accesses x sqrt(users);  cost = 10 x unhandled + 3 x manual + hint features
    score = value / (1 + cost)
    tag:  needs-gap-review (any unhandled feature) | migrate-first (score >= 20, no manual/unhandled) |
          easy-win (score >= 10) | moderate | not-scanned (no complexity yet)

recommended_path: land-data-first | convert-with-gap-review | convert
suggested_wave (a starting point for the wave plan, not a decision), set per cluster from
its least-ready member, since a cluster moves together:
    1 in-snowflake, 2 warehouse-mapped (after its pipelines reach Snowflake),
    3 needs-mapping or needs-landing

Population: EVERY workbook is in the migration population. Retire candidates are not
filtered out of any count, ranking, cluster or wave until the customer confirms retirement
and a filter is explicitly added; retire_candidate / retire_candidate_reports are
information only. Usage still orders the ranking, so unused workbooks rank lower.

Pipeline priority: which warehouse tables to land in Snowflake first. Tables already in
Snowflake count as landed. A workbook is unlocked once every table it reads is landed and
it has no other blocker (files, extract-only, localhost, unmapped sources). Tables are
picked greedily: each pick is the table with the most pending usage behind it, where a
workbook's opens (at least 1) are shared across the tables it still waits on. The
cumulative columns then read "landing the first N tables unlocks X% of usage".
It is an anchor for the pipeline conversation, not a plan: it knows nothing yet about
the pipelines (Talend jobs) behind each table.

Gap features: which unhandled (and manual) Tableau features recur across the estate.
A workbook is tagged needs-gap-review for ANY unhandled feature, so the count alone hides
whether that is hundreds of distinct problems or a few repeated ones. Unhandled features are
ranked by how many workbooks have them; cumulative_gap_review_cleared counts the gap-review
workbooks whose unhandled features are ALL within the top N, i.e. that would no longer need
gap review once those N have a known Sigma pattern.

Writes to output/<site>/report-inventory/:
  workbooks.csv          one row per workbook
  clusters.csv           one row per data-model cluster
  pipeline-priority.csv  one row per warehouse table the workbooks use, ranked
  gap-features.csv       one row per unhandled or manual feature found, ranked
  migration-plan.json    all of it, plus a summary
and to output/_server/:
  pipeline-priority-all-sites.csv   the same ranking over every full-access site together
                                    (one table often feeds several sites)
  gap-features-all-sites.csv        the gap ranking over every full-access site together
and adds "workbooks", "pipelines" and "gaps" sections to summary.csv, then refreshes the rollup.
"""

import argparse
import csv
import json
import math
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import build_report_inventory as inv  # noqa: E402
from lib.hosts import normalize_host  # noqa: E402
from lib.tableau_client import add_site_args, code_version, log, resolve_site_args  # noqa: E402

READINESS = ["in-snowflake", "warehouse-mapped", "needs-mapping", "needs-landing"]
WAVE_FOR = {"in-snowflake": "1", "warehouse-mapped": "2", "needs-mapping": "3", "needs-landing": "3"}


def num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def split(v):
    return [x for x in (v or "").split("; ") if x]


def report_readiness(r):
    status, types, hosts = r["lineage_status"], set(split(r["source_types"])), split(r["source_hosts"])
    if status in ("file-source", "extract-only") or "localhost" in hosts:
        return "needs-landing"
    if status in ("no-tables-indexed", "published-unresolved", "unparsed-custom-sql", "no-source-info"):
        return "needs-mapping"
    if status == "tables":
        return "in-snowflake" if types and types <= {"snowflake"} else "warehouse-mapped"
    return None  # no-datasource: no data dependency


class UF:
    def __init__(self):
        self.p = {}

    def find(self, x):
        self.p.setdefault(x, x)
        while self.p[x] != x:
            self.p[x] = self.p[self.p[x]]
            x = self.p[x]
        return x

    def union(self, a, b):
        self.p[self.find(a)] = self.find(b)


def load_table_meta(dest):
    meta = {}
    p = dest / "tables.csv"
    if p.exists():
        with open(p, encoding="utf-8") as fh:
            for t in csv.DictReader(fh):
                meta[t["table_key"]] = {"connection_type": t.get("connection_type") or "",
                                        "host": normalize_host(t.get("host")) or (t.get("host") or "")}
    return meta


def pipeline_priority(wbs, meta):
    """Greedy table ranking. wbs: dicts with key, tables (set), weight, unlockable, accesses.

    Returns (rows, milestones)."""
    landed = {t for t, m in meta.items() if m["connection_type"] == "snowflake"}
    pending = {w["key"]: set(w["tables"]) - landed for w in wbs}
    by_table = defaultdict(set)
    for w in wbs:
        for t in pending[w["key"]]:
            by_table[t].add(w["key"])
    W = {w["key"]: w for w in wbs}
    score = {t: sum(W[k]["weight"] / len(pending[k]) for k in ks) for t, ks in by_table.items()}
    unlockable = [w for w in wbs if w["unlockable"]]
    total_wb, total_acc = len(unlockable), sum(w["accesses"] for w in unlockable) or 0
    unlocked = {w["key"] for w in unlockable if not pending[w["key"]]}
    acc_done = sum(W[k]["accesses"] for k in unlocked)
    rows, n = [], 0
    while score:
        t = max(score, key=lambda x: (round(score[x], 6), len(by_table[x]), x))
        del score[t]
        n += 1
        newly = []
        for k in by_table[t]:
            before = len(pending[k])
            pending[k].discard(t)
            for u in pending[k]:  # this workbook's share moves to its remaining tables
                if u in score:
                    score[u] += W[k]["weight"] / len(pending[k]) - W[k]["weight"] / before
            if not pending[k] and W[k]["unlockable"]:
                newly.append(k)
        unlocked |= set(newly)
        acc_done += sum(W[k]["accesses"] for k in newly)
        users = by_table[t]
        rows.append({
            "pipeline_rank": n, "table_key": t, "connection_type": meta.get(t, {}).get("connection_type", ""),
            "host": meta.get(t, {}).get("host", ""), "workbooks": len(users),
            "accesses": sum(W[k]["accesses"] for k in users),
            "sites": len({W[k]["site"] for k in users}),
            "workbooks_unlocked_now": len(newly),
            "cumulative_workbooks_unlocked": len(unlocked),
            "cumulative_pct_unlockable_workbooks": round(100 * len(unlocked) / total_wb, 1) if total_wb else "",
            "cumulative_accesses_unlocked": acc_done,
            "cumulative_pct_unlockable_accesses": round(100 * acc_done / total_acc, 1) if total_acc else "",
            "clusters": "; ".join(sorted({W[k]["cluster"] for k in users if W[k]["cluster"]}))[:300],
            "example_workbooks": "; ".join(sorted(W[k]["name"] or "" for k in users)[:5]),
        })

    def first(pct_key, level):
        return next((r["pipeline_rank"] for r in rows if r[pct_key] != "" and r[pct_key] >= level), "")
    milestones = {
        "tables_to_land": len(rows),
        "tables_already_in_snowflake": len({t for w in wbs for t in w["tables"]} & landed),
        "unlockable_workbooks": total_wb,
        "blocked_beyond_tables": sum(1 for w in wbs if not w["unlockable"]),
        "unlocked_before_any_landing": len({w["key"] for w in unlockable if not (set(w["tables"]) - landed)}),
        "tables_for_50pct_usage": first("cumulative_pct_unlockable_accesses", 50),
        "tables_for_80pct_usage": first("cumulative_pct_unlockable_accesses", 80),
        "tables_for_50pct_workbooks": first("cumulative_pct_unlockable_workbooks", 50),
        "tables_for_80pct_workbooks": first("cumulative_pct_unlockable_workbooks", 80),
    }
    return rows, milestones


PIPE_COLS = ["pipeline_rank", "table_key", "connection_type", "host", "workbooks", "accesses", "sites",
             "workbooks_unlocked_now", "cumulative_workbooks_unlocked", "cumulative_pct_unlockable_workbooks",
             "cumulative_accesses_unlocked", "cumulative_pct_unlockable_accesses", "clusters", "example_workbooks"]
PIPE_HELP = {
    "tables_to_land": "warehouse tables the workbooks use that are not yet in Snowflake",
    "tables_already_in_snowflake": "tables the workbooks use that are already in Snowflake",
    "unlockable_workbooks": "workbooks whose only data dependency is warehouse tables",
    "blocked_beyond_tables": "workbooks that also need files, extracts or local data landed, or sources mapped",
    "unlocked_before_any_landing": "unlockable workbooks already reading only Snowflake",
    "tables_for_50pct_usage": "tables to land, in pipeline-priority order, to unlock half the usage of unlockable workbooks",
    "tables_for_80pct_usage": "the same, for 80% of usage",
    "tables_for_50pct_workbooks": "tables to land to unlock half the unlockable workbooks",
    "tables_for_80pct_workbooks": "the same, for 80% of them",
}


def write_pipeline_csv(path, rows):
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=PIPE_COLS, extrasaction="ignore", lineterminator="\n")
        w.writeheader()
        w.writerows(rows)


AUDIT_UNAVAILABLE = "Converter-backed formula audit unavailable"
GAP_COLS = ["gap_rank", "feature", "status", "workbooks", "pct_of_scanned", "occurrences",
            "accesses", "sole_unhandled_for", "cumulative_gap_review_cleared", "cumulative_pct_gap_review_cleared",
            "example_workbooks"]
GAP_HELP = {
    "scanned": "workbooks with a complexity scan",
    "gap_review_workbooks": "scanned workbooks with at least one unhandled feature",
    "unhandled_feature_types": "distinct unhandled features across them",
    "manual_feature_types": "distinct manual features across scanned workbooks",
    "cleared_by_top1": "gap-review workbooks whose only unhandled feature is the most common one",
    "cleared_by_top5": "gap-review workbooks whose unhandled features are all among the 5 most common",
    "cleared_by_top10": "the same, for the 10 most common",
    "features_for_80pct_gap_review": "most common unhandled features that together cover 80% of gap-review workbooks",
    "audit_unavailable_workbooks": "workbooks scanned without Node.js (formula features overstated)",
}


def gap_breakdown(entries):
    """entries: dicts with key, name, accesses, features [{name, status, count}] (scanned workbooks).

    Returns (rows, milestones)."""
    stats = {}
    unhandled_of = {}
    for e in entries:
        un = set()
        for f in e["features"]:
            st = f.get("status")
            if st not in ("unhandled", "manual"):
                continue
            k = (f.get("name") or "?", st)
            s = stats.setdefault(k, {"wbs": set(), "occ": 0, "acc": 0, "names": []})
            if e["key"] not in s["wbs"]:
                s["wbs"].add(e["key"])
                s["acc"] += e["accesses"]
                s["names"].append(e["name"] or "")
            s["occ"] += int(f.get("count") or 1)
            if st == "unhandled":
                un.add(k[0])
        if un:
            unhandled_of[e["key"]] = un
    n = len(entries)
    order = sorted(stats, key=lambda k: (k[1] != "unhandled", -len(stats[k]["wbs"]), -stats[k]["occ"], k[0]))
    gap_total = len(unhandled_of)
    covered, rows, rank = set(), [], 0
    for k in order:
        s = stats[k]
        row = {"feature": k[0], "status": k[1], "workbooks": len(s["wbs"]),
               "pct_of_scanned": round(100 * len(s["wbs"]) / n, 1) if n else "",
               "occurrences": s["occ"], "accesses": s["acc"],
               "example_workbooks": "; ".join(sorted(s["names"])[:5])}
        if k[1] == "unhandled":
            rank += 1
            covered.add(k[0])
            row.update(gap_rank=rank,
                       sole_unhandled_for=sum(1 for w in s["wbs"] if unhandled_of.get(w) == {k[0]}),
                       cumulative_gap_review_cleared=sum(1 for u in unhandled_of.values() if u <= covered))
            row["cumulative_pct_gap_review_cleared"] = (round(100 * row["cumulative_gap_review_cleared"] / gap_total, 1)
                                                 if gap_total else "")
        rows.append(row)
    un_rows = [r for r in rows if r["status"] == "unhandled"]

    def cleared(top):
        return un_rows[min(top, len(un_rows)) - 1]["cumulative_gap_review_cleared"] if un_rows else 0
    ms = {"scanned": n, "gap_review_workbooks": gap_total, "unhandled_feature_types": len(un_rows),
          "manual_feature_types": len(rows) - len(un_rows), "cleared_by_top1": cleared(1),
          "cleared_by_top5": cleared(5), "cleared_by_top10": cleared(10),
          "features_for_80pct_gap_review": next((r["gap_rank"] for r in un_rows
                                                 if r["cumulative_pct_gap_review_cleared"] != ""
                                                 and r["cumulative_pct_gap_review_cleared"] >= 80), ""),
          "audit_unavailable_workbooks": len(stats.get((AUDIT_UNAVAILABLE, "unhandled"), {}).get("wbs", ()))}
    return rows, ms


def gap_entries(rows, cx, site=""):
    return [{"key": f"{site}|{r['workbook_luid']}" if site else r["workbook_luid"], "name": r["workbook"],
             "accesses": int(r["accesses"] or 0), "features": (cx.get(r["workbook_luid"]) or {}).get("features") or []}
            for r in rows if r["workbook_luid"] in cx]


def write_rows_csv(path, cols, rows):
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore", lineterminator="\n")
        w.writeheader()
        w.writerows(rows)


def rebuild_gaps_all_sites(root, sites):
    entries = []
    for site in sites:
        cxp, wbp = root / site / "complexity.json", root / site / "report-inventory" / "workbooks.csv"
        if not (cxp.exists() and wbp.exists()):
            continue
        cx = json.loads(cxp.read_text(encoding="utf-8"))
        with open(wbp, encoding="utf-8") as fh:
            entries += gap_entries(list(csv.DictReader(fh)), cx, site)
    if not entries:
        return {}
    rows, ms = gap_breakdown(entries)
    write_rows_csv(root / "_server" / "gap-features-all-sites.csv", GAP_COLS, rows)
    return ms


def rebuild_pipeline_all_sites(root):
    """Rank tables over every full-access site's workbooks together."""
    access = inv.site_access(root)
    wbs, meta, sites = [], {}, []
    for plan_path in sorted(root.glob("*/report-inventory/migration-plan.json")):
        site = plan_path.parent.parent.name
        if site.startswith("_") or access.get(site, {}).get("access", "FULL") != "FULL":
            continue
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
        pw = plan.get("pipeline_workbooks")
        if pw is None:
            continue
        sites.append(site)
        wbs += [{**w, "tables": set(w["tables"]), "key": f"{site}|{w['key']}",
                 "cluster": f"{site}:{w['cluster']}" if w.get("cluster") else ""} for w in pw]
        for t, m in load_table_meta(plan_path.parent).items():
            meta.setdefault(t, m)
    if not sites:
        return {}
    rows, ms = pipeline_priority(wbs, meta)
    server = root / "_server"
    server.mkdir(parents=True, exist_ok=True)
    write_pipeline_csv(server / "pipeline-priority-all-sites.csv", rows)
    gms = rebuild_gaps_all_sites(root, sites)
    totals = {**{f"pipelines|{k}": v for k, v in ms.items()}, **{f"gaps|{k}": v for k, v in gms.items()}}
    (server / "step11-totals.json").write_text(json.dumps({"sites": sites, "totals": totals}, indent=2))
    old = server / "pipeline-totals.json"  # earlier name of the same file
    if old.exists():
        old.unlink()
    return {**ms, "_gaps": gms}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    add_site_args(ap)
    ap.add_argument("--similarity", type=float, default=0.5, help="Jaccard threshold on warehouse tables")
    ap.add_argument("--hub-pct", type=float, default=5.0,
                    help="ignore tables used by more than this %% of workbooks when clustering")
    args = ap.parse_args()
    site, out = resolve_site_args(args)
    dest = out / "report-inventory"
    rpath = dest / "reports.csv"
    if not rpath.exists():
        sys.exit(f"missing {rpath}; run step 10 first")
    with open(rpath, encoding="utf-8") as fh:
        reports = list(csv.DictReader(fh))
    pub_by_report = defaultdict(set)
    epath = dest / "edges_report_datasource.csv"
    if epath.exists():
        with open(epath, encoding="utf-8") as fh:
            for e in csv.DictReader(fh):
                for luid in (e.get("published_luids") or "").split(";"):
                    if luid.strip():
                        pub_by_report[e["report_id"]].add(luid.strip())
    cx_path = out / "complexity.json"
    cx = json.loads(cx_path.read_text(encoding="utf-8")) if cx_path.exists() else {}
    if not cx:
        log("No complexity.json (step 05): tags and paths use usage and sources only")

    # ---- roll reports up to workbooks
    wbs = {}
    for r in reports:
        w = wbs.setdefault(r["workbook_luid"], {
            "workbook_luid": r["workbook_luid"], "workbook": r["workbook"], "project": r["project"],
            "owner": r["owner"], "reports": 0, "data_reports": 0, "reports_in_use": 0, "retire_candidates": 0,
            "review": 0, "traffic_accesses": 0.0, "traffic_users_max": 0.0, "view_accesses": 0.0,
            "has_traffic": False, "readiness": set(), "statuses": Counter(), "tables": set(),
            "published": set(), "source_types": set(), "hosts": set(), "last_accessed": "",
        })
        w["reports"] += 1
        rc = r.get("retire_candidate")
        if r["lineage_status"] != "no-datasource":
            w["data_reports"] += 1
            w["statuses"][r["lineage_status"]] += 1
        if rc in ("no", "review"):
            w["reports_in_use"] += 1
        w["retire_candidates"] += rc == "yes"
        w["review"] += rc == "review"
        if r.get("traffic_accesses") not in (None, ""):
            w["has_traffic"] = True
            w["traffic_accesses"] += num(r["traffic_accesses"]) or 0
            w["traffic_users_max"] = max(w["traffic_users_max"], num(r.get("traffic_users")) or 0)
        w["view_accesses"] += num(r.get("view_accesses")) or 0
        w["last_accessed"] = max(w["last_accessed"], r.get("traffic_last_accessed") or "")
        rd = report_readiness(r)
        if rd:
            w["readiness"].add(rd)
        w["tables"] |= set(split(r["tables"]))
        w["published"] |= pub_by_report.get(r["report_id"], set())
        w["source_types"] |= set(split(r["source_types"]))
        w["hosts"] |= set(split(r["source_hosts"]))

    # ---- score, tag, path
    rows = []
    for luid, w in wbs.items():
        c = cx.get(luid) or {}
        readiness = max(w["readiness"], key=READINESS.index) if w["readiness"] else "no-data"
        acc = w["traffic_accesses"] if w["has_traffic"] else w["view_accesses"]
        users = w["traffic_users_max"] if w["has_traffic"] else 1
        n_un, n_man, n_hint = (int(c.get(k) or 0) for k in ("n_unhandled", "n_manual", "n_hint"))
        score = round(acc * math.sqrt(max(users, 1)) / (1 + 10 * n_un + 3 * n_man + n_hint), 1)
        # Information only: retire candidates stay in the population until retirement is confirmed.
        retire_flag = ("yes" if w["data_reports"] and w["retire_candidates"] == w["data_reports"]
                       else "some reports" if w["retire_candidates"] else "no")
        if not c:
            tag = "not-scanned"
        elif n_un:
            tag = "needs-gap-review"
        elif score >= 20 and not n_man:
            tag = "migrate-first"
        elif score >= 10:
            tag = "easy-win"
        else:
            tag = "moderate"
        if readiness == "needs-landing":
            path = "land-data-first"
        elif n_un:
            path = "convert-with-gap-review"
        else:
            path = "convert"
        blockers = []
        if n_un:
            blockers.append(f"{n_un} unhandled feature(s)")
        if n_man:
            blockers.append(f"{n_man} manual feature(s)")
        if readiness in ("needs-mapping", "needs-landing"):
            blockers.append(readiness.replace("-", " "))
        if cx and not c:
            blockers.append("not scanned (see twb-fetch-results.json / complexity-failures.json)")
        if c.get("formula_audit") == "unavailable":
            blockers.append("formula audit unavailable (parity understated)")
        rows.append({
            "workbook_luid": luid, "workbook": w["workbook"], "project": w["project"], "owner": w["owner"],
            "reports": w["reports"], "data_reports": w["data_reports"], "reports_in_use": w["reports_in_use"],
            "retire_candidate": retire_flag, "retire_candidate_reports": w["retire_candidates"], "review": w["review"],
            "accesses": int(acc), "users_at_least": int(w["traffic_users_max"]) if w["has_traffic"] else "",
            "last_accessed": w["last_accessed"],
            "parity_band": c.get("parity_band", ""), "predicted_parity_pct": c.get("predicted_parity_pct", ""),
            "n_unhandled": n_un if c else "", "n_manual": n_man if c else "", "n_hint": n_hint if c else "",
            "top_gaps": c.get("top_gaps", ""), "source_readiness": readiness,
            "lineage_statuses": "; ".join(f"{k} {v}" for k, v in w["statuses"].most_common()),
            "source_types": "; ".join(sorted(w["source_types"])), "source_hosts": "; ".join(sorted(w["hosts"])),
            "published_datasources": len(w["published"]), "warehouse_tables": len(w["tables"]),
            "score": score, "tag": tag, "recommended_path": path, "blockers": "; ".join(blockers),
            "_tables": w["tables"], "_published": w["published"],
        })

    # ---- clusters: shared published datasource first, then table overlap without hub tables
    data_wbs = [r for r in rows if r["data_reports"]]  # every workbook that uses data
    freq = Counter(t for r in data_wbs for t in r["_tables"])
    hub_n = max(10, math.ceil(len(data_wbs) * args.hub_pct / 100))
    hubs = {t for t, n in freq.items() if n > hub_n}
    uf = UF()
    by_pub = defaultdict(list)
    for r in data_wbs:
        uf.find(r["workbook_luid"])
        for p in r["_published"]:
            by_pub[p].append(r["workbook_luid"])
    for members in by_pub.values():
        for m in members[1:]:
            uf.union(members[0], m)
    pub_clustered = {r["workbook_luid"] for r in data_wbs if r["_published"]}
    rest = sorted([r for r in data_wbs if r["workbook_luid"] not in pub_clustered and r["_tables"] - hubs],
                  key=lambda r: -len(r["_tables"] - hubs))
    unassigned = list(rest)
    while unassigned:
        seed = unassigned.pop(0)
        st = seed["_tables"] - hubs
        keep = []
        for r in unassigned:
            t = r["_tables"] - hubs
            if len(st & t) / len(st | t) >= args.similarity:
                uf.union(seed["workbook_luid"], r["workbook_luid"])
            else:
                keep.append(r)
        unassigned = keep
    groups = defaultdict(list)
    for r in data_wbs:
        groups[uf.find(r["workbook_luid"])].append(r)
    clusters = []
    multi = sorted([g for g in groups.values() if len(g) > 1], key=lambda g: (-len(g), g[0]["workbook"] or ""))
    for i, g in enumerate(multi, 1):
        cid = f"dm-{i:03d}"
        shared_pub = set.intersection(*(m["_published"] for m in g)) if all(m["_published"] for m in g) else set()
        shared_tbl = set.intersection(*(m["_tables"] for m in g)) - hubs
        worst = max((m["source_readiness"] for m in g if m["source_readiness"] in READINESS),
                    key=READINESS.index, default="no-data")
        for m in g:
            m["cluster_id"] = cid
        clusters.append({"cluster_id": cid, "workbooks": len(g), "reports_in_use": sum(m["reports_in_use"] for m in g),
                         "accesses": sum(m["accesses"] for m in g),
                         "linked_by": "published datasource" if any(m["_published"] for m in g) else "shared tables",
                         "shared_published_datasources": len(shared_pub),
                         "shared_tables": "; ".join(sorted(shared_tbl)[:15]),
                         "source_readiness": worst, "suggested_wave": WAVE_FOR.get(worst, "3"),
                         "workbook_names": "; ".join(sorted(m["workbook"] or "" for m in g))[:1000]})
    cluster_wave = {c["cluster_id"]: c["suggested_wave"] for c in clusters}
    for r in rows:
        r.setdefault("cluster_id", "")
        if r["cluster_id"]:
            r["suggested_wave"] = cluster_wave[r["cluster_id"]]
        else:
            r["suggested_wave"] = WAVE_FOR.get(r["source_readiness"], "1")
    rows.sort(key=lambda r: ({"1": 0, "2": 1, "3": 2}[r["suggested_wave"]], -r["score"]))
    for i, r in enumerate(rows, 1):
        r["priority_rank"] = i

    # ---- write
    cols = ["priority_rank", "suggested_wave", "cluster_id", "workbook", "project", "owner", "tag",
            "recommended_path", "score", "accesses", "users_at_least", "last_accessed", "reports", "data_reports",
            "reports_in_use", "retire_candidate", "retire_candidate_reports", "review", "parity_band", "predicted_parity_pct", "n_unhandled",
            "n_manual", "n_hint", "top_gaps", "source_readiness", "lineage_statuses", "source_types",
            "source_hosts", "published_datasources", "warehouse_tables", "blockers", "workbook_luid"]
    with open(dest / "workbooks.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore", lineterminator="\n")
        w.writeheader()
        w.writerows(rows)
    ccols = ["cluster_id", "suggested_wave", "workbooks", "reports_in_use", "accesses", "linked_by",
             "shared_published_datasources", "source_readiness", "shared_tables", "workbook_names"]
    with open(dest / "clusters.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=ccols, lineterminator="\n")
        w.writeheader()
        w.writerows(clusters)
    tags, paths, waves = (Counter(r[k] for r in rows) for k in ("tag", "recommended_path", "suggested_wave"))
    bands = Counter(r["parity_band"] for r in rows if r["parity_band"])
    readiness = Counter(r["source_readiness"] for r in rows)
    summary = {"code_version": code_version(), "workbooks": len(rows),
               "retire_candidate_workbooks": sum(1 for r in rows if r["retire_candidate"] == "yes"), "tags": tags, "paths": paths,
               "waves": waves, "parity_bands": bands, "readiness": readiness,
               "clusters": len(clusters), "clustered_workbooks": sum(c["workbooks"] for c in clusters),
               "hub_tables_ignored": sorted(hubs)[:50], "scanned": bool(cx)}
    # ---- pipeline priority
    meta = load_table_meta(dest)
    pipe_wbs = [{"key": r["workbook_luid"], "name": r["workbook"], "site": out.name, "cluster": r["cluster_id"],
                 "tables": sorted(r["_tables"]), "accesses": r["accesses"], "weight": max(r["accesses"], 1),
                 "unlockable": r["source_readiness"] in ("in-snowflake", "warehouse-mapped")}
                for r in rows if r["_tables"]]
    pipe_rows, ms = pipeline_priority([{**w, "tables": set(w["tables"])} for w in pipe_wbs], meta)
    write_pipeline_csv(dest / "pipeline-priority.csv", pipe_rows)
    plan = {"summary": {**summary, "pipelines": ms}, "clusters": clusters,
            "workbooks": [{k: v for k, v in r.items() if not k.startswith("_")} for r in rows],
            "pipeline_workbooks": pipe_wbs}
    (dest / "migration-plan.json").write_text(json.dumps(plan, indent=2, default=list))

    # ---- summary.csv section and the cross-site rollup
    # Every workbook is in the population; retire-candidate counts are information only.
    srows = [("workbooks", len(rows), "workbooks with at least one report (all in the migration population)"),
             ("retire_candidate_workbooks", sum(1 for r in rows if r["retire_candidate"] == "yes"),
              "information only: every data report is a retire candidate (not excluded until confirmed)"),
             ("workbooks_with_retire_candidate_reports", sum(1 for r in rows if r["retire_candidate"] == "some reports"),
              "information only: some, not all, data reports are retire candidates")]
    srows += [(f"path_{k}", paths.get(k, 0), d) for k, d in (
        ("convert", "convert directly"), ("convert-with-gap-review", "convert after reviewing unhandled features"),
        ("land-data-first", "data must land in Snowflake before conversion"))]
    srows += [(f"wave_{k}", waves.get(k, 0), d) for k, d in (
        ("1", "suggested wave 1: sources already in Snowflake"),
        ("2", "suggested wave 2: after pipelines reach Snowflake"),
        ("3", "suggested wave 3: sources to map or land first"))]
    if cx:
        scanned = [r for r in rows if r["parity_band"]]
        avg = round(sum(float(r["predicted_parity_pct"]) for r in scanned) / len(scanned), 1) if scanned else ""
        srows += [("parity_scanned", len(scanned), "workbooks with a complexity scan (step 05)"),
                  ("avg_predicted_parity_pct", avg,
                   "average predicted parity of scanned workbooks; across sites, weighted by workbooks"),
                  ("unhandled_features", sum(int(r["n_unhandled"] or 0) for r in scanned),
                   "distinct unhandled features, summed over scanned workbooks"),
                  ("manual_features", sum(int(r["n_manual"] or 0) for r in scanned),
                   "distinct manual features, summed over scanned workbooks")]
        srows += [(f"parity_band_{b}", bands.get(b, 0), d) for b, d in (
            ("A", "workbooks predicted >= 90% parity (drop-in)"), ("B", "75-90% (light touch-up)"),
            ("C", "50-75% (meaningful manual work)"), ("D", "< 50% (hard, re-author risk)"))]
    srows += [("data_model_clusters", len(clusters), "groups of workbooks that would share a Sigma data model"),
              ("clustered_workbooks", summary["clustered_workbooks"], "workbooks in a cluster")]
    inv.replace_summary_section(dest, out.name, "workbooks", srows)
    inv.replace_summary_section(dest, out.name, "pipelines", [(k, v, PIPE_HELP[k]) for k, v in ms.items()])
    if cx:
        gap_rows, gms = gap_breakdown(gap_entries(rows, cx))
        write_rows_csv(dest / "gap-features.csv", GAP_COLS, gap_rows)
        inv.replace_summary_section(dest, out.name, "gaps", [(k, v, GAP_HELP[k]) for k, v in gms.items()])
        top = ", ".join(f"{r['feature']} ({r['workbooks']})" for r in gap_rows if r["status"] == "unhandled")
        log(f"gaps: {gms['gap_review_workbooks']} gap-review workbooks, {gms['unhandled_feature_types']} unhandled "
            f"feature types; top 5 clear {gms['cleared_by_top5']}; most common: {top[:300]}")
    all_ms = rebuild_pipeline_all_sites(out.parent)
    inv.rebuild_cross_site(out)
    log(f"pipelines: {ms['tables_to_land']} tables to land; {ms['tables_for_50pct_usage'] or '-'} unlock half the "
        f"usage of {ms['unlockable_workbooks']} unlockable workbooks, {ms['tables_for_80pct_usage'] or '-'} unlock 80%; "
        f"{ms['blocked_beyond_tables']} workbooks also need non-table sources")
    if all_ms.get("_gaps"):
        g = all_ms["_gaps"]
        log(f"all full-access sites: {g['gap_review_workbooks']} gap-review workbooks, {g['unhandled_feature_types']} "
            f"unhandled feature types; top 5 clear {g['cleared_by_top5']}, top 10 clear {g['cleared_by_top10']} "
            f"(output/_server/gap-features-all-sites.csv)")
    if all_ms:
        log(f"all full-access sites: {all_ms['tables_to_land']} tables to land; "
            f"{all_ms['tables_for_50pct_usage'] or '-'} for half the usage, {all_ms['tables_for_80pct_usage'] or '-'} "
            f"for 80% (output/_server/pipeline-priority-all-sites.csv)")
    log(f"workbooks: {len(rows)} (all in the population; {summary['retire_candidate_workbooks']} are retire "
        f"candidates, information only); "
        f"paths {dict(paths)}; waves {dict(waves)}; {len(clusters)} clusters "
        f"({summary['clustered_workbooks']} workbooks); {len(hubs)} hub tables ignored")
    log(f"Wrote workbooks.csv, clusters.csv, migration-plan.json to {dest}/")


if __name__ == "__main__":
    main()
