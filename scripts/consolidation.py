#!/usr/bin/env python3
"""Step 12: workbooks that could become ONE Sigma workbook (consolidation candidates).

Usage:
    python scripts/consolidation.py --site <site> [--emit-floor 0.45]
    python scripts/consolidation.py --site <site> --decide consolidation-03=consolidate \
                                                  --decide consolidation-07=as-is

Ported from the Sigma assessment skill's consolidation-candidates.rb (same profile, scoring
and thresholds). Typical finds: copies of one dashboard that differ only by a filter value,
a year, a region or a copy/test/v2 suffix. In Sigma those become one workbook and a control.

Pure analysis: reads what steps 04, 05 and 11 wrote and never calls Tableau.

  * Profiles every downloaded .twb: sheets, dashboards, zones, fields each sheet uses,
    filters and their selected values, warehouse tables, published datasources,
    calculated-field formulas, reference lines, row-level-security functions.
  * Compares only workbooks that share a warehouse table or a published datasource.
    Pair score = 0.40 used-field overlap + 0.25 sheet structure + 0.15 name similarity
               + 0.10 filter fields + 0.10 gap-feature profile (step 05, when present)
  * Groups by complete linkage (every pair in a group scores >= 0.55), so A~B and B~C do
    not drag an unrelated C in. Near misses are listed as keep-separate, with evidence.
  * Recommendation is deliberately conservative (a false "consolidate" costs trust):
      consolidate    used-field overlap >= 70% and sheet-set similarity >= 60% for every pair
      review         same data and meaningful overlap; or RLS differs; or all are empty stubs
      keep-separate  same data, different content
    Each group carries similarity drivers, differences and proposed controls.

Decisions (--decide GROUP=consolidate|as-is) are kept in consolidation-decisions.json by
member workbook, so they survive re-runs and renumbered groups. Each run applies them to
migration-plan.json and workbooks.csv: merged members get recommended_path
consolidate-into-primary (original path kept in pre_consolidation_path).

Writes to output/<site>/report-inventory/:
  consolidation-candidates.json    groups with evidence
  consolidation-candidates.csv     one row per group member
and adds consolidation columns to workbooks.csv, a "consolidation" section to summary.csv,
then refreshes the cross-site rollup.
"""

import argparse
import csv
import json
import math
import re
import sys
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import build_report_inventory as inv  # noqa: E402
from lib.tableau_client import add_site_args, code_version, log, resolve_site_args  # noqa: E402

CONSOLIDATE_MIN_FIELD = 0.70
CONSOLIDATE_MIN_SHEETS = 0.60
REVIEW_MIN_SCORE = 0.55
GROUP_LINK = 0.55
GENERIC_TABLES = {"EXTRACT.EXTRACT"}
WEIGHTS = {"field": 0.40, "structure": 0.25, "name": 0.15, "filter": 0.10, "feature": 0.10}

VARIANT_TOKENS = set("""
copy copies v1 v2 v3 v4 v5 final draft test testing tests old new backup
wip tmp temp dev qa uat prod staging republish republished revised updated
edit edited version archive archived deprecated
dashboard dashboards report reports workbook workbooks sheet sheets viz
q1 q2 q3 q4 jan feb mar apr may jun jul aug sep oct nov dec
january february march april june july august september october november december
""".split())
YEAR = re.compile(r"^(19|20)\d{2}$")

FIELD_PREFIX = re.compile(r"^(none|attr|usr|sum|avg|min|max|cnt|cntd|count|median|stdev|var|yr|qr|mn|dy|hr|"
                          r"tyr|tqr|tmn|tdy|week|wk):", re.I)
RE_SHEET = re.compile(r"<worksheet\b[^>]*?name='([^']*)'")
RE_DASH = re.compile(r"<dashboard\b[^>]*?name='([^']*)'")
RE_ZONE = re.compile(r"<zone\b")
RE_COLUMN = re.compile(r"<column\b[^>]*>")
RE_CAPTION = re.compile(r"caption='([^']*)'")
RE_CNAME = re.compile(r"name='\[([^\]]*)\]'")
RE_DEPS = re.compile(r"<datasource-dependencies\b.*?</datasource-dependencies>", re.S)
RE_DEP_COL = re.compile(r"<column\b[^>]*?name='\[([^\]]*)\]'")
RE_FILTER = re.compile(r"<filter\b[^>]*column='[^']*'[^>]*?(?:/>|>.*?</filter>)", re.S)
RE_FILTER_COL = re.compile(r"column='([^']*)'")
RE_MEMBER = re.compile(r"member='(?:&quot;)?(.*?)(?:&quot;)?'")
RE_REPO = re.compile(r"<repository-location\b[^>]*\bid='([^']*)'")
RE_CALC = re.compile(r"<calculation\b[^>]*formula='([^']*)'")
RE_REFLINE = re.compile(r"<reference-line\b")
RE_RLS = re.compile(r"USERNAME\(\)|ISMEMBEROF|USERDOMAIN\(\)|FULLNAME\(\)")
RE_RELATION = re.compile(r"<relation\b[^>]*type='table'[^>]*>")
RE_TABLE_ATTR = re.compile(r"table='([^']*)'")


def variant_token(t):
    return t in VARIANT_TOKENS or bool(YEAR.match(t)) or t.isdigit()


def name_tokens(name):
    return re.findall(r"[a-z0-9]+", (name or "").lower())


def name_stem(name):
    return [t for t in name_tokens(name) if not variant_token(t)]


def normalize_field(raw):
    f = FIELD_PREFIX.sub("", raw or "", count=1)
    f = re.sub(r":(nk|qk|ok)$", "", f)
    return f.lower().strip()


def warehouse_tables(twb):
    out = []
    for tag in RE_RELATION.findall(twb):
        m = RE_TABLE_ATTR.search(tag)
        raw = re.sub(r"[\[\]]", "", m.group(1) if m else "")
        m1 = re.search(r"\(([^)]+)\)$", raw)
        m2 = re.search(r"[0-9a-f-]{30,}\.(.+)$", raw, re.I)
        t = (m1.group(1) if m1 else m2.group(1) if m2 else raw).upper().strip()
        if t and t not in out:
            out.append(t)
    return out


def build_profile(luid, name, path):
    twb = path.read_text(encoding="utf-8", errors="replace")
    sheets = list(dict.fromkeys(RE_SHEET.findall(twb)))
    captions = {}
    for tag in RE_COLUMN.findall(twb):
        cap, cname = RE_CAPTION.search(tag), RE_CNAME.search(tag)
        if cap and cname:
            captions[cname.group(1)] = cap.group(1)

    def resolve(raw):
        key = normalize_field(raw)
        return (captions.get(raw) or captions.get(key) or key).lower().strip()

    used = set()
    for block in RE_DEPS.findall(twb):
        used |= {resolve(c) for c in RE_DEP_COL.findall(block)}
    filters = defaultdict(set)
    for el in RE_FILTER.findall(twb):
        m = RE_FILTER_COL.search(el)
        col = m.group(1) if m else ""
        parts = re.findall(r"\[([^\]]*)\]", col)  # [datasource].[field]; datasource names may contain dots
        raw = parts[-1] if parts else re.sub(r"^[^.]*\.", "", col, count=1)
        field = resolve(raw)
        if not field or field.startswith("action ("):
            continue
        filters[field] |= set(RE_MEMBER.findall(el))
    return {
        "luid": luid, "name": name or "", "sheets": sheets,
        "dashboards": list(dict.fromkeys(RE_DASH.findall(twb))), "zones": len(RE_ZONE.findall(twb)),
        "used_fields": used, "schema_fields": {c.lower().strip() for c in captions.values()},
        "filters": {k: sorted(v) for k, v in filters.items()},
        "warehouse_tables": warehouse_tables(twb),
        "published_ids": list(dict.fromkeys(RE_REPO.findall(twb))),
        "calc_formulas": {f.strip() for f in RE_CALC.findall(twb) if f.strip()},
        "refline_count": len(RE_REFLINE.findall(twb)), "has_rls": bool(RE_RLS.search(twb)),
        "stub": not used and len(sheets) <= 1,
    }


def jaccard(a, b):
    a, b = set(a), set(b)
    u = a | b
    return len(a & b) / len(u) if u else 0.0


def levenshtein_ratio(a, b):
    a, b = (a or "").lower(), (b or "").lower()
    if a == b:
        return 1.0
    if not a or not b:
        return 0.0
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        cur = [i]
        for j, cb in enumerate(b, 1):
            cur.append(min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (ca != cb)))
        prev = cur
    return 1.0 - prev[-1] / max(len(a), len(b))


def tokens_match(a, b):
    if a == b:
        return True
    if min(len(a), len(b)) >= 4 and (a.startswith(b) or b.startswith(a)):
        return True
    pre = 0
    while pre < min(len(a), len(b)) and a[pre] == b[pre]:
        pre += 1
    return pre >= 6


def stem_overlap(sa, sb):
    if not sa or not sb:
        return 0.0
    remaining, matched = list(sb), 0
    for t in sa:
        hit = next((u for u in remaining if tokens_match(t, u)), None)
        if hit is not None:
            matched += 1
            remaining.remove(hit)
    return matched / max(len(sa), len(sb))


def size_ratio(a, b):
    if a == 0 and b == 0:
        return 1.0
    if a == 0 or b == 0:
        return 0.0
    return min(a, b) / max(a, b)


def feature_vector(c):
    v = Counter()
    for f in (c or {}).get("features") or []:
        v[f.get("name")] += int(f.get("count") or 0)
    return v


def cosine(va, vb):
    if not va or not vb:
        return None
    dot = sum(va[k] * vb.get(k, 0) for k in va)
    mag = math.sqrt(sum(x * x for x in va.values())) * math.sqrt(sum(x * x for x in vb.values()))
    return dot / mag if mag else 0.0


def score_pair(pa, pb, fv, floor):
    if len(pa["used_fields"]) >= 4 and len(pb["used_fields"]) >= 4:
        field, basis = jaccard(pa["used_fields"], pb["used_fields"]), "used"
    else:
        field, basis = jaccard(pa["schema_fields"], pb["schema_fields"]), "schema"
    sheet_j = jaccard(pa["sheets"], pb["sheets"])
    structure = (0.6 * sheet_j + 0.25 * size_ratio(len(pa["sheets"]), len(pb["sheets"]))
                 + 0.15 * size_ratio(pa["zones"], pb["zones"]))
    fa, fb = pa["filters"], pb["filters"]
    filt = 1.0 if not fa and not fb else jaccard(fa, fb)
    differing = sorted(k for k in set(fa) & set(fb) if fa[k] != fb[k] and (fa[k] or fb[k]))
    fcos = cosine(fv.get(pa["luid"]), fv.get(pb["luid"]))
    comp = {"field": field, "structure": structure, "filter": filt, "feature": fcos}
    avail = {k: v for k, v in comp.items() if v is not None}
    total_w = sum(WEIGHTS[k] for k in avail) + WEIGHTS["name"]
    partial = sum(WEIGHTS[k] * v for k, v in avail.items())
    if (partial + WEIGHTS["name"]) / total_w < floor:  # cannot reach the floor even with identical names
        return None
    name = max(stem_overlap(name_stem(pa["name"]), name_stem(pb["name"])), levenshtein_ratio(pa["name"], pb["name"]))
    score = (partial + WEIGHTS["name"] * name) / total_w
    return {"score": round(score, 3), "field_overlap": round(field, 3), "field_basis": basis,
            "sheet_jaccard": round(sheet_j, 3), "structure": round(structure, 3), "name_similarity": round(name, 3),
            "filter_similarity": round(filt, 3), "feature_cosine": None if fcos is None else round(fcos, 3),
            "differing_filter_values": differing,
            "shared_sheets": [s for s in pa["sheets"] if s in set(pb["sheets"])],
            "shared_tables": [t for t in pa["warehouse_tables"] if t in set(pb["warehouse_tables"])]}


def candidate_pairs(profiles):
    """Pairs sharing a warehouse table or a published datasource (an inverted index, not all n^2)."""
    index = defaultdict(list)
    for luid, p in profiles.items():
        for t in set(p["warehouse_tables"]) - GENERIC_TABLES:
            index[("t", t)].append(luid)
        for r in p["published_ids"]:
            index[("p", r)].append(luid)
    seen = set()
    for members in index.values():
        members = sorted(members)
        for i, a in enumerate(members):
            for b in members[i + 1:]:
                if (a, b) not in seen:
                    seen.add((a, b))
                    yield a, b


def pct(x):
    return round((x or 0) * 100)


def build_group(gid, members, pairs, profiles, usage, plan_by):
    members = sorted(members, key=lambda l: (-usage.get(l, 0), -len(profiles[l]["sheets"])))
    mset = set(members)
    gp = [p for p in pairs if p["a"] in mset and p["b"] in mset]
    min_field = min(p["field_overlap"] for p in gp)
    min_sheets = min(p["sheet_jaccard"] for p in gp)
    min_score = min(p["score"] for p in gp)
    basis = "schema" if any(p["field_basis"] == "schema" for p in gp) else "used"
    all_stubs = all(profiles[l]["stub"] for l in members)
    shared_tables = sorted(set.intersection(*(set(profiles[l]["warehouse_tables"]) for l in members)) - GENERIC_TABLES)
    diff_fields = sorted({f for p in gp for f in p["differing_filter_values"]})
    sheet_counts = Counter(s for l in members for s in profiles[l]["sheets"])
    unique_sheets = {profiles[l]["name"]: [s for s in profiles[l]["sheets"] if sheet_counts[s] == 1] for l in members}
    variant = list(dict.fromkeys(t for l in members for t in name_tokens(profiles[l]["name"]) if variant_token(t)))
    controls = [{"column": f, "kind": "list-control",
                 "values_observed": sorted({v for l in members for v in profiles[l]["filters"].get(f, [])})}
                for f in diff_fields]
    years = [t for t in variant if YEAR.match(t)]
    if not controls and years:
        controls.append({"column": "(date field)", "kind": "date-range-control", "values_observed": years})
    rls_mismatch = len({profiles[l]["has_rls"] for l in members}) > 1
    calc_sets = [profiles[l]["calc_formulas"] for l in members]
    calc_diff = len(set.union(*calc_sets) - set.intersection(*calc_sets))
    reflines = {profiles[l]["refline_count"] for l in members}
    secondaries_unused = all(not usage.get(l, 0) for l in members[1:])

    if all_stubs:
        rec, reason = "review", ("All members are near-empty publish-test stubs (no fields used, one blank sheet). "
                                 "Keep one (or retire all) rather than migrating each.")
    elif rls_mismatch:
        rec, reason = "review", ("The variants differ in row-level-security functions (USERNAME()/ISMEMBEROF); "
                                 "consolidating could change who sees what. Verify the RLS intent first.")
    elif basis == "used" and min_field >= CONSOLIDATE_MIN_FIELD and min_sheets >= CONSOLIDATE_MIN_SHEETS:
        detail = (f"the only differences map to {len(controls)} control(s)" if controls else
                  "the non-primary variants have no usage: keep the primary, the copies are retire candidates"
                  if secondaries_unused else "the variants are near-identical: keep one")
        rec, reason = "consolidate", f"High overlap in used fields ({pct(min_field)}%+) and sheet structure; {detail}."
    elif min_score >= REVIEW_MIN_SCORE:
        rec, reason = "review", ("Same data and meaningful overlap, but the variants differ in structure or field use "
                                 "beyond what a control covers: compare them side by side first.")
    else:
        rec, reason = "keep-separate", "The workbooks share data but differ in content: migrate separately."

    drivers = [f"schema field overlap {pct(min_field)}% (too few used fields to compare actual use)" if basis == "schema"
               else f"used-field overlap {pct(min_field)}%",
               f"shared sheets: {len({s for p in gp for s in p['shared_sheets']})} "
               f"(sheet-set similarity {pct(min_sheets)}%)"]
    if shared_tables:
        drivers.append("shared warehouse tables: " + ", ".join(shared_tables[:10]))
    if variant:
        drivers.append("name stem overlap (variant tokens: " + ", ".join(variant) + ")")
    if diff_fields:
        drivers.append("same filter fields, values differ on: " + ", ".join(diff_fields))
    diffs = []
    for wb, u in unique_sheets.items():
        if u:
            diffs.append(f"{wb} has {len(u)} sheet(s) not in the others: {', '.join(u[:5])}{', ...' if len(u) > 5 else ''}")
    for f in diff_fields:
        diffs.append(f"filter '{f}' values: " + " | ".join(
            f"{profiles[l]['name']}={'/'.join(profiles[l]['filters'].get(f, []))}" for l in members))
    if rls_mismatch:
        diffs.append("row-level-security functions only in: "
                     + ", ".join(profiles[l]["name"] for l in members if profiles[l]["has_rls"]))
    if calc_diff:
        diffs.append(f"calculated-field formulas differ ({calc_diff} not shared; "
                     + " | ".join(f"{profiles[l]['name']}={len(profiles[l]['calc_formulas'])}" for l in members) + ")")
    if len(reflines) > 1:
        diffs.append("reference-line counts differ: "
                     + " | ".join(f"{profiles[l]['name']}={profiles[l]['refline_count']}" for l in members))
    if not diffs:
        diffs.append("no structural differences detected")
    pub_shared = sorted(set.intersection(*(set(profiles[l]["published_ids"]) for l in members)))
    return {
        "group_id": gid, "recommendation": rec, "recommendation_reason": reason,
        "workbooks": [{"workbook_luid": l, "name": profiles[l]["name"], "accesses": usage.get(l, 0),
                       "sheets": len(profiles[l]["sheets"]), "dashboards": len(profiles[l]["dashboards"]),
                       "used_fields": len(profiles[l]["used_fields"]),
                       "tag": (plan_by.get(l) or {}).get("tag"),
                       "recommended_path": (plan_by.get(l) or {}).get("pre_consolidation_path")
                       or (plan_by.get(l) or {}).get("recommended_path"),
                       "project": (plan_by.get(l) or {}).get("project")} for l in members],
        "primary": {"workbook_luid": members[0], "name": profiles[members[0]]["name"]},
        "shared_datasource": {"warehouse_tables": shared_tables, "published_ids": pub_shared},
        "field_overlap_pct": pct(min_field), "field_basis": basis, "similarity_drivers": drivers,
        "differences": diffs, "proposed_controls": controls,
        "pairwise": [{"a_name": profiles[p["a"]]["name"], "b_name": profiles[p["b"]]["name"],
                      **{k: p[k] for k in ("score", "field_overlap", "field_basis", "sheet_jaccard", "name_similarity",
                                           "filter_similarity", "feature_cosine", "differing_filter_values")}}
                     for p in gp],
        "conversions_avoided": len(members) - 1 if rec == "consolidate" else 0,
    }


def apply_decisions(decisions, groups, dest):
    """Apply stored decisions to migration-plan.json and workbooks.csv; return decisions in force."""
    plan_path, wb_path = dest / "migration-plan.json", dest / "workbooks.csv"
    plan = json.loads(plan_path.read_text(encoding="utf-8")) if plan_path.exists() else None
    role, into, dgroup, ctrls = {}, {}, {}, {}
    for d in decisions:
        if d["decision"] != "consolidate":
            continue
        for l in d["workbooks"]:
            dgroup[l] = d["group_id"]
            if l == d["primary"]:
                role[l], ctrls[l] = "primary", d.get("proposed_controls") or []
            else:
                role[l], into[l] = "merged", d["primary"]
    cand = {w["workbook_luid"]: (g["group_id"], g["recommendation"]) for g in groups for w in g["workbooks"]}

    def update(w):
        luid = w["workbook_luid"]
        if w.get("pre_consolidation_path"):  # undo an earlier application before re-applying
            w["recommended_path"] = w["pre_consolidation_path"]
        for k in ("consolidation_role", "consolidate_into", "consolidation_group", "consolidation_controls",
                  "pre_consolidation_path"):
            w.pop(k, None)
        w["consolidation_candidate"], w["consolidation_recommendation"] = cand.get(luid, ("", ""))
        if luid in role:
            w["consolidation_group"], w["consolidation_role"] = dgroup[luid], role[luid]
            if role[luid] == "primary":
                w["consolidation_controls"] = ctrls[luid]
            else:
                w["pre_consolidation_path"] = w["recommended_path"]
                w["recommended_path"], w["consolidate_into"] = "consolidate-into-primary", into[luid]
        return w

    if plan:
        plan["workbooks"] = [update(w) for w in plan.get("workbooks", [])]
        plan["consolidation"] = {"decisions": decisions}
        plan_path.write_text(json.dumps(plan, indent=2, default=list))
    if wb_path.exists():
        with open(wb_path, encoding="utf-8") as fh:
            rd = csv.DictReader(fh)
            rows, cols = list(rd), list(rd.fieldnames)
        extra = ["consolidation_candidate", "consolidation_recommendation", "consolidation_role", "consolidate_into",
                 "pre_consolidation_path"]
        cols += [c for c in extra if c not in cols]
        rows = [update(r) for r in rows]
        with open(wb_path, "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore", lineterminator="\n")
            w.writeheader()
            w.writerows(rows)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    add_site_args(ap)
    ap.add_argument("--emit-floor", type=float, default=0.45, help="minimum pair score considered at all")
    ap.add_argument("--decide", action="append", default=[], metavar="GROUP=consolidate|as-is",
                    help="record a decision for a group from the last run (repeatable)")
    args = ap.parse_args()
    site, out = resolve_site_args(args)
    dest = out / "report-inventory"
    dest.mkdir(parents=True, exist_ok=True)
    fetched_path = out / "twb-fetch-results.json"
    if not fetched_path.exists():
        sys.exit(f"missing {fetched_path}; run step 04 (fetch_workbooks.py) first")
    fetched = json.loads(fetched_path.read_text(encoding="utf-8"))
    cx_path = out / "complexity.json"
    cx = json.loads(cx_path.read_text(encoding="utf-8")) if cx_path.exists() else {}
    plan_path = dest / "migration-plan.json"
    plan = json.loads(plan_path.read_text(encoding="utf-8")) if plan_path.exists() else {}
    plan_by = {w["workbook_luid"]: w for w in plan.get("workbooks", [])}
    usage = {l: int(w.get("accesses") or 0) for l, w in plan_by.items()}
    if not plan:
        log("No migration-plan.json (step 11): groups are ordered without usage")

    profiles = {}
    todo = [(l, i) for l, i in fetched.items() if not i.get("error") and (out / "twbs" / f"{l}.twb").exists()]
    for n, (luid, info) in enumerate(todo, 1):
        try:
            profiles[luid] = build_profile(luid, info.get("name"), out / "twbs" / f"{luid}.twb")
        except OSError as e:
            log(f"  could not read {info.get('name')}: {e}")
        if n % 250 == 0 or n == len(todo):
            log(f"  profiled {n}/{len(todo)} workbooks")
    fv = {l: feature_vector(c) for l, c in cx.items()}

    pairs, compared = [], 0
    for a, b in candidate_pairs(profiles):
        compared += 1
        s = score_pair(profiles[a], profiles[b], fv, args.emit_floor)
        if s and s["score"] >= args.emit_floor:
            pairs.append({"a": a, "b": b, **s})
    log(f"{compared} workbook pairs share data; {len(pairs)} score >= {args.emit_floor}")

    # Complete-linkage grouping.
    pscore = {tuple(sorted((p["a"], p["b"]))): p["score"] for p in pairs}
    cluster_of = {}
    for p in pairs:
        for l in (p["a"], p["b"]):
            cluster_of.setdefault(l, [l])
    for p in sorted(pairs, key=lambda p: -p["score"]):
        if p["score"] < GROUP_LINK:
            break
        ca, cb = cluster_of[p["a"]], cluster_of[p["b"]]
        if ca is cb:
            continue
        if all(pscore.get(tuple(sorted((x, y))), 0.0) >= GROUP_LINK for x in ca for y in cb):
            ca.extend(cb)
            for l in cb:
                cluster_of[l] = ca
    seen, members_list = set(), []
    for c in cluster_of.values():
        if id(c) not in seen and len(c) >= 2:
            seen.add(id(c))
            members_list.append(list(c))
    grouped = {l for m in members_list for l in m}
    for p in sorted(pairs, key=lambda p: -p["score"]):
        if p["a"] not in grouped and p["b"] not in grouped:
            members_list.append([p["a"], p["b"]])
            grouped |= {p["a"], p["b"]}
    rank = {"consolidate": 0, "review": 1, "keep-separate": 2}
    groups = [build_group("", m, pairs, profiles, usage, plan_by) for m in members_list]
    groups.sort(key=lambda g: (rank[g["recommendation"]], -len(g["workbooks"]),
                               -sum(w["accesses"] for w in g["workbooks"])))
    for i, g in enumerate(groups, 1):
        g["group_id"] = f"consolidation-{i:02d}"

    # Decisions: stored by member workbooks so they survive renumbering.
    dec_path = dest / "consolidation-decisions.json"
    decisions = json.loads(dec_path.read_text(encoding="utf-8")) if dec_path.exists() else []
    by_gid = {g["group_id"]: g for g in groups}
    for d in args.decide:
        gid, _, choice = d.partition("=")
        if choice not in ("consolidate", "as-is"):
            sys.exit(f"--decide expects GROUP=consolidate|as-is, got {d}")
        if gid not in by_gid:
            sys.exit(f"unknown group {gid}; groups this run: {', '.join(by_gid) or 'none'}")
        g = by_gid[gid]
        luids = [w["workbook_luid"] for w in g["workbooks"]]
        decisions = [x for x in decisions if not set(x["workbooks"]) & set(luids)]
        decisions.append({"group_id": gid, "decision": choice, "decided_at": date.today().isoformat(),
                          "workbooks": luids, "names": [w["name"] for w in g["workbooks"]],
                          "primary": g["primary"]["workbook_luid"], "proposed_controls": g["proposed_controls"]})
        log(f"Recorded {gid} -> {choice}")
    if args.decide:
        dec_path.write_text(json.dumps(decisions, indent=2))
    luid_group = {w["workbook_luid"]: g for g in groups for w in g["workbooks"]}
    for d in decisions:  # point each stored decision at this run's group, if it still exists
        g = luid_group.get(d["primary"])
        d["current_group"] = g["group_id"] if g else ""
        if g:
            g["decision"] = d["decision"]
    apply_decisions(decisions, groups, dest)

    summary = {"code_version": code_version(), "workbooks_analyzed": len(profiles), "pairs_compared": compared,
               "groups_total": len(groups), **{k.replace("-", "_"): sum(g["recommendation"] == k for g in groups)
                                               for k in rank},
               "conversions_avoidable": sum(g["conversions_avoided"] for g in groups),
               "decided_consolidate": sum(d["decision"] == "consolidate" for d in decisions),
               "decided_as_is": sum(d["decision"] == "as-is" for d in decisions)}
    (dest / "consolidation-candidates.json").write_text(json.dumps(
        {"generated_at": date.today().isoformat(),
         "params": {"emit_floor": args.emit_floor, "group_link": GROUP_LINK,
                    "consolidate_min_field_overlap": CONSOLIDATE_MIN_FIELD,
                    "consolidate_min_sheet_jaccard": CONSOLIDATE_MIN_SHEETS, "review_min_score": REVIEW_MIN_SCORE},
         "summary": summary, "groups": groups}, indent=2))
    cols = ["group_id", "recommendation", "decision", "role", "workbook", "project", "accesses", "sheets",
            "used_fields", "tag", "recommended_path", "field_overlap_pct", "field_basis", "proposed_controls",
            "similarity_drivers", "differences", "recommendation_reason", "workbook_luid"]
    with open(dest / "consolidation-candidates.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, lineterminator="\n")
        w.writeheader()
        for g in groups:
            for i, m in enumerate(g["workbooks"]):
                w.writerow({"group_id": g["group_id"], "recommendation": g["recommendation"],
                            "decision": g.get("decision", ""), "role": "primary" if i == 0 else "variant",
                            "workbook": m["name"], "project": m["project"], "accesses": m["accesses"],
                            "sheets": m["sheets"], "used_fields": m["used_fields"], "tag": m["tag"],
                            "recommended_path": m["recommended_path"], "field_overlap_pct": g["field_overlap_pct"],
                            "field_basis": g["field_basis"],
                            "proposed_controls": "; ".join(c["column"] for c in g["proposed_controls"]),
                            "similarity_drivers": "; ".join(g["similarity_drivers"]),
                            "differences": "; ".join(g["differences"])[:1000],
                            "recommendation_reason": g["recommendation_reason"], "workbook_luid": m["workbook_luid"]})

    if (dest / "summary.csv").exists():
        inv.replace_summary_section(dest, out.name, "consolidation", [
            ("workbooks_profiled", len(profiles), "downloaded workbooks compared (step 04)"),
            ("groups", len(groups), "groups of workbooks that share data and look alike"),
            ("consolidate", summary["consolidate"], "near-identical variants: one Sigma workbook (+ controls)"),
            ("review", summary["review"], "overlap worth a side-by-side look"),
            ("keep_separate", summary["keep_separate"], "share data, different content"),
            ("conversions_avoidable", summary["conversions_avoidable"],
             "workbooks that need no conversion of their own if consolidate groups are accepted"),
            ("decided_consolidate", summary["decided_consolidate"], "groups the client agreed to consolidate"),
            ("decided_as_is", summary["decided_as_is"], "groups the client chose to keep separate")])
        inv.rebuild_cross_site(out)
    log(f"Consolidation: {len(groups)} groups (consolidate {summary['consolidate']}, review {summary['review']}, "
        f"keep-separate {summary['keep_separate']}); {summary['conversions_avoidable']} conversions avoidable; "
        f"{len(decisions)} decision(s) on file")
    for g in groups[:15]:
        log(f"  {g['group_id']} [{g['recommendation']}{'; ' + g['decision'] if g.get('decision') else ''}] "
            + " + ".join(w["name"] for w in g["workbooks"])[:160])
    if len(groups) > 15:
        log(f"  ... {len(groups) - 15} more in consolidation-candidates.csv")


if __name__ == "__main__":
    main()
