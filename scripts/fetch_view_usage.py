#!/usr/bin/env python3
"""Step 02: per-view (dashboard / sheet) usage.

Usage:
    python scripts/fetch_view_usage.py --site <site>            # take a snapshot and recompute
    python scripts/fetch_view_usage.py --site <site> --rebuild  # recompute from saved snapshots only
    python scripts/fetch_view_usage.py --site <site> --force    # snapshot even if the latest is recent

Tableau Server (API only): REST snapshots
------------------------------------------
REST exposes one usage number per view: usage.totalViewCount, a lifetime counter
with no dates. Each run of this step saves that counter for every view to
<out>/usage-snapshots/<UTC timestamp>.json, then diffs all saved snapshots:

  * accesses           views opened between the first and latest snapshot (windowed)
  * tracked_days       how long this view has been tracked
  * active_intervals   snapshot intervals with any access, out of intervals observed
  * last_active_between the most recent interval in which the view was opened
  * lifetime_views, lifetime_avg_views_per_30d (average rate over the view's whole life
    since creation; NOT views in the last 30 days)

Take the first snapshot as early as possible and schedule this step (weekly is
plenty) for the length of the assessment. One snapshot gives lifetime numbers
only; windowed usage starts with the second.

A counter that goes DOWN between snapshots (typically a workbook republished in
a way that recreated the view) is flagged counter_reset and that interval is
ignored.

Tableau Cloud: Admin Insights
-----------------------------
On Cloud, --source admin-insights (auto-selected) queries TS Events over the
VizQL Data Service instead: windowed, with users and weekly activity.

Writes to <out>/ (default output/<site>/):
  usage-snapshots/<ts>.json      (REST) raw counter snapshots; keep them, they are the history
  view-usage.json / .csv         one record per view LUID, plus the window and source used
  view-users.csv                 (Cloud, --with-users) view x user access counts
  raw-ts-events-per-view.json    {"data": [...]} in the shape the skill's analyze-views step reads
"""

import argparse
import csv
import json
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib import usage_queries as uq  # noqa: E402
from lib.tableau_client import (TableauClient, TableauError, add_site_args, cfg,  # noqa: E402
                                deployment, log, resolve_site_args, site_folder)

SNAP_FMT = "%Y%m%dT%H%M%SZ"


def latest_snapshot_age_hours(snap_dir):
    """Hours since the newest snapshot, or None if there are none."""
    times = []
    for p in snap_dir.glob("*.json") if snap_dir.exists() else []:
        try:
            times.append(datetime.strptime(p.stem, SNAP_FMT).replace(tzinfo=timezone.utc))
        except ValueError:
            continue
    if not times:
        return None
    return (datetime.now(timezone.utc) - max(times)).total_seconds() / 3600


def num(v):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def day(v):
    return str(v)[:10] if v else None


def parse_ts(v):
    if not v:
        return None
    try:
        return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    except ValueError:
        return None


# ---------------------------------------------------------------- REST snapshots (Server)
def take_snapshot(site, snap_dir):
    with TableauClient(site) as client:
        log("REST: listing workbooks")
        wbs = {w["id"]: w for w in client.rest_paged("/workbooks", "workbooks", "workbook", page_size=100)}
        log("REST: listing views with usage statistics")
        views = {}
        for v in client.rest_paged("/views?includeUsageStatistics=true", "views", "view", page_size=100):
            wb = wbs.get((v.get("workbook") or {}).get("id")) or {}
            views[v["id"]] = {
                "name": v.get("name"), "sheet_type": v.get("sheetType"), "content_url": v.get("contentUrl"),
                "workbook_luid": wb.get("id"), "workbook": wb.get("name"),
                "project": (wb.get("project") or {}).get("name"),
                "created_at": v.get("createdAt") or wb.get("createdAt"),
                "total_view_count": num((v.get("usage") or {}).get("totalViewCount")),
            }
    ts = datetime.now(timezone.utc).replace(microsecond=0)
    snap_dir.mkdir(parents=True, exist_ok=True)
    path = snap_dir / f"{ts.strftime(SNAP_FMT)}.json"
    path.write_text(json.dumps({"taken_at": ts.isoformat(), "site": site_folder(site), "views": views}, indent=1))
    log(f"Snapshot {path.name}: {len(views)} views")


def load_snapshots(snap_dir):
    snaps = []
    for p in sorted(snap_dir.glob("*.json")):
        try:
            doc = json.loads(p.read_text())
            snaps.append((parse_ts(doc["taken_at"]), doc["views"]))
        except (ValueError, KeyError) as e:
            log(f"  skipping unreadable snapshot {p.name}: {e}")
    return sorted(snaps, key=lambda s: s[0])


def usage_from_snapshots(snaps):
    first_ts, latest_ts = snaps[0][0], snaps[-1][0]
    latest = snaps[-1][1]
    now = datetime.now(timezone.utc)
    out = {}
    for vid, cur in latest.items():
        series = [(ts, v[vid]["total_view_count"]) for ts, v in snaps
                  if vid in v and v[vid].get("total_view_count") is not None]
        rec = {
            "view_luid": vid, "view_name": cur.get("name"), "workbook": cur.get("workbook"),
            "project": cur.get("project"), "sheet_type": cur.get("sheet_type"),
            "created_at": day(cur.get("created_at")),
            "lifetime_views": cur.get("total_view_count"),
            "first_tracked": day(series[0][0].isoformat()) if series else None,
            "snapshots": len(series),
        }
        created = parse_ts(cur.get("created_at"))
        if created and rec["lifetime_views"] is not None:
            if created.tzinfo is None:
                created = created.replace(tzinfo=timezone.utc)
            age = max(1.0, (now - created).total_seconds() / 86400)
            rec["age_days"] = round(age)
            rec["lifetime_avg_views_per_30d"] = round(rec["lifetime_views"] / age * 30, 2)
        if len(series) >= 2:
            accesses, active, observed, reset, last_active = 0, 0, 0, False, None
            active_keys = []
            for (t1, c1), (t2, c2) in zip(series, series[1:]):
                d = c2 - c1
                if d < 0:
                    reset = True
                    continue
                observed += 1
                accesses += d
                if d > 0:
                    active += 1
                    last_active = (t1, t2)
                    active_keys.append(t2.strftime(SNAP_FMT))
            rec.update({
                "accesses": accesses,
                "tracked_days": round((series[-1][0] - series[0][0]).total_seconds() / 86400, 1),
                "active_intervals": active, "intervals": observed,
                "active_interval_pct": round(100 * active / observed) if observed else None,
                "last_active_between": f"{day(last_active[0].isoformat())}..{day(last_active[1].isoformat())}"
                if last_active else None,
                "counter_reset": reset,
                "active_interval_keys": active_keys,
            })
        out[vid] = rec
    meta = {"first_snapshot": first_ts.isoformat(), "latest_snapshot": latest_ts.isoformat(),
            "snapshots": len(snaps),
            "usage_days": round((latest_ts - first_ts).total_seconds() / 86400, 1) if len(snaps) > 1 else "lifetime"}
    return out, meta


# ---------------------------------------------------------------- Admin Insights (Cloud)
def from_admin_insights(site, days, weekly_on, with_users, errors):
    min_date = (date.today() - timedelta(days=days)).isoformat()
    with TableauClient(site) as client:
        ai = client.admin_insights()
        luid = ai.get("TS Events")
        if not luid:
            raise TableauError(f"Admin Insights is visible but 'TS Events' is missing (found: {', '.join(ai)})")
        log(f"Admin Insights: window {days} days (since {min_date})")
        rows = client.vds_query(luid, uq.ai_per_view(min_date))
        weekly = users = []
        if weekly_on:
            try:
                weekly = client.vds_query(luid, uq.ai_per_view_week(min_date))
            except TableauError as e:
                errors["weekly"] = str(e)
        if with_users:
            try:
                users = client.vds_query(luid, uq.ai_per_view_user(min_date))
            except TableauError as e:
                errors["users"] = str(e)

    window_weeks = max(1, -(-days // 7))
    weeks = defaultdict(set)
    for w in weekly:
        if (num(w.get("accesses")) or 0) > 0:
            weeks[w.get("view_luid")].add(day(w.get("week")))
    by_user = defaultdict(list)
    for u in users:
        by_user[u.get("view_luid")].append((u.get("user"), num(u.get("accesses")) or 0))
    out = {}
    for r in rows:
        vid = r.get("view_luid")
        if not vid:
            continue
        rec = out.setdefault(vid, {"view_luid": vid, "view_name": r.get("view_name"), "workbook": r.get("workbook"),
                                   "project": r.get("project"), "accesses": 0, "actors": 0,
                                   "first_accessed": None, "last_accessed": None})
        rec["accesses"] += num(r.get("accesses")) or 0
        rec["actors"] = max(rec["actors"], num(r.get("actors")) or 0)
        f = [x for x in (rec["first_accessed"], day(r.get("first_accessed"))) if x]
        l = [x for x in (rec["last_accessed"], day(r.get("last_accessed"))) if x]
        rec["first_accessed"], rec["last_accessed"] = (min(f) if f else None), (max(l) if l else None)
    for vid, rec in out.items():
        if rec["last_accessed"]:
            rec["days_since_last_access"] = (date.today() - date.fromisoformat(rec["last_accessed"])).days
        if weekly:
            rec["active_weeks"] = len(weeks.get(vid, ()))
            rec["active_week_pct"] = round(100 * rec["active_weeks"] / window_weeks)
        if users:
            top = sorted(by_user.get(vid, []), key=lambda x: -x[1])[:5]
            rec["top_users"] = "; ".join(f"{u} ({n})" for u, n in top)
    return out, {"usage_days": days, "min_date": min_date}, users


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    add_site_args(ap)
    ap.add_argument("--source", choices=["auto", "rest-snapshots", "admin-insights"], default="auto",
                    help="auto = admin-insights on Tableau Cloud, rest-snapshots on Tableau Server")
    ap.add_argument("--rebuild", action="store_true",
                    help="(rest-snapshots) recompute from saved snapshots without calling Tableau")
    ap.add_argument("--force", action="store_true",
                    help="(rest-snapshots) take a snapshot even if the latest is newer than --min-hours")
    ap.add_argument("--min-hours", type=float, default=float(cfg("SNAPSHOT_MIN_HOURS", None, "20")),
                    help="(rest-snapshots) skip the snapshot if the latest is newer than this "
                         "(default 20, or SNAPSHOT_MIN_HOURS)")
    ap.add_argument("--days", type=int, default=None, help="(admin-insights) window, default 90")
    ap.add_argument("--no-weekly", action="store_true", help="(admin-insights) skip the per-week query")
    ap.add_argument("--with-users", action="store_true", help="(admin-insights) view x user access counts")
    args = ap.parse_args()
    site, out = resolve_site_args(args)

    source = args.source
    if source == "auto":
        cloud = deployment(cfg("TABLEAU_SERVER_URL", site) or "", site) == "cloud"
        source = "admin-insights" if cloud else "rest-snapshots"
    log(f"Site '{site_folder(site)}': usage source {source}")
    errors, users = {}, []

    if source == "rest-snapshots":
        if args.with_users:
            log("Note: --with-users needs Admin Insights (Cloud); REST has no per-user view history")
        snap_dir = out / "usage-snapshots"
        age = latest_snapshot_age_hours(snap_dir)
        if args.rebuild:
            pass
        elif age is not None and age < args.min_hours and not args.force:
            log(f"Latest snapshot is {age:.1f} h old (< {args.min_hours:g} h): skipping the new snapshot and "
                f"recomputing from saved ones. Use --force to snapshot anyway.")
        else:
            take_snapshot(site, snap_dir)
        snaps = load_snapshots(snap_dir)
        if not snaps:
            sys.exit(f"No snapshots in {snap_dir}; run without --rebuild first")
        views, meta = usage_from_snapshots(snaps)
        if len(snaps) == 1:
            log("First snapshot: lifetime counts only. Re-run this step later (weekly is plenty) "
                "to get windowed usage; every run adds a snapshot.")
        else:
            log(f"{len(snaps)} snapshots over {meta['usage_days']} days")
    else:
        days = args.days or int(cfg("USAGE_DAYS", site, "90"))
        views, meta, users = from_admin_insights(site, days, not args.no_weekly, args.with_users, errors)

    meta.update({"format": 2, "computed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                 "site": site_folder(site), "source": source, "errors": errors})
    (out / "view-usage.json").write_text(json.dumps({"window": meta, "views": views}, indent=2, default=str))

    preferred = ["view_luid", "view_name", "workbook", "project", "sheet_type", "accesses", "actors",
                 "tracked_days", "active_intervals", "intervals", "active_interval_pct", "last_active_between",
                 "first_accessed", "last_accessed", "days_since_last_access", "active_weeks", "active_week_pct",
                 "lifetime_views", "lifetime_avg_views_per_30d", "created_at", "age_days", "first_tracked", "snapshots",
                 "counter_reset", "top_users"]
    present = {k for v in views.values() for k in v}
    cols = [c for c in preferred if c in present]
    with open(out / "view-usage.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore", lineterminator="\n")
        w.writeheader()
        key = "accesses" if "accesses" in present else "lifetime_views"
        for rec in sorted(views.values(), key=lambda r: -(r.get(key) or 0)):
            w.writerow(rec)

    if users:
        with open(out / "view-users.csv", "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh, lineterminator="\n")
            w.writerow(["view_luid", "view_name", "workbook", "user", "accesses"])
            for u in users:
                v = views.get(u.get("view_luid"), {})
                w.writerow([u.get("view_luid"), v.get("view_name"), v.get("workbook"), u.get("user"),
                            num(u.get("accesses"))])

    compat = [{"Item LUID": v["view_luid"], "Item Name": v.get("view_name"), "Workbook Name": v.get("workbook"),
               "accesses": v.get("accesses", v.get("lifetime_views")), "actors": v.get("actors")}
              for v in views.values()]
    (out / "raw-ts-events-per-view.json").write_text(json.dumps({"data": compat}, indent=2))
    log(f"Wrote view-usage.json / view-usage.csv for {len(views)} views to {out}/")


if __name__ == "__main__":
    try:
        main()
    except TableauError as e:
        sys.exit(f"ERROR: {e}")
