#!/usr/bin/env python3
"""Run the pipeline for several sites, one site at a time.

Usage:
    python scripts/run_sites.py --site finance --site ops
    python scripts/run_sites.py --all          # every site in TABLEAU_SITES (.env), else output/_server/sites.json
    python scripts/run_sites.py --site finance --steps 02,03,10 --with-users
    python scripts/run_sites.py --all --with-migration   # also 04, 05, 11, 12 (workbook download,
                                                          # complexity, priority/waves, consolidation)

Sites run sequentially on purpose: a PAT holds one session at a time, so running
two sites in parallel with the same PAT makes them knock each other's sessions out.

For each site, steps run in order. If step 00 fails, the rest of that site is
skipped and the runner moves on to the next site. If sign-in fails on two sites
in a row, the runner stops (four consecutive failures lock the PAT). Each site writes to
output/<site>/ and a per-site log to output/<site>/run.log, headed with the code version.

Steps always run in pipeline order (00, 02, 02b, 03, 03b, 04, 05, 10, 11, 12), whatever
order --steps lists them in. The default is the inventory (00-10), which is what a weekly
usage run needs. --with-migration adds the migration-planning steps: step 04 downloads
every workbook the first time (then only republished ones) and step 05 needs Node.js.

Step 02b (import Traffic to Views) runs only when output/<site>/traffic-to-views/ has a
CSV newer than the last import, or the last import predates the current format;
otherwise it is reported as skipped.
"""

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from lib.tableau_client import (REPO_ROOT, code_version, configured_sites, ensure_env,  # noqa: E402
                                normalize_site, site_folder)

STEPS = {
    "00": "check_connectivity.py",
    "02": "fetch_view_usage.py",
    "02b": "import_traffic_to_views.py",
    "03": "fetch_report_lineage.py",
    "03b": "fetch_workbook_connections.py",
    "04": "fetch_workbooks.py",
    "05": "complexity.py",
    "10": "build_report_inventory.py",
    "11": "workbook_priority.py",
    "12": "consolidation.py",
}
INVENTORY_STEPS = ["00", "02", "02b", "03", "03b", "10"]
MIGRATION_STEPS = ["04", "05", "11", "12"]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--site", action="append", default=[], help="repeatable; 'default' for the Server Default site")
    ap.add_argument("--all", action="store_true", help="all sites in TABLEAU_SITES, else output/_server/sites.json")
    ap.add_argument("--steps", default=",".join(INVENTORY_STEPS),
                    help=f"comma list from {','.join(STEPS)} (default: {','.join(INVENTORY_STEPS)})")
    ap.add_argument("--with-migration", action="store_true",
                    help=f"add the migration-planning steps {','.join(MIGRATION_STEPS)}")
    ap.add_argument("--allow-no-node", action="store_true", help="passed to step 05")
    ap.add_argument("--exclude-projects", default=None, help="passed to step 04")
    ap.add_argument("--with-users", action="store_true", help="passed to step 02")
    ap.add_argument("--force-snapshot", action="store_true",
                    help="passed to step 02 as --force: snapshot even if the latest is under 20 h old")
    ap.add_argument("--source", default=None, help="passed to step 02 (auto|rest-snapshots|admin-insights)")
    ap.add_argument("--days", default=None, help="passed to step 02")
    args = ap.parse_args()
    ensure_env()  # also prints the code version

    sites = [normalize_site(s) for s in args.site]
    if args.all:
        listing = REPO_ROOT / "output" / "_server" / "sites.json"
        if configured_sites():
            sites += configured_sites()
        elif listing.exists():
            sites += [s["contentUrl"] for s in json.loads(listing.read_text())]
        else:
            sys.exit("No site list: set TABLEAU_SITES=site1,site2 in .env (Site Admin PATs cannot list sites)")
    sites = list(dict.fromkeys(sites))
    if not sites:
        sys.exit("No sites given: use --site (repeatable) or --all")
    steps = [s.strip() for s in args.steps.split(",") if s.strip()]
    unknown = [s for s in steps if s not in STEPS]
    if unknown:
        sys.exit(f"Unknown step(s): {', '.join(unknown)}")
    if args.with_migration:
        steps += MIGRATION_STEPS
    order = list(STEPS)
    steps = sorted(set(steps), key=order.index)

    summary, streak, stopped = {}, 0, False
    for site in sites:
        if streak >= 2:
            summary[site_folder(site)] = {"-": "not run (stopped after 2 consecutive sign-in failures)"}
            stopped = True
            continue
        folder = site_folder(site)
        out = REPO_ROOT / "output" / folder
        out.mkdir(parents=True, exist_ok=True)
        print(f"\n######## site '{folder}' ########", flush=True)
        results = {}
        with open(out / "run.log", "a", encoding="utf-8") as logf:
            logf.write(f"\n=== {datetime.now().isoformat(timespec='seconds')} steps {','.join(steps)} "
                       f"(tableau-assessment-py {code_version()})\n")
            for step in steps:
                if step == "02b":
                    why = traffic_import_skip_reason(out)
                    if why:
                        print(f"-- 02b skipped: {why}", flush=True)
                        logf.write(f"--- 02b skipped: {why}\n")
                        results[step] = "skip"
                        continue
                # UTF-8 mode, so workbook names outside the Windows code page pass through the pipe.
                cmd = [sys.executable, "-X", "utf8", str(HERE / STEPS[step]), "--site", site or "default"]
                if step == "02":
                    cmd += ["--with-users"] if args.with_users else []
                    cmd += ["--force"] if args.force_snapshot else []
                    cmd += ["--source", args.source] if args.source else []
                    cmd += ["--days", args.days] if args.days else []
                if step == "04" and args.exclude_projects:
                    cmd += ["--exclude-projects", args.exclude_projects]
                if step == "05" and args.allow_no_node:
                    cmd += ["--allow-no-node"]
                print(f"-- {step} {STEPS[step]}", flush=True)
                proc = subprocess.run(cmd, capture_output=True, encoding="utf-8", errors="replace")
                sys.stdout.write(proc.stdout)
                sys.stderr.write(proc.stderr)
                logf.write(f"--- {step} exit {proc.returncode}\n{proc.stdout}{proc.stderr}")
                results[step] = "ok" if proc.returncode == 0 else f"exit {proc.returncode}"
                signin_failed = "/auth/signin -> HTTP" in proc.stderr or "[FAIL] sign-in" in proc.stdout
                if signin_failed:
                    streak += 1
                    print("   sign-in failed; skipping the rest of this site", flush=True)
                    break
                if "Signing in" in proc.stderr or step == "00":
                    streak = 0
                if proc.returncode and step == "00":
                    print("   step 00 failed; skipping the rest of this site", flush=True)
                    break
        summary[folder] = results

    print("\n======== summary ========")
    for folder, results in summary.items():
        role = ""
        cj = REPO_ROOT / "output" / folder / "connectivity.json"
        if cj.exists():
            try:
                r = json.loads(cj.read_text()).get("site_role") or ""
                if r:
                    full = r.startswith(("SiteAdministrator", "ServerAdministrator"))
                    role = f"[{r}{'' if full else ': PARTIAL'}]"
            except ValueError:
                pass
        print(f"  {folder:<24} {role:<34} " + "  ".join(f"{k}:{v}" for k, v in results.items()))
    if stopped:
        print("\nStopped signing in after 2 consecutive failures to protect the PAT. "
              "Run `python scripts/check_connectivity.py --all-sites --roles-only` to see which sites refuse it.")
    sys.exit(1 if any(v not in ("ok", "skip") for r in summary.values() for v in r.values()) else 0)


def traffic_import_skip_reason(out):
    """None if step 02b should run for this site, else why it is skipped."""
    csvs = list((out / "traffic-to-views").glob("*.csv")) if (out / "traffic-to-views").exists() else []
    if not csvs:
        return "no Traffic to Views export in traffic-to-views/"
    done = out / "traffic-usage.json"
    if not done.exists():
        return None
    try:
        fmt = json.loads(done.read_text(encoding="utf-8")).get("window", {}).get("format", 1)
    except ValueError:
        return None
    if fmt < 3:  # imported by older code: re-import (format 3 records the export's full window)
        return None
    if max(p.stat().st_mtime for p in csvs) > done.stat().st_mtime:
        return None
    return "already imported (no newer export)"


if __name__ == "__main__":
    main()
