#!/usr/bin/env python3
"""Run the pipeline for several sites, one site at a time.

Usage:
    python scripts/run_sites.py --site finance --site ops
    python scripts/run_sites.py --all          # every site in TABLEAU_SITES (.env), else output/_server/sites.json
    python scripts/run_sites.py --site finance --steps 02,03,10 --with-users

Sites run sequentially on purpose: a PAT holds one session at a time, so running
two sites in parallel with the same PAT makes them knock each other's sessions out.

For each site, steps run in order. If step 00 fails, the rest of that site is
skipped and the runner moves on to the next site. If sign-in fails on two sites
in a row, the runner stops (four consecutive failures lock the PAT). Each site writes to
output/<site>/ and a per-site log to output/<site>/run.log.
"""

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from lib.tableau_client import REPO_ROOT, configured_sites, ensure_env, normalize_site, site_folder  # noqa: E402

STEPS = {
    "00": "check_connectivity.py",
    "02": "fetch_view_usage.py",
    "03": "fetch_report_lineage.py",
    "03b": "fetch_workbook_connections.py",
    "10": "build_report_inventory.py",
}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--site", action="append", default=[], help="repeatable; 'default' for the Server Default site")
    ap.add_argument("--all", action="store_true", help="all sites in TABLEAU_SITES, else output/_server/sites.json")
    ap.add_argument("--steps", default=",".join(STEPS), help=f"comma list from {','.join(STEPS)}")
    ap.add_argument("--with-users", action="store_true", help="passed to step 02")
    ap.add_argument("--force-snapshot", action="store_true",
                    help="passed to step 02 as --force: snapshot even if the latest is under 20 h old")
    ap.add_argument("--source", default=None, help="passed to step 02 (auto|rest-snapshots|admin-insights)")
    ap.add_argument("--days", default=None, help="passed to step 02")
    args = ap.parse_args()
    ensure_env()

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
            logf.write(f"\n=== {datetime.now().isoformat(timespec='seconds')} steps {','.join(steps)}\n")
            for step in steps:
                cmd = [sys.executable, str(HERE / STEPS[step]), "--site", site or "default"]
                if step == "02":
                    cmd += ["--with-users"] if args.with_users else []
                    cmd += ["--force"] if args.force_snapshot else []
                    cmd += ["--source", args.source] if args.source else []
                    cmd += ["--days", args.days] if args.days else []
                print(f"-- {step} {STEPS[step]}", flush=True)
                proc = subprocess.run(cmd, capture_output=True, text=True)
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
    sys.exit(1 if any(v != "ok" for r in summary.values() for v in r.values()) else 0)


if __name__ == "__main__":
    main()
