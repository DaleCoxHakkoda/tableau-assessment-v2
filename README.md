# Tableau Assessment (Python)

A Python rebuild of Sigma's
[`tableau-assessment`](https://github.com/sigmacomputing/sigma-migration-skills/tree/main/plugins/tableau-to-sigma/skills/tableau-assessment)
skill, extended to produce what a Tableau-to-Sigma migration actually needs:

- a **report inventory** at dashboard / sheet level, keyed by LUID
- **per-view usage**, for rationalization
- a **report → datasource → warehouse table** lineage graph, keyed `DATABASE.SCHEMA.TABLE`
  so it can be stitched to the upstream pipeline DAG (Talend, dbt, or otherwise)

It runs against **Tableau Server** (primary target) and Tableau Cloud, one site at a time,
with each site's outputs in its own folder, using **only the Tableau APIs** (REST and the
Metadata API). No repository / Postgres access is needed. The upstream skill is Ruby and
drives Admin Insights through the Tableau MCP inside an agent session; this repo calls the
APIs directly, so every step runs headless from a terminal, scheduler or CI job.

## Status

| Steps | State |
|---|---|
| 00 connectivity, 02/02b usage, 03/03b lineage, 10 inventory, multi-site runner | **Ready**, and run against the client's Tableau Server. |
| 04 workbook download, 05 complexity, 11 priority and waves, 12 consolidation | **Ready.** Tested against a mock server and Sigma's sample workbooks; not yet run on the live server. |
| 01 inventory extras (users, extract tasks, jobs) | Backlog, below. Nothing else depends on it. |

## Tableau Server prerequisites

Send this list to the Tableau Server admin before the first run. Step 00 checks each item.

| Need | Why | How |
|---|---|---|
| **Site Administrator role on each site** | Site Admins see all content on their site through REST and the Metadata API. | Granted per site. Step 00 reports your role on every site. |
| **A PAT for that account** | One PAT signs in to every site the account belongs to. | Created by you under My Account Settings. |
| **Metadata API enabled** | Lineage (step 03). A server-level switch, so a Server Admin has to turn it on if step 00 reports it off. | `tsm maintenance metadata-services enable` (Server 2019.3+). The initial index backfill can take hours on a large server. |
| **Network path to the server over HTTPS** | Every step. | Usual firewall / VPN access from the machine running this repo. |
| Internal CA bundle, if any | TLS to an internally signed server cert. | Set `TABLEAU_CA_BUNDLE`. |

### Role on each site matters

Step 00 reports the PAT owner's role on every site. With anything below Site Administrator
(Creator, Explorer, Viewer):

- REST lists only the content you can open, so the inventory covers only that.
- The Metadata API still **lists every asset on the site but returns their names, queries
  and lineage as null.** Step 00 flags this ("lists N workbooks but this PAT can open M").
  Step 03 counts them under `redacted` in `report-lineage-raw.json`; step 10 leaves the
  restricted workbooks out of `reports.csv`, names restricted datasources `(restricted)`,
  marks restricted custom SQL `restricted`, and prints the counts.

The fix is the Site Administrator role on that site; there is no API workaround.

**Audit every site's access in seconds** (sign-in, role and visibility only):

```bash
python scripts/check_connectivity.py --all-sites --roles-only
```

It prints an access summary and writes `output/_server/access-summary.md` (a table ready
to paste into an access request) and `access-summary.csv`. Each site is `FULL` (Site or
Server Administrator), `PARTIAL` (lower role; shows how many workbooks you can open versus
how many the site has), or `NO ACCESS` (sign-in refused: not a member, or a misspelled
contentUrl). Any multi-site step 00 run writes the same summary, and the runner tags each
site's role in its own summary.

### About the PAT

- **One PAT covers all sites** the owner belongs to, with the owner's role on each. Pass the
  site per run with `--site`, or list them in `TABLEAU_SITES` for `--all-sites` and
  `run_sites.py --all`. A Site Admin PAT cannot list the server's sites, so the list has to
  be given. Per-site PATs are supported (see `.env.example`) if any site needs its own account.
- **One session at a time.** Signing in with a PAT ends any other session using it, on any
  site. Run sites **sequentially** (the runner does). Two concurrent runs with the same PAT
  knock each other out; the client re-authenticates once, then fails.
- **Four consecutive failed sign-ins lock the PAT.** Every script makes exactly one sign-in
  attempt and never retries a failure. A sign-in to a site you are not a member of counts as
  a failure, so step 00 and the runner both **stop signing in after two consecutive
  failures** across sites and mark the rest "not attempted". A successful sign-in resets
  the count, so one refused site in the middle of the list does not stop the others.
- **Expiry:** 15 days unused, or one year after creation. The weekly usage snapshot (below)
  keeps the PAT in use, so it also prevents the 15-day expiry during the assessment.

## Setup

Requirements: Python 3.9+, standard library only. Step 05 also needs **Node.js** (18 or
later) on `PATH`, or its path in `NODE_BIN`, for the scanner's formula audit.

```bash
cp .env.example .env    # server URL and PAT
```

## Run sequence

Every script takes `--site <contentUrl>` (the URL segment after `/site/`; use `default` for
the Default site) and writes to `output/<site>/` unless `--out` is given.

```bash
# 00: check every site in TABLEAU_SITES
python scripts/check_connectivity.py --all-sites

# All sites, all ready steps, one after another (this also takes the first usage snapshot)
python scripts/run_sites.py --all

# Then weekly, for the length of the assessment: add a usage snapshot to every site
python scripts/run_sites.py --all --steps 02,10

# Or chosen sites / steps
python scripts/run_sites.py --site finance --site ops --steps 02,03,10

# Migration planning: download workbooks, score complexity, priority/waves, consolidation
python scripts/run_sites.py --all --with-migration
```

The runner always runs steps in pipeline order. `--with-migration` adds 04, 05, 11 and 12
to whatever `--steps` lists. It skips those four on any site where the PAT user is not Site
Administrator (the role step 00 records): Tableau then refuses most workbook downloads
(HTTP 403, code 403019), and the site is outside the totals anyway. The summary shows
`skip(role)`; `--include-partial` runs them regardless. The first step 04 run downloads every workbook (no extract
data, so it stays small); later runs fetch only workbooks republished since.

Single site, step by step:

```bash
python scripts/check_connectivity.py     --site finance   # 00
python scripts/fetch_view_usage.py       --site finance   # 02 (adds a usage snapshot)
python scripts/fetch_report_lineage.py   --site finance   # 03
python scripts/fetch_workbook_connections.py --site finance   # 03b
python scripts/build_report_inventory.py --site finance   # 10 (safe to re-run)
python scripts/fetch_workbooks.py        --site finance   # 04 (resumable)
python scripts/complexity.py             --site finance   # 05 (needs Node.js)
python scripts/workbook_priority.py      --site finance   # 11 (after 10, and 05 if run)
python scripts/consolidation.py          --site finance   # 12 (after 04, and 11 if run)
```

Steps 04 and 05 can run any time before 11; 11 needs step 10's `reports.csv`; 12 reads
11's plan when it exists. Re-running step 10 keeps the summary sections that 11 and 12 add.

The runner skips the rest of a site when its step 00 fails, carries on with the next site,
and appends each site's output to `output/<site>/run.log`.

### Full target pipeline

| # | Script | Status | Reads | Writes |
|---|---|---|---|---|
| 00 | `check_connectivity.py` | ready | Tableau | `connectivity.json`, `_server/sites.json` |
| 01 | `fetch_inventory.py` | backlog (users, groups, extract tasks, jobs) | Tableau | |
| 02 | `fetch_view_usage.py` | ready | REST snapshots (Server) / Admin Insights (Cloud) | `usage-snapshots/`, `view-usage.json`, `view-usage.csv`, `raw-ts-events-per-view.json` |
| 02b | `import_traffic_to_views.py` | ready; the runner runs it when a new export is present | `traffic-to-views/*.csv` (manual export) | `traffic-usage.json`, `.csv`, `traffic-unmatched.csv` |
| 03 | `fetch_report_lineage.py` | ready | Metadata API | `report-lineage-raw.json` |
| 03b | `fetch_workbook_connections.py` | ready | REST connections | `workbook-connections.json` |
| 04 | `fetch_workbooks.py` | ready | REST | `twbs/<luid>.twb`, `twb-fetch-results.json`, `twb-fetch-errors.log` (full error responses) |
| 05 | `complexity.py` | ready (Node.js) | `twbs/`, vendored Sigma scanner | `gaps/<luid>/`, `complexity.json`, `.csv` |
| 10 | `build_report_inventory.py` | ready | 02 to 03b | `report-inventory/`, `_server/` rollups |
| 11 | `workbook_priority.py` | ready | `reports.csv`, `tables.csv`, edges, `complexity.json` | `workbooks.csv`, `clusters.csv`, `pipeline-priority.csv`, `gap-features.csv`, `migration-plan.json`; `_server/*-all-sites.csv` |
| 12 | `consolidation.py` | ready | `twbs/`, `complexity.json`, `migration-plan.json` | `consolidation-candidates.json`, `.csv`; updates `workbooks.csv` and the plan |

## Migration planning (steps 04, 05, 11, 12)

### Population: nothing is filtered out

Every workbook is in the migration population. Retire candidates are **not** excluded
from any count, ranking, cluster, wave, pipeline or gap figure: `retire_candidate` (per
report in `reports.csv`, per workbook in `workbooks.csv`) and the retire counts in the
summaries are information only. Retirement is a suggestion until the customer confirms it;
filtering confirmed retirements is a capability to add explicitly when that happens.
Usage still orders the priority ranking, so a workbook nobody opened ranks lower, but it
keeps its path, wave and place in every count.

### Complexity (step 05)

Runs Sigma's own gap scanner (vendored unchanged in `vendor/tableau-to-sigma/`, Apache 2.0)
on every downloaded workbook. Each Tableau feature found gets a conversion tier: `auto`
(converts), `hint` (converts, verify the formula), `manual` (hand work after publishing) or
`unhandled` (likely lost). **Predicted parity** is the occurrence-weighted share that
converts (auto 1.0, hint 0.85, manual 0.5, unhandled 0), banded A ≥ 90 (drop-in), B 75–90
(light touch-up), C 50–75 (meaningful manual work), D < 50 (hard, re-author risk). It is a
prediction from the file, not a measured result.

Without Node.js the scanner counts every formula as unhandled, so parity is far too low;
step 05 refuses to run unless `--allow-no-node` is passed, and then marks every row
`formula_audit=unavailable`.

### Priority and waves (step 11): `workbooks.csv`

One row per workbook, ranked. The Sigma skill's shortlist, migration plan and datasource
verdicts in one table:

| Column | Meaning |
|---|---|
| `score`, `tag` | value = accesses × √users; cost = 10 × unhandled + 3 × manual + hint features; score = value ÷ (1 + cost). Tags: `needs-gap-review` (any unhandled), `migrate-first` (score ≥ 20, no manual or unhandled), `easy-win` (≥ 10), `moderate`, `not-scanned`. |
| `source_readiness` | Worst of its reports: `in-snowflake`, `warehouse-mapped` (traced to warehouse tables not yet in Snowflake), `needs-mapping` (database known, tables not), `needs-landing` (files, extract-only, localhost). |
| `recommended_path` | `land-data-first`, `convert-with-gap-review`, `convert`; `consolidate-into-primary` once a step 12 decision is recorded. |
| `cluster_id` | Data-model cluster: workbooks that would share one Sigma data model. Linked first by a shared published datasource, then by warehouse-table overlap (Jaccard ≥ 0.5, `--similarity`), ignoring hub tables used by more than 5% of workbooks (`--hub-pct`, e.g. a date dimension). `clusters.csv` lists them. |
| `suggested_wave` | A starting point for the wave plan, set per cluster from its least-ready member, since a cluster moves together: 1 in Snowflake, 2 warehouse-mapped (after its pipelines reach Snowflake), 3 needs mapping or landing. |
| `blockers` | Why it is not wave 1: unhandled or manual features, sources, a missing scan. |

Waves are a proposal to take to the client with the pipeline plan, not a decision.

Step 11's `workbooks` section in `summary.csv` (and the rollup) counts paths, waves and
parity bands, and gives `avg_predicted_parity_pct` over scanned workbooks. Across sites
that average is weighted by each site's `parity_scanned`, not a plain mean of site
averages; `unhandled_features` and `manual_features` sum the gap counts.

### Pipeline priority (step 11): `pipeline-priority.csv`

Which warehouse tables to land in Snowflake first. One row per table that the
workbooks read, ranked. Tables already in Snowflake count as landed. A workbook is
**unlocked** once every table it reads is landed and it has no other blocker (files,
extract-only, localhost, unmapped sources; those are counted as `blocked_beyond_tables`).

Ranking is greedy: each pick is the table with the most pending usage behind it, where a
workbook's opens are shared across the tables it still waits on. A table that completes a
fact-and-dimension set therefore follows its siblings, and a date dimension everything
reads goes early. The cumulative columns read as "landing the first N tables unlocks X% of
the usage (and Y% of the workbooks) that tables alone can unlock".

| Column | Meaning |
|---|---|
| `workbooks`, `accesses`, `sites` | Workbooks that read the table, their opens, and how many sites they are on. |
| `workbooks_unlocked_now` | Workbooks whose last missing table this is. |
| `cumulative_pct_unlockable_accesses`, `cumulative_pct_unlockable_workbooks` | Running share unlocked after landing tables 1 to N. |
| `clusters`, `example_workbooks` | Which data-model clusters and workbooks depend on it. |

`output/_server/pipeline-priority-all-sites.csv` runs the same ranking over every
full-access site together, since one table often feeds several sites. The `pipelines`
section of `summary.csv` gives the milestones (`tables_for_50pct_usage`,
`tables_for_80pct_usage`, and the workbook equivalents); in the rollup they come from the
all-sites ranking, not a sum of sites.

This is an anchor for the pipeline conversation, not a plan: it knows tables, not the
Talend jobs behind them. Once the job inventory exists, rank jobs by the tables they write.

### Gap features (step 11): `gap-features.csv`

A workbook is routed to `convert-with-gap-review` for any unhandled feature, so that count
alone cannot say whether the estate has hundreds of distinct problems or a few repeated
ones. This file answers it: one row per unhandled or manual feature found in the
workbooks, unhandled first, ranked by how many workbooks have it.

| Column | Meaning |
|---|---|
| `workbooks`, `pct_of_scanned`, `occurrences`, `accesses` | How widespread the feature is. |
| `sole_unhandled_for` | Workbooks where it is the only unhandled feature. |
| `cumulative_gap_review_cleared`, `cumulative_pct_gap_review_cleared` | Gap-review workbooks whose unhandled features are all within the top N: the ones a known Sigma pattern for those N would clear. |

`output/_server/gap-features-all-sites.csv` ranks across all full-access sites, and the
`gaps` section of the rollup gives `cleared_by_top5`, `cleared_by_top10` and
`features_for_80pct_gap_review` from it. `audit_unavailable_workbooks` should be 0; if not,
those sites were scanned without Node.js and formula gaps are overstated.

### Consolidation (step 12)

Finds workbooks that could become one Sigma workbook: copies that differ by a filter value,
a year, a region or a copy/test/v2 suffix. A port of the skill's
`consolidation-candidates.rb` with the same scoring and thresholds, and deliberately
conservative: `consolidate` needs ≥ 70% overlap in the fields the sheets use and ≥ 60%
sheet-set similarity for every pair; RLS differences or empty stubs force `review`. Each
group in `consolidation-candidates.csv` carries its evidence (similarity drivers,
differences, proposed controls).

Record the client's answer per group:

```bash
python scripts/consolidation.py --site finance --decide consolidation-03=consolidate --decide consolidation-07=as-is
```

Decisions are stored in `consolidation-decisions.json` by member workbook, so they survive
re-runs and renumbered groups, and are applied to `workbooks.csv` and `migration-plan.json`
on every run.

## Usage on Tableau Server: snapshots (step 02)

With API access only, Tableau Server exposes one usage number per view:
`usage.totalViewCount` from `views?includeUsageStatistics=true`. It is a lifetime counter
with no dates, so on its own it cannot tell a report opened last week from one last opened
three years ago.

Step 02 turns it into a time series. Every run saves the counter for every view to
`output/<site>/usage-snapshots/<UTC timestamp>.json`, then diffs all saved snapshots:

| Column (in `reports.csv`) | Meaning |
|---|---|
| `view_accesses` | Views opened between the first and latest snapshot. The windowed number. |
| `usage_window_days` | Span between first and latest snapshot. |
| `view_active_intervals` / `view_intervals` | Snapshot intervals with any access, out of intervals observed. With weekly snapshots, this is active weeks. |
| `view_last_active_between` | The most recent interval in which the view was opened. |
| `view_lifetime_views`, `view_lifetime_avg_per_30d` | Lifetime count, and its **average** rate over the view's whole life (lifetime views ÷ age in days × 30). Not views in the last 30 days: a view created in 2017 with 11 lifetime views shows 0.1 whether those views were last week or in 2018. Useful from the first snapshot for spotting near-dead content. |
| `view_counter_reset` | The counter went down between two snapshots (usually a republish that recreated the view). That interval is ignored. |

**Recommendation:** take the first snapshot as soon as step 00 passes, then schedule step 02
weekly for every site (cron, Windows Task Scheduler, or a CI schedule running
`python scripts/run_sites.py --all --steps 02,10`). Each run is one sign-in plus a paged
view listing per site. Four to six weeks of snapshots separates live reports from dormant
ones; rationalization reads become credible from there. Keep `usage-snapshots/`: it is the
only usage history you will have. `--rebuild` recomputes from saved snapshots without
calling Tableau.

Reruns are safe. If the latest snapshot for a site is under 20 hours old, step 02 skips the
new snapshot and just recomputes, so testing one site and then running all sites the same
day does not add a near-duplicate snapshot that would distort the interval counts. Use
`--force` (or `run_sites.py --force-snapshot`) to snapshot anyway; tune the threshold with
`SNAPSHOT_MIN_HOURS`.

**Reading the numbers:**

- High lifetime count, zero window accesses: was used, no longer is. Strong retirement
  candidate, after owner confirmation.
- Low lifetime rate, one active interval: possibly a monthly or quarterly report. Do not
  retire on a window shorter than its cycle.
- Created recently: `view_lifetime_avg_per_30d` is noisy until the view has some age.

**Historical backfill from Traffic to Views (step 02b).** Snapshots only see forward. The
built-in **Traffic to Views** admin view (Site Admins can open it from the site's Status
page) has dated, per-user access history for the server's retention window (183 days by
default). Export it once per site:

1. Set the date range to the maximum and clear every other filter.
2. Download **Full Data** as CSV with all columns. Split by month if the download stalls.
3. Copy the file(s) into `output/<site>/traffic-to-views/` **without opening them in Excel**.
   Excel writes `########` for any date column too narrow to display and saves that text
   into the CSV, which destroys the timestamps. (If you must inspect it, open a copy.)
4. Run `python scripts/import_traffic_to_views.py --site <site>`, then step 10.

The window is the period the export covers, not the span of its events: a quiet site may
have only a handful of opens in six months. By default it ends on the date the export file
was saved and starts `TRAFFIC_WINDOW_DAYS` (183) earlier, or at the first event if that is
earlier; pass `--from` and `--to` if the export covered a different range.

The importer matches each row to a current view by Repository Url (the same value as the
REST view `contentUrl`), falling back to project + workbook + view names; `--fetch-map`
signs in once for exact URL matching if the latest snapshot predates the URL field. It
de-duplicates rows that overlap across monthly files, ignores other sites and non-access
events, and lists views that no longer exist in `traffic-unmatched.csv`. Step 10 adds
`traffic_window`, `traffic_accesses`, `traffic_users`, `traffic_last_accessed`,
`traffic_active_weeks` and `traffic_top_users` to `reports.csv`; a report with 0 accesses
was not opened during the export window.

On **Tableau Cloud**, step 02 instead queries Admin Insights `TS Events` (windowed, with
users and weekly activity; `--source admin-insights`, auto-selected on Cloud).

## Outputs that matter for the migration

All in `output/<site>/report-inventory/`:

| File | Grain | Use |
|---|---|---|
| `reports.csv` | one row per report | The migration inventory. A report is a dashboard, or a visible sheet that is not inside any dashboard. Carries workbook, project, owner, datasources, tables, custom SQL and extract flags, per-view usage and retire status. Priority, parity, path, wave and cluster are per workbook, in step 11's `workbooks.csv`. |
| `unpublished_dashboards.csv` | one row per unpublished dashboard | Dashboards the author left out when publishing (no LUID, no URL, no REST view). Not in scope, so excluded from `reports.csv`, edges and table fan-in; kept here for reference. One exception to check by hand: a dashboard hidden because it is shown inside a published story. |
| `tables.csv` | one row per warehouse table | The dependent-models list. Report, workbook and datasource fan-in, total report accesses, and any Prep flows that read the table. |
| `edges_report_datasource.csv` | report × datasource | Graph edges. Embedded datasources that proxy a published one carry its LUID. |
| `edges_datasource_table.csv` | datasource × table | Graph edges, with `via` = `direct`, `custom_sql:tableau` or `custom_sql:regex`. |
| `custom_sql.csv` | one row per custom SQL block | Tables parsed by Tableau where it can, regex FROM/JOIN extraction where it cannot, `unparsed` for manual review. |
| `host-aliases.csv` | one row per raw host spelling | How each host name seen was normalized, so the collapsing can be checked. |
| `summary.csv` | one row per metric | The run's headline numbers (inventory, datasources, lineage status, source types and hosts, snapshot and traffic usage, including the count of candidates to retire) as `site, section, metric, value, description`. Paste or import into a workbook. |
| `lineage-graph.json` | nodes + edges | Load into a graph tool or stitch to the pipeline DAG. |

`lineage_status` in `reports.csv` says why a report has, or lacks, table lineage:

| Value | Meaning | What to do |
|---|---|---|
| `tables` | Warehouse tables found. | Nothing. |
| `no-datasource` | Uses no datasource: text, image, web-page or blank objects only. | Usually a landing or navigation page. Rebuild as a Sigma page; no data dependency. |
| `file-source` | Datasources connect only to files (Excel, CSV, Google Sheets). | Data has to land in the warehouse first; see the skill's `tableau-vds-to-cdw` path. |
| `extract-only` | An extract whose only recorded connection is the extract itself. The data exists only in Tableau. | Highest risk: find the owner and the real source before migrating. |
| `published-unresolved` | Connects to a published datasource that could not be resolved. | Check the published datasource exists and is on this site. |
| `unparsed-custom-sql` | Custom SQL that could not be parsed into tables. | Review the SQL in `custom_sql.csv`. |
| `no-tables-indexed` | Database source, type and host known, but no tables recorded. | Check the source in Tableau; often an extract from a database. `source_hosts` says which server. |
| `no-source-info` | No tables or connection info, even from REST connections. | Run step 03b if you have not; otherwise inspect the workbook. |

`source_types` lists the datasource connection types (e.g. `sqlserver; excel-direct`),
`source_hosts` the database servers behind them (normalized, see below), and `source_info` where that came from:
`metadata` (Metadata API), `rest` (REST connections, step 03b), or `rest-inferred` (the
workbook's only unknown datasource took its only unmatched connections).

**Step 03b** (`fetch_workbook_connections.py`) fills the gaps the Metadata API leaves,
most often extracts reported with no connection or tables. It makes one REST call per
workbook or published datasource that still has an unknown source (or every one, with
`--all`, which also gives server addresses for the whole site) and stores connection type,
server address and port. Usernames are not stored. The runner includes it by default
between steps 03 and 10.

To see *why* reports are `no-source-info` or `no-datasource`, run
`python scripts/diagnose_lineage.py --site <site>`. It reads the local outputs only (no
Tableau calls), groups those reports by what the Metadata API returned for their
datasources (extract with no connection info, proxy to an unresolved published datasource,
the Parameters pseudo-datasource, dashboards with no member sheets, and so on), prints
counts with examples, and writes `report-inventory/lineage-diagnosis.csv`.

### Usage columns: which to read

`reports.csv` carries only the usage columns your deployment can fill. On Tableau Server:

| Question | History (Traffic to Views, ~6 months back) | Ongoing (weekly snapshots, from the first run) |
|---|---|---|
| How often was it opened? | `traffic_accesses` | `view_accesses` |
| By how many people? | `traffic_users` | (not available from snapshots) |
| When was it last opened? | `traffic_last_accessed`, `traffic_days_since_access` | `view_last_active_between` |
| How regularly? | `traffic_active_weeks`, `traffic_active_week_pct` | `view_active_intervals` of `view_intervals`, `view_active_interval_pct` |
| Who uses it? | `traffic_top_users` | (not available from snapshots) |
| Lifetime | | `view_lifetime_views`, `view_lifetime_avg_per_30d` |

The two sources cover different windows (history ends at the export date; snapshots start
at the first run), so their numbers are not expected to match. On Tableau Cloud the
snapshot columns are replaced by `view_actors`, `view_last_accessed`, `view_active_weeks`
and `view_top_users` from Admin Insights.

**Dashboards include their sheets.** A visible sheet inside a dashboard can be opened on
its own tab, and those opens count against the sheet. A dashboard's usage therefore adds
its member sheets' usage, counting each user and each week once; `usage_member_views` says
how many member views were added.

Other usage columns: `usage_source`, `usage_window_days` (where the snapshot numbers came
from and over what period) and `usage_join` (`luid`; `name`, a fallback on workbook + view
name; `members-only`, a dashboard whose usage comes only from its sheets; `not-in-snapshot`,
a view created after the latest snapshot; `no-events`, Cloud with zero accesses).

### Candidates to retire

`retire_candidate` and `retire_reason` flag reports for owner review. They are candidates,
not decisions: a report becomes retired only after its owner confirms.

| `retire_candidate` | Rule |
|---|---|
| `yes` | Not opened in the Traffic to Views window, nor in the snapshot window if there is one. Without Traffic to Views, not opened in a snapshot window of at least 28 days (`RETIRE_MIN_WINDOW_DAYS`). |
| `review` | Not opened, but created inside the window, so too new to judge. |
| `no` | Opened in either window. The reason says how often and where. |
| `unknown` | Not enough windowed usage yet. |
| `n/a` | No-datasource pages (cover, glossary); they migrate with their workbook. |

`retire_reason` states the window, e.g. "not opened 2026-04-30..2026-10-30 (Traffic to
Views) or 2026-10-30..2026-11-10 (usage snapshots); pending owner confirmation", so the
criteria travel with the flag. Annual reports (year-end, regulatory) can look unused in
a six-month window: confirm with the `owner` before acting. `summary.csv` counts each
value under the `retire` section.

### Host names

The same server is spelled many ways across workbooks, so step 10 normalizes every host
before counting it (`scripts/lib/hosts.py`): lower-case; drop a trailing port (`,1433`,
` , 1433`, `:1433`); drop the company domains in `HOST_DOMAINS` (as `.domain` or
`@domain`); and collapse every spelling of the local machine (`local`, `(local)`,
`localhost`, `127.0.0.1`, `::1`, `.`, with or without an instance such as
`(local)\sqlexpress`) to `localhost`. A named instance on a real server is kept
(`p196\inst`). With `HOST_DOMAINS=regence.com`, `P196.regence.com,1433` becomes `p196`.
`host-aliases.csv` lists every raw spelling and what it became. The cross-site rollup applies
the same rules to older site summaries, so they merge correctly too.

`localhost` hosts mean the workbook was built against a database on the author's own
machine: the source is unreachable from the server and needs its real home identified.

### Combining sites

Step 10 also rebuilds three cross-site files in `output/_server/` from every site folder's
`summary.csv`:

| File | Shape | Use |
|---|---|---|
| `summary-rollup.csv` | one row per metric: the all-sites total | The rollup to paste into a workbook or deck. |
| `summary-all-sites-wide.csv` | one row per metric: `ALL_SITES`, then one column per site | Totals next to the per-site breakdown. |
| `summary-sources.csv` | one row per connection type and per database host: `ALL_SITES`, then one column per site, busiest first | The source detail, kept out of the two files above, which show only the distinct counts. |
| `summary-all-sites.csv` | every site's rows stacked, plus `ALL_SITES` rows | Import or pivot tables (includes the source detail). |

The `rollup` column says how each total was formed. Counts are summed (`sum`, or
`sum of N of M sites` when some sites have not run that step). Warehouse tables and
database hosts are counted **once across sites** (`distinct across sites`), because several
sites often share them. The traffic window spans the `earliest` start to the `latest` end.
Role, run time and window lengths are `per-site only` and left out of `summary-rollup.csv`.
Each site's numbers are as of its own last step 10 run; the `generated_at` row shows when.

**Coverage.** Totals include only sites where the PAT owner is a Site (or Server)
Administrator. A site below that (`PARTIAL` in step 00) yields an inventory of only what
the owner can open, so its numbers are kept out of the totals (marked
`PARTIAL, not in totals` in the wide file) and its size is reported instead. The
`coverage` rows at the top of `summary-rollup.csv` give sites by access level, workbooks
assessed, workbooks **not** assessed (the Metadata API's workbook count for partial sites,
a floor), the coverage percentage, and the sites pending access with their current role.
They come from each site's latest step 00 result, so include every site in `TABLEAU_SITES`
and run `check_connectivity.py --all-sites --roles-only`; a site with no step 00 result is
not counted.

Each site's outputs are self-contained. Table keys are identical across sites, so
concatenating `tables.csv` from every site and grouping by `table_key` shows which
warehouse tables serve reports on several sites. That cross-site view is what the pipeline
sequencing needs, because one Talend job usually feeds more than one site.

### Stitching to the pipeline DAG

Table nodes are keyed `DATABASE.SCHEMA.TABLE`, upper-cased with quotes and brackets
removed. Emit target tables from the Talend inventory with the same key and the two graphs
join into one: report → datasource → table → pipeline job → upstream jobs. Any table in
`tables.csv` with no pipeline writer is fed by something else (a Prep flow, a file extract,
a manual load, another tool). Run that coverage check early; it settles whether these
reports really are Talend-fed and how much of the scope sits outside it.

Upper-casing matches Snowflake identifier folding. SQL Server and Oracle sources are
case-insensitive by default, so this works for them too; for case-sensitive Postgres or
Databricks identifiers, adjust `table_key()` in `build_report_inventory.py`.

## Port backlog

The Sigma skill's Ruby scripts were ported only where they add something here:

| Upstream script | Here |
|---|---|
| `fetch-all-twbs.rb` | step 04, `fetch_workbooks.py` |
| `aggregate-complexity.rb` | step 05, `complexity.py`, calling the skill's Python scanner (vendored) |
| `build-shortlist.rb`, `migration-plan.rb`, `analyze-datasources.rb` | merged into step 11, `workbook_priority.py`, using step 03's lineage instead of regex `.twb` table extraction |
| `consolidation-candidates.rb` | step 12, `consolidation.py` (filter fields on datasources with dots in their names now resolve to captions) |
| `inventory-rest.rb` | backlog: step 01, adding REST users with `lastLogin`, groups, extract refresh tasks and recent jobs |

Not porting, with reasons:

| Upstream script | Why |
|---|---|
| `fetch-metadata-graph.rb` | Superseded by step 03. |
| `analyze-views.rb` | Superseded by steps 02, 02b and 10. |
| `analyze-users.rb` | Needs per-user events; on Server, `traffic_top_users` from Traffic to Views covers the useful part. |
| `render-readout.rb`, `render-readout-html.rb` | The readout is built from the CSVs and rollups (the deck). |
| `probe-admin-insights.rb` | Folded into `TableauClient.admin_insights()` and step 00. |
| `setup-tableau.sh`, `get-tableau-token.sh`, `bootstrap.*`, `doctor.*` | Replaced by `.env`, `lib/tableau_client.py` and step 00. |
| `orchestrate-batch.rb`, `test-predicted-parity.rb` | Drive or test the converter agent; not part of an assessment. |

## After the inventory: rationalize and sequence

Planned as step 13 (`sequence_migration.py`) once the pipeline DAG exists:

1. **Rationalize.** Retire on zero usage only after owners and top users confirm. Collapse
   consolidation candidates into one Sigma workbook plus a control (step 12). Share one
   Sigma data model per cluster (step 11).
2. **Compute each surviving report's pipeline closure**: every job upstream of its tables.
3. **Sequence in waves by shared closure.** Reports whose upstream pipelines are migrated or
   unaffected go first; after that, order by value relative to the new jobs each wave pulls
   in. That keeps Sigma waves behind the pipeline work, never ahead of it.

## Caveats

- **Not yet run against a live server.** GraphQL field availability varies by version.
  Step 03 records per-section errors in `report-lineage-raw.json` instead of aborting, and
  retries custom SQL without the version-sensitive `tables` field. Check the `errors` block
  after the first real run.
- **Metadata API completeness.** Step 00 compares the Metadata API's workbook count with
  REST's. A shortfall usually means the index backfill is still running after enablement.
- **Sensitive output.** `output/` holds owner and user emails, custom SQL text and activity
  data. It is git-ignored; keep it that way.

## Versions

The repo version is in `VERSION` at the root. Every script prints it on its first line
(`tableau-assessment-py 2026.11.11.1; loaded config from ...`), `run.log` records it on each
run's header line, and `summary.csv` carries it as `code_version`, so any output can be
traced to the code that produced it. Bump `VERSION` with each change.

## Layout

```
.env.example
VERSION                        repo version, printed by every script
scripts/
  lib/tableau_client.py        .env + per-site config, PAT sign-in, REST, GraphQL, VizQL Data Service
  lib/usage_queries.py         Admin Insights queries (Cloud only)
  check_connectivity.py        00
  fetch_view_usage.py / import_traffic_to_views.py      02 / 02b
  fetch_report_lineage.py / fetch_workbook_connections.py   03 / 03b
  fetch_workbooks.py           04
  complexity.py                05
  build_report_inventory.py    10
  workbook_priority.py         11
  consolidation.py             12
  diagnose_lineage.py          groups unresolved lineage by pattern
  run_sites.py                 multi-site runner
vendor/tableau-to-sigma/       Sigma's gap scanner and converter helpers, unmodified (VENDORED.md)
output/
  _server/sites.json           from step 00 --all-sites (Server Admin PATs only)
  <site>/                      one folder per site; git-ignored
  <site>/usage-snapshots/      weekly view-counter snapshots; the usage history, keep them
```

## Provenance

Derived from `sigmacomputing/sigma-migration-skills` (Apache License 2.0). The gap scanner
in `vendor/tableau-to-sigma/` is copied unmodified with its `LICENSE` and `NOTICE`;
`VENDORED.md` records the upstream commit.
