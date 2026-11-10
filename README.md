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
| 00 connectivity, 02 usage, 03 lineage, 10 inventory, multi-site runner | **Ready.** Tested against a mock Tableau Server (including changing view counters and the one-session-per-PAT rule). Not yet run against a live server. |
| 01, 04 to 09, 11, 12 | Port backlog, below. The ready steps do not depend on them. |

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

Requirements: Python 3.9+, standard library only. Nothing to install.

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
```

Single site, step by step:

```bash
python scripts/check_connectivity.py     --site finance   # 00
python scripts/fetch_view_usage.py       --site finance   # 02 (adds a usage snapshot)
python scripts/fetch_report_lineage.py   --site finance   # 03
python scripts/fetch_workbook_connections.py --site finance   # 03b
python scripts/build_report_inventory.py --site finance   # 10 (always last; safe to re-run)
```

The runner skips the rest of a site when its step 00 fails, carries on with the next site,
and appends each site's output to `output/<site>/run.log`.

### Full target pipeline

| # | Script | Status | Reads | Writes |
|---|---|---|---|---|
| 00 | `check_connectivity.py` | ready | Tableau | `connectivity.json`, `_server/sites.json` |
| 01 | `fetch_inventory.py` | **new** (Server: port `inventory-rest.rb` and add REST users, groups, extract tasks and jobs; Cloud: the skill's Admin Insights recipes) | Tableau | `inventory.json`, `users.json`-input files |
| 02 | `fetch_view_usage.py` | ready | REST snapshots (Server) / Admin Insights (Cloud) | `usage-snapshots/`, `view-usage.json`, `view-usage.csv`, `raw-ts-events-per-view.json` |
| 02b | `import_traffic_to_views.py` | ready (one-time per site) | `traffic-to-views/*.csv` (manual export) | `traffic-usage.json`, `.csv`, `traffic-unmatched.csv` |
| 03 | `fetch_report_lineage.py` | ready | Metadata API | `report-lineage-raw.json` |
| 03b | `fetch_workbook_connections.py` | ready | REST connections | `workbook-connections.json` |
| 04 | `fetch_all_twbs.py` | port `fetch-all-twbs.rb` | REST | `twbs/<luid>.twb`, `twb-fetch-results.json` |
| 05 | `aggregate_complexity.py` | port `aggregate-complexity.rb` | `twbs/` | `complexity.json` |
| 06 | `build_shortlist.py` | port `build-shortlist.rb` | `inventory.json`, `complexity.json`, `view-usage.json` | `shortlist.json` |
| 07 | `analyze_datasources.py` | port `analyze-datasources.rb` | `report-lineage-raw.json` | `data-sources.json` |
| 08 | `migration_plan.py` | port `migration-plan.rb` | `shortlist.json`, `data-sources.json`, `report-lineage-raw.json` | `migration-plan.json` |
| 09 | `consolidation_candidates.py` | port `consolidation-candidates.rb` | `twb-fetch-results.json`, `shortlist.json`, `complexity.json`, `migration-plan.json` | `consolidation-candidates.json` |
| 10 | `build_report_inventory.py` | ready | everything above that exists | `report-inventory/` |
| 11 | `analyze_users.py` | port `analyze-users.rb` (optional) | `raw-ts-users.json`, `raw-ts-events-per-user.json`, `shortlist.json` | `users.json` |
| 12 | `render_readout.py` | port `render-readout.rb` (optional) | `inventory.json`, `complexity.json`, `shortlist.json` | `readout.md` |

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
| `reports.csv` | one row per report | The migration inventory. A report is a dashboard, or a visible sheet that is not inside any dashboard. Carries workbook, project, owner, datasources, tables, custom SQL and extract flags, per-view usage, and (once ported) shortlist tag, predicted parity, recommended path and DM cluster. |
| `unpublished_dashboards.csv` | one row per unpublished dashboard | Dashboards the author left out when publishing (no LUID, no URL, no REST view). Not in scope, so excluded from `reports.csv`, edges and table fan-in; kept here for reference. One exception to check by hand: a dashboard hidden because it is shown inside a published story. |
| `tables.csv` | one row per warehouse table | The dependent-models list. Report, workbook and datasource fan-in, total report accesses, and any Prep flows that read the table. |
| `edges_report_datasource.csv` | report × datasource | Graph edges. Embedded datasources that proxy a published one carry its LUID. |
| `edges_datasource_table.csv` | datasource × table | Graph edges, with `via` = `direct`, `custom_sql:tableau` or `custom_sql:regex`. |
| `custom_sql.csv` | one row per custom SQL block | Tables parsed by Tableau where it can, regex FROM/JOIN extraction where it cannot, `unparsed` for manual review. |
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
`source_hosts` the database servers behind them, and `source_info` where that came from:
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

Key usage columns in `reports.csv`:

- `usage_source`, `usage_window_days`: where the numbers came from and over what period.
- The snapshot columns described above (Server), or `view_actors`, `view_last_accessed`,
  `view_active_weeks` and `view_top_users` (Cloud, Admin Insights).
- `usage_join`: `luid`, `name` (fallback on workbook + view name), `not-in-snapshot` (view
  created after the latest snapshot), or `no-events` (Cloud: zero accesses in the window).

### Combining sites

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

Recommended order. Wave 1 is everything rationalization and sequencing depend on.

| Wave | Script | Ruby lines | Notes |
|---|---|---|---|
| 1 | `fetch_inventory.py` (step 01) | 127 (`inventory-rest.rb`) | On Server, port `inventory-rest.rb` for counts and add the REST calls that cover what Admin Insights gives on Cloud: users with site role and `lastLogin`, groups, extract refresh tasks and their schedules, and recent jobs (REST job history is short, so snapshot it like usage). Add Cloud's Admin Insights recipes only if any site is on Cloud. |
| 1 | `fetch_all_twbs.py` | 253 | Keep the upstream behaviors: resumable, `.twbx` unzip, backoff on 429/5xx, `--limit` for a sanity pass. Use a thread pool over one client; all threads share the PAT's single session. |
| 1 | `aggregate_complexity.py` | 119 | Call the upstream **Python** scanner, `tableau-to-sigma/scripts/scan-workbook-gaps.py` (already ported upstream, Apache 2.0, with tests). Its report format may differ from what the Ruby aggregator parses, so verify against one workbook first. Needs Node.js for its formula-audit helper. |
| 1 | `build_shortlist.py` | 136 | Small. Use per-view usage from step 02 instead of workbook totals. |
| 1 | `analyze_datasources.py` | 320 | Retarget from `metadata-graph.json` to `report-lineage-raw.json`, which is paginated and LUID-keyed. Keep the connection-type → Sigma-readiness verdict constants. |
| 1 | `migration_plan.py` | 317 | Replace its regex `.twb` table extraction with the lineage tables from step 03, which already include custom SQL. |
| 1 | `consolidation_candidates.py` | 578 | The largest analytical port; it finds variant workbooks that collapse into one Sigma workbook plus a control. High value for rationalization. |
| 2 | `analyze_users.py` | 270 | User segmentation from REST users and `lastLogin`. Per-user view history is not available from the Server APIs, so pilot coverage by user is Cloud-only unless the Traffic to Views export is used. |
| 2 | `render_readout.py` | 313 | Markdown readout from the JSON outputs. |

Not porting, with reasons:

| Upstream script | Why |
|---|---|
| `fetch-metadata-graph.rb` | Superseded by step 03. Revisit only if you later run the `tableau-to-sigma` converter and want it to read calc formulas from this folder; that needs the `CalculatedField` fragment added to step 03. |
| `analyze-views.rb` | Superseded by steps 02 and 10. Step 02 still writes `raw-ts-events-per-view.json` in its input shape. |
| `probe-admin-insights.rb` | Folded into `TableauClient.admin_insights()` and step 00. |
| `setup-tableau.sh`, `get-tableau-token.sh`, `bootstrap.*`, `doctor.*` | Replaced by `.env`, `lib/tableau_client.py` and step 00. |
| `render-readout-html.rb` (1,556 lines) | Presentation only; build the client readout from the CSVs instead. |
| `orchestrate-batch.rb` | Only useful for driving `tableau-to-sigma` agent conversions in Claude Code. |

## After the inventory: rationalize and sequence

Planned as step 13 (`sequence_migration.py`) once the pipeline DAG exists:

1. **Rationalize.** Retire on zero usage only after owners and top users confirm. Collapse
   consolidation candidates into one Sigma workbook plus a control. Share one Sigma data
   model per DM cluster.
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

## Layout

```
.env.example
scripts/
  lib/tableau_client.py        .env + per-site config, PAT sign-in, REST, GraphQL, VizQL Data Service
  lib/usage_queries.py         Admin Insights queries (Cloud only)
  check_connectivity.py        00
  fetch_view_usage.py          02
  fetch_report_lineage.py      03
  build_report_inventory.py    10
  run_sites.py                 multi-site runner
output/
  _server/sites.json           from step 00 --all-sites (Server Admin PATs only)
  <site>/                      one folder per site; git-ignored
  <site>/usage-snapshots/      weekly view-counter snapshots; the usage history, keep them
```

## Provenance

Derived from `sigmacomputing/sigma-migration-skills` (Apache License 2.0). If upstream
code is copied in during the port (for example the gap scanner), keep its license header
and add the upstream `LICENSE` and any `NOTICE` file to this repo.
