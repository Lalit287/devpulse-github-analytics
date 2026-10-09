# Week 5 — PostgreSQL and Streamlit operations

All commands run from `~/Desktop/DevPulse` using the existing `.venv`. The current
database/dashboard verification status is recorded in `reports/week5_report.md`.
The native database must start from normal Mac Terminal because Codex's execution
sandbox rejects PostgreSQL's `shmget` shared-memory operation. Folder/network access
does not grant that IPC operation.

## Finish and verify Week 5

```bash
cd ~/Desktop/DevPulse
.venv/bin/python -m scripts.complete_week5
```

This command starts the isolated project cluster, copies all 15 existing real Gold
tables, checks a zero-copy rerun, starts the dashboard, runs offline and native
integration tests plus earlier-week regressions, verifies Parquet/SQL parity,
and generates the measured report. A failed check exits with an error and records
pending/failed status. Native integration success is required for `COMPLETE`.
It does not redownload events or refresh GitHub metadata. Initial loading can take
several minutes. Per-table progress is printed; test output is in `reports/week5/`.

PostgreSQL 14.18 is already installed locally. The helper detects Homebrew's
`postgresql@14` binaries even when they are absent from your shell PATH. The Python
dependencies are pinned in `requirements.txt`; the new platform-specific freeze
is `requirements-lock-macos-py312-week5.txt`. The original lock is preserved.

## Normal operation

```bash
.venv/bin/python -m database.cluster start
.venv/bin/python -m database.load_analytics
.venv/bin/python -m dashboard.run start
```

Open [DevPulse](http://127.0.0.1:18501). Database connection endpoint:
`127.0.0.1:15432`, database `devpulse`. Both services bind only to loopback.
The dashboard remains a local development application; shared deployment and
application authentication require separate work.

```bash
.venv/bin/python -m database.cluster status
.venv/bin/python -m dashboard.run status
.venv/bin/python -m dashboard.run stop
.venv/bin/python -m database.cluster stop
```

Stopping services retains all data. PostgreSQL files and three randomly generated
passwords live under private `.runtime/postgres/`; do not commit or remove this
directory while the service runs. The owner identity marker protects the lifecycle
commands from managing another database. Dashboard logs are in
`.runtime/dashboard/streamlit.log`; PostgreSQL logs are in
`.runtime/postgres/postgres.log`. Keep the ports free; the helpers refuse occupied
ports instead of modifying another service. The existing HDFS cluster is separate.

## Pages and data meaning

1. **Activity overview:** clean events, distinct repository/account counts, push,
   star and fork actions, UTC hourly volume, event-type distribution and calendar.
2. **Repositories:** activity/star/intraday/attention/participation rankings,
   literal repository search, minimum activity, language/date filters, CSV,
   two-to-four stable-ID comparisons, and hourly histories.
3. **Languages & technology:** primary-language activity, code-byte allocation,
   coverage and unknown categories, language-associated intraday attention and
   overlapping technology categories.
4. **Public accounts:** searchable account rankings, histories, CSV, and a bounded
   Sankey of the strongest participation edges in the existing 1,000-edge sample.
5. **Popularity prediction:** empty reserved model-output table; actual evaluation
   and predictions are Week 6 deliverables.
6. **Pipeline monitor:** load statuses, source/load identifiers, loaded row counts,
   dataset and database sizes, measured local Spark times and throughput.

The dataset covers **2025-01-01 UTC only**. Date filters therefore cannot demonstrate
weekly trends. Repository date filtering selects repositories observed on those
days; its counts, distinct participation, and scores retain the explicitly labelled
whole-snapshot scope. Intraday scores compare equal 12-hour windows and exclude
low-support candidates. Zero baselines are labelled rather than shown as infinite
growth. GitHub language metadata was fetched later and is linked by stable ID;
it is not a historical language snapshot. Selected metadata covers **1.63%** of
events; a named primary language covers **0.24%**. Unknown rows remain visible.
Accounts include bots, and events do not measure individual productivity.

## Load guarantees and recovery

Gold `_CURRENT.json` and its complete checksummed file inventory are validated
before loading. Arrow batches contain at most 4,096 rows; the full dataset is not
collected into Pandas. Explicit PostgreSQL binary types preserve BIGINT IDs,
dates, NULLs, Unicode, floating-point values, and text arrays. All tables have
contract primary keys and constraints; query indexes are built before activation.

A load holds a PostgreSQL advisory lock. Snapshot tables, row-count/metric/field
checks, registry insertion and logical-view activation share one transaction.
Readers pin a complete schema ID; a new load cannot mix old and new page data.
A failure rolls back staging while the previous completed version stays usable.
The ledger records a failed run without storing secrets or query text.
An unchanged source/code rerun verifies SQL counts/totals and reuses the version
with zero copied rows. Changed source/code produces a separate retained schema.
Control-schema changes require an explicit migration. No automatic destructive
schema cleanup is performed.

If the process is killed hard, its transaction rolls back and the advisory lock is
released by PostgreSQL; its ledger record can remain `running`. Review service logs
and confirm no loader remains before marking an interrupted record failed.
Run the normal loader again to recover; do not delete the current schema.

Allow at least 2 GiB of free space for native tables, indexes and WAL. Old schemas
consume additional space; inspect them before any manual cleanup. Raw gzip, HDFS
bronze, Silver and Gold Parquet stay outside PostgreSQL and are retained.

## Recheck after changes

```bash
.venv/bin/python -m scripts.complete_week5
.venv/bin/python -m scripts.generate_week5_report
```

The completion helper runs 13 offline tests, 11 real database/page tests and 131
earlier-week regression tests. Offline fixtures are restricted to their test
process. The live application has no synthetic-data fallback. Real tests cover
read-only privileges, full-table counts, idempotent reuse, a deliberate mid-load
rollback, real filters/comparisons, accounting and six dashboard pages.
Verification checks all 76 protected source/report hashes and records single local
query timings; these are not distributed scalability benchmarks.

References: [Psycopg binary COPY](https://www.psycopg.org/psycopg3/docs/basic/copy.html),
[Streamlit navigation](https://docs.streamlit.io/develop/api-reference/navigation/st.navigation),
[Streamlit AppTest](https://docs.streamlit.io/develop/api-reference/app-testing/st.testing.v1.apptest).
