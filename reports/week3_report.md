# DevPulse — Week 3 Spark ETL Report

**Status: COMPLETE — all Week 3 criteria satisfied.**
Verification recorded at **2026-10-08T17:25:29.748661+00:00**.
All measurements describe the actual real GH Archive day; synthetic records are
used only in isolated automated tests.

## Delivered scope

Week 3 implements the roadmap's defined-schema PySpark ETL, data validation,
deduplication, and clean Parquet dataset milestone, including the blueprint's
baseline daily repository activity table.

- [x] Read all 24 real, checksum-verified HDFS gzip archives
- [x] Defined common schema and preserved nested payload JSON
- [x] Required IDs/timestamps/public flags validated; quarantine retains reasons
- [x] Deterministic global event-ID deduplication and duplicate audit
- [x] Every input line accounted for
- [x] UTC date/hour Snappy Parquet with readback validation
- [x] Hourly writer avoids small-file proliferation with unchanged totals
- [x] Daily repository activity and action-based metrics verified
- [x] New hours extend source inventory without overwriting historical snapshots
- [x] Identical rerun reuses a verified snapshot
- [x] Quota/input failures preserve current dataset; Spark-failure recovery
- [x] Real source-to-Parquet field parity
- [x] Automated ETL tests and previous-week regressions pass
- [x] Week 1/2 saved artifacts and raw sources unchanged
- [x] Reusable CLI, reader helper, contract, and operations guide

## Real input and saved output

The existing Week 2 collection covers 2025-01-01, all 24 UTC archive hours, with
**1,618,723,169 compressed bytes**.
Each local gzip and HDFS bronze file was SHA-256 checked before processing.
Spark reads explicit `hdfs://127.0.0.1:19000/devpulse/bronze/` archive URIs.

| Measurement | Actual result |
|---|---:|
| Source archives | 24 |
| Input physical lines | 3,909,993 |
| Clean unique public events | 3,909,986 |
| Quarantined invalid rows | 0 |
| Excluded duplicate rows | 7 |
| Duplicate IDs with differing line hashes | 0 |
| Distinct repository IDs | 683,676 |
| Distinct active public account IDs | 419,709 |
| Repository/day rows | 683,676 |
| UTC event partitions | 24 |
| Event Parquet files | 24 |
| Snapshot output bytes including checksum sidecars | 2,934,082,972 |
| ETL and persisted-data checks, seconds | 164.171 |
| Full invocation including source/output checksums, seconds | 172.234 |
| Checksum-verified idempotent rerun, seconds | 3.652 |

Event-time range: **2025-01-01T00:00:00.000000Z** through
**2025-01-01T23:59:59.000000Z**. Quality warnings: `{}`.
Quarantine reasons: `{}`. Zero observed exceptions do not replace the
negative test coverage for malformed rows and duplicates.

Current snapshot: `37f73c2302379bc640363b8a`.
The pointer is `data/silver/_CURRENT.json`; its checksum-protected manifest is
`data/silver/snapshots/37f73c2302379bc640363b8a/manifest.json`.
Clean events, quarantine, excluded duplicates, and daily repository tables each
have separate Parquet roots. Full payload JSON is retained for future analytics.
Grouping output by UTC event-date/hour reduced event Parquet files from 4,608 to
24, with the same event totals; the previous full-day snapshot remains verified.
Daily repository totals use eight Parquet files. Event and repository output files
have a 250,000-row cap.

## Clean event distribution

Push events are pushes rather than individual commits. WatchEvent records count
observed activity rather than a repository's current total star inventory.

| Event type | Clean events | Share |
|---|---:|---:|
| PushEvent | 2,704,254 | 69.16% |
| CreateEvent | 404,975 | 10.36% |
| PullRequestEvent | 238,548 | 6.10% |
| WatchEvent | 173,464 | 4.44% |
| IssueCommentEvent | 113,438 | 2.90% |
| DeleteEvent | 93,978 | 2.40% |
| IssuesEvent | 44,940 | 1.15% |
| ForkEvent | 39,772 | 1.02% |
| PullRequestReviewEvent | 36,515 | 0.93% |
| ReleaseEvent | 22,242 | 0.57% |
| PullRequestReviewCommentEvent | 16,142 | 0.41% |
| PublicEvent | 9,736 | 0.25% |
| MemberEvent | 5,458 | 0.14% |
| GollumEvent | 4,282 | 0.11% |
| CommitCommentEvent | 2,242 | 0.06% |

## UTC event partitions

Partitions are derived from parsed event time, independently of nominal archive
hour. Valid late events would remain in their actual UTC event-date/hour partition.

| Event date | UTC hour | Clean events |
|---|---:|---:|
| 2025-01-01 | 00 | 142,347 |
| 2025-01-01 | 01 | 170,960 |
| 2025-01-01 | 02 | 130,356 |
| 2025-01-01 | 03 | 155,884 |
| 2025-01-01 | 04 | 141,011 |
| 2025-01-01 | 05 | 140,309 |
| 2025-01-01 | 06 | 160,133 |
| 2025-01-01 | 07 | 149,305 |
| 2025-01-01 | 08 | 147,528 |
| 2025-01-01 | 09 | 158,102 |
| 2025-01-01 | 10 | 155,750 |
| 2025-01-01 | 11 | 158,512 |
| 2025-01-01 | 12 | 223,540 |
| 2025-01-01 | 13 | 197,224 |
| 2025-01-01 | 14 | 175,304 |
| 2025-01-01 | 15 | 185,049 |
| 2025-01-01 | 16 | 178,036 |
| 2025-01-01 | 17 | 175,988 |
| 2025-01-01 | 18 | 188,459 |
| 2025-01-01 | 19 | 167,103 |
| 2025-01-01 | 20 | 162,106 |
| 2025-01-01 | 21 | 160,796 |
| 2025-01-01 | 22 | 145,196 |
| 2025-01-01 | 23 | 140,988 |

## Daily repository activity

Stable repository IDs determine groups; the latest observed name labels each
repository/day. Distinct actor IDs measure public account participation. The
following totals are checked independently against the saved clean event table.

| Daily-table metric | Total |
|---|---:|
| `total_events` | 3,909,986 |
| `push_events` | 2,704,254 |
| `watch_events` | 173,464 |
| `star_events` | 173,464 |
| `fork_events` | 39,772 |
| `pull_request_events` | 238,548 |
| `pull_requests_opened` | 129,665 |
| `issue_events` | 44,940 |
| `issues_opened` | 26,165 |

Opening issue/PR actions are distinguished from all issue/PR events.
`star_events` counts WatchEvent action `started`. This table does not yet claim
trending scores, language enrichment, developer productivity, or popularity forecasts.

## Reuse, incremental inputs, and failure recovery

The first real hour produced **142,347** clean events. Its immutable
snapshot, `db459d69849e4ddbac45d9e1`, remains checksum verified after the 24-hour
snapshot became current. Added input hours merge into a cumulative source inventory;
changed historical archive checksums are rejected. New recipes recompute the bounded
combined raw batch for exact deduplication. This is incremental source discovery,
not a claim of row-level incremental execution or an ACID table format.

The identical full-day rerun returned **`reused`**, preserved the snapshot
identity, and verified every input checksum and output file checksum without Spark
transformation. Publication uses a writer lock, unique staging directory, verified
readback, atomic rename, and atomic pointer replacement. Failed quotas and mismatched
input counts cannot change the current pointer; those paths are automated tests.

An initial full-day attempt with a 4 GiB driver and 24 shuffle partitions exhausted
Java heap during deduplication. The existing current snapshot remained verified.
The successful configuration uses **local[4]**,
**6g** driver memory,
**192** shuffle partitions, disabled adaptive
partition coalescing, and **1000**-row cache batches.
Cleanup was hardened so an unavailable JVM cannot mask the original failure or
prevent staging cleanup. The earlier failure log and recovery evidence are retained.

## Validation evidence

**110 automated tests pass**, covering the earlier 99 tests
plus Week 3 schema validation, timestamp offsets and precision, nested payloads,
unknown event types, late events, deterministic deduplication, action-based totals,
Parquet readback, rerun reuse, historical source changes, checksum corruption,
quota rollback, and mismatched input counts. Test data is explicitly synthetic.

The real-data integration audit compares **480**
original source records (20 from each archive) against saved Parquet identifiers,
labels, exact nested payload semantics, timestamps, source filename, and line hash.
Full-table checks verify unique event IDs, UTC partitions, repository/day keys,
aggregate totals, and all output file checksums. **32**
saved Week 1/2 artifacts, including every raw archive, retain their original hashes.

Evidence: `reports/week3/etl_tests.xml`, `previous_week_regression.xml`,
`one_hour_run.json`, `full_day_run.json`, `idempotent_rerun.json`,
`integration_verification.json`, `failure_recovery.json`, and corresponding logs.

## Practical limits and next milestone

This is Spark SQL running on a single Apple Silicon Mac with an isolated local HDFS
cluster. It demonstrates processing millions of events and reproducible output,
but is not a multi-node scalability benchmark. Parquet Silver snapshots are saved
on local disk in the project; Bronze inputs are in HDFS. Snapshot retention uses
additional space. The 12 GiB output quota excludes Spark scratch data. A crash may
leave an unreferenced staging directory requiring inspection before removal.

No new packages or global Hadoop settings were required. The schema and operations
are documented in [the ETL guide](../docs/week3_spark_etl.md) and
[the Silver contract](../docs/contracts/silver_events.md).
The next milestone is Week 4 repository/account analytics and cached language
enrichment. Dashboard, model, orchestration/streaming, and final evaluation follow
in Weeks 5–8.
