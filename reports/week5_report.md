# Week 5 — PostgreSQL and interactive dashboard

**Status: COMPLETE.**

Report generated: 2026-10-08T18:52:48.920297+00:00.

## Implemented deliverables

- Project-owned native PostgreSQL cluster on loopback port 15432 with private credentials and separate owner/writer/reader roles.
- Bounded binary COPY from all 15 verified Gold Parquet tables into typed snapshot schemas, indexes, full row-count and aggregate checks, and sampled field parity.
- Atomic activation, unchanged-load reuse, advisory loader lock, durable run ledger, and transaction rollback drill.
- Six Streamlit pages: overview, repositories and comparisons, languages/technology, public accounts and participation, prediction status, and pipeline monitoring.
- Parameterized read-only queries, stable-ID grouping, bounded search/rankings, CSV exports, snapshot-aware caching, and explicit coverage/window labels.

The prediction page reserves Week 6 model output; it does not invent probabilities.

## Validation evidence

| Group | Status | Tests |
|---|---|---:|
| offline_tests | passed | 13 |
| database_tests | passed | 11 |
| previous_week_regression | passed | 131 |

Offline page tests use explicitly synthetic fixtures only inside Pytest. They establish UI behavior and do not establish a successful PostgreSQL load.

Native server: PostgreSQL 14.18 (Homebrew).

All 15 loaded tables match the current Gold snapshot `6e03839285499224b0403539`; clean-event total: **3,909,986**.

Active load: `61745299597589669e231948`. Verification checks read-only grants, zero-copy rerun, failed-load rollback, dashboard HTTP health, and all 76 protected source/report hashes.

Query measurements are single local requests including connection time, not distributed benchmarks.

| Ranking | Rows | Seconds |
|---|---:|---:|
| Activity | 20 | 0.0045 |
| Star actions | 20 | 0.0037 |
| Intraday score | 20 | 0.0037 |
| Attention growth | 20 | 0.0037 |
| Participation | 20 | 0.1112 |

## Scope and interpretation

The available dataset is one complete UTC day (2025-01-01), with 3,909,986 unique clean events. Intraday comparisons use equal 12-hour windows; they do not establish multi-day trends. Metadata is current and targeted, covering 1.63% of event activity; named primary language covers 0.24%. Accounts include bots. Star events are observed actions, not current cumulative repository stars.

Earlier-week reports, immutable snapshots, metadata cache, and raw archives are retained. Credentials and native database files are Git-ignored in `.runtime/`.

Operations: [Week 5 guide](../docs/week5_database_dashboard.md). Schema: [PostgreSQL contract](../docs/contracts/postgresql.md).
