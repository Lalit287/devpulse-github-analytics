# Week 7 — completed: native Airflow and Kafka streaming

Verified at 2026-10-08T21:09:37.385668+00:00. All 200 tests passed, with no failures or skips.

## Delivered and measured

- Isolated Airflow 3.3.2 with PostgreSQL metadata, authenticated loopback UI, scheduler, DAG processor and LocalExecutor.
- A genuine daily scheduled DAG plus a manually retried run: collection → Spark Silver → Gold → native PostgreSQL → parity checks. All final tasks succeeded.
- Native Kafka 4.2.2 KRaft broker, controlled acknowledged replay, Spark Kafka Structured Streaming and persistent checkpoints.
- Real source: January 1, 2025, 12:00–13:00 UTC; 223,540 unique events, 60 exact minute windows and 170,126 exact repository-minute windows.
- Pipeline monitor with historical minute charts, recent repositories/accounts, spikes, duplicate/late metrics, offsets and Airflow run history. Queries use the read-only database role.

## Recovery verification

- Independent recovery stream: 224,540 messages = 223,540 unique events + 1,000 logical duplicates.
- 1 late unique event(s) corrected historical windows; 10,352 out-of-order unique events retained.
- Real broker restart preserved partition offsets.
- An injected crash after SQL commit and before Spark checkpoint commit recovered without double counting.
- Replacement checkpoint replay preserved all event/message/duplicate/late counters.
- Continuous microbatch mode, invalid JSON, out-of-hour timestamps, conflicting IDs and transaction rollback passed.

## Tests

| Group | Passed | Failures/errors/skips |
|---|---:|---:|
| unit tests | 24 | 0/0/0 |
| integration tests | 8 | 0/0/0 |
| regression tests | 168 | 0/0/0 |

## Preservation and operating state

119 original source/evidence files verified unchanged. Original Silver, Gold and model pointers are preserved, and the original PostgreSQL dashboard dataset is restored.
Both demonstration DAGs are paused after measured verification; the daily UTC schedule remains configured. Kafka and Airflow serve only localhost. This is a single-machine development deployment, and the replay is historical.

## Evidence and usage

- [Native verification](week7/verification.json)
- [Airflow task/run proof](week7/airflow_verification.json)
- [Recovery drills](week7/recovery_verification.json)
- [Broker offset recovery](week7/broker_recovery.json)
- [Fresh test/code receipt](week7/test_receipt.json)
- [Operations and semantics guide](../docs/week7_orchestration_streaming.md)

Week 8 remains: performance optimization/benchmarking, packaging/deployment and final handover.
