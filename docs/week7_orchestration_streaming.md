# Week 7 — Airflow orchestration and Kafka streaming

The project runs native Airflow 3.3.2, Kafka 4.2.2 in KRaft mode, Spark 4.0.1
Structured Streaming, and PostgreSQL. This is a single-machine development
installation. It does not claim production high availability or current GitHub
activity. Week 8 still covers optimization, packaging, deployment and handover.

## Locations and services

| Component | Project-owned files | Local endpoint |
|---|---|---|
| Airflow Python environment | `.venv-airflow/` | Separate from the working Spark environment |
| Airflow configuration, credentials and task logs | `.runtime/airflow/` | `http://127.0.0.1:18080` |
| Airflow metadata | PostgreSQL database `devpulse_airflow` | `127.0.0.1:15432` |
| Kafka binaries and matching Spark connector JARs | `.tools/` | Java 17 |
| Main Kafka KRaft data and logs | `.runtime/kafka/` | Broker `127.0.0.1:19092`, controller `19093` |
| Kafka restart verification | `.runtime/kafka-recovery-PORT/` | One free reserved pair between 19192/19193 and 19992/19993 |
| Spark checkpoints | `.runtime/streaming/STREAM/CHECKPOINT/` | Persistent offsets and commit logs |
| Scheduled batch profiles | `data/orchestrated/YYYY-MM-DD/` | Raw, Bronze, Silver, core, Gold and private metadata |
| Streaming ledger and aggregates | SQL schema `devpulse_stream` | Read-only dashboard queries |
| Measured evidence | `reports/week7/`, `reports/week7_report.md` | Kept separately from earlier reports |
| Dashboard | `dashboard/pages/pipeline.py` | `http://127.0.0.1:18501/pipeline` |

Runtime credentials are generated inside private project directories (0700), with
secret files 0600. Airflow uses its own PostgreSQL login and database. The dashboard
uses the existing reader login: SELECT on stream tables and only the four Airflow
status tables (`dag_run`, `task_instance`, `job`, `dag`). It cannot change either
pipeline. Airflow's SimpleAuthManager is appropriate only for this local development
installation. Its username is `devpulse`; obtain the generated password locally from
`.runtime/airflow/passwords.json`. Keep this file out of source control.

## Reproduction

From a normal Terminal in `~/Desktop/DevPulse`:

```sh
.venv/bin/python -m scripts.setup_week7
.venv/bin/python -m streaming.broker start
.venv/bin/python -m orchestration.runtime start
.venv/bin/python -m scripts.complete_week7 --verify-existing
```

The last command verifies existing measured Airflow/fault-drill results, restores
the original validated dashboard dataset, runs all tests, checks real-hour parity
and preserves previous evidence. Without `--verify-existing`, it also executes new
batch/replay DAG runs and a fresh isolated recovery drill. Runtime installation is
separate so the completion helper never silently installs packages. Airflow uses the
saved official Python 3.12 constraints; both environments have frozen lock files.
The installer verifies the official Kafka SHA-512 and Maven connector checksums.

On a fresh installation, complete Weeks 1–6 first, including the PostgreSQL setup.
If a service cannot be signalled by the desktop app's process sandbox, manage it in
the normal Terminal where it was started. The recovery verifier starts and stops its
own child broker in one invocation; it does not need to stop the main broker.

## Airflow workflows

`devpulse_daily_batch` has the chain:

```text
collect 24 completed UTC hours → validated Spark Silver → 15-table Gold
→ transactional PostgreSQL publication → source/SQL parity check
```

Its schedule is 08:00 UTC (`0 8 * * *`), catchup is disabled, and it admits one active
run with one task at a time. A one-slot `devpulse_heavy` pool protects the local
machine from concurrent Spark stages. Tasks retry twice, with a 10-second delay and
30-minute execution timeout. Failed collection/transformation tasks block publication;
the existing loader commits validation and activation atomically.

The default `source_date` is the reproducible historical day `2025-01-01`, not today's
data. Setting it to null uses the UTC day preceding the scheduled interval end.
Only completed, strictly validated `YYYY-MM-DD` dates are accepted. Paths and quotas
are bounded; raw/Bronze collection budgets are 4 GiB each. Existing verified raw files
are hardlinked into the isolated profile, and replacement is atomic. Each profile
has its own manifest, SQLite status ledger and enrichment cache. Spark verifies source
hashes and publishes immutable snapshots. `metadata_requests=0` allows cache-only
execution; an explicit value of 1–50 enables bounded public API enrichment. Missing
usable language metadata fails Gold publication instead of pretending coverage.

The verification executed a genuine scheduler-created daily run and a manually
triggered run with a deliberate one-time collection failure. All final tasks succeeded;
the manual collection task retried successfully. Development failures and their logs
are retained. Both demo DAGs are paused after verification so opening the project
does not start an unexpected daily download. Unpause the daily DAG in Airflow when
ongoing scheduling is desired and review the date/request parameters first.

`devpulse_historical_replay` runs on demand: producer → Kafka Spark consumer → full
source parity. It uses the same heavy pool and retry controls. Producer reruns may
emit logical duplicates; the sink excludes them reliably. A recurring replay can be
scheduled by changing this DAG's schedule deliberately.

The batch verifier temporarily published its isolated Gold snapshot to demonstrate
actual SQL activation. Completion restores the original Gold database publication;
original raw files, Silver/Gold/model pointers, source manifests and earlier measured
reports are verified unchanged. No earlier report is used as fresh Week 7 evidence.

## Real historical stream and analytics

The source is all **223,540 clean Silver events in the UTC event hour 12:00–13:00 on
January 1, 2025**. It selects event time, not the raw archive file's hour. The producer
preserves event IDs, entity IDs, action and aware UTC timestamp, keys by repository,
and paces delivery at the configured rate (default 10,000 events/s; accepted range
1–50,000). Every record must receive a successful broker acknowledgement. Kafka
idempotence protects transport retries; durable event-ID deduplication protects
producer reruns and consumer/checkpoint recovery.

Spark uses the matching Scala 2.13 / Spark 4.0.1 Kafka connector. It reads up to 10,000
offsets per trigger, computes UTC minute boundaries, and passes bounded batches to
a SQL transaction. `availableNow` drains retained data for Airflow; `--continuous`
runs 5-second microbatches until interrupted. A checkpoint resumes saved offsets;
`startingOffsets=earliest` applies only to a new checkpoint. `failOnDataLoss=true`
fails rather than silently skipping lost retained offsets. Checkpoint contracts pin
the source, topic, broker and sink version.

The monitor refreshes every 5 seconds and displays minute total/star/fork counts,
exact per-minute active accounts, active repositories and recent accounts over the
latest ten **event-time** minutes. Activity spikes compare that window with the
preceding ten minutes, using `(recent+1)/(previous+1)`, requiring at least 10 recent
events and a ratio of 2 or more. These are historical activity spikes, not predictions.

## Commit, deduplication and late-event contract

1. Each Kafka message has a durable `(stream, topic, partition, offset)` identity.
   Re-reading an offset does not change counters; differing content at an already
   stored offset fails with a possible topic-recreation/data-loss error.
2. Unique events use `(stream, event_id)` and a canonical common-field fingerprint.
   Repeated identical IDs become duplicates. Conflicting content becomes a conflict;
   it cannot replace the original event. Invalid JSON, private/unsupported events,
   nonpositive or imprecise IDs, naive times, foreign snapshots and events outside
   the registered hour are recorded as invalid message dispositions.
3. New unique events, message dispositions, affected minute/repository aggregates,
   counters and batch receipts commit in **one PostgreSQL transaction**. The batch
   digest includes its exact offsets and payload hashes, so a retried batch recognizes
   an existing receipt. Message/event keys also protect against different batch
   boundaries after replacement of the checkpoint.
4. Watermark = maximum previously committed event time minus 10 minutes. A unique
   event older than the previous maximum is out of order; one older than the previous
   watermark is late. Both are retained. A late event recomputes its original minute,
   including exact actor counts. Duplicates never inflate late correction counts.
5. This durable correction policy lives in the SQL `foreachBatch` sink. It does not
   claim that Spark's built-in stateful watermark dropping supplies late corrections,
   or that Kafka and PostgreSQL share a distributed transaction. Idempotent SQL
   commits bridge the crash window before Spark saves its checkpoint commit.

Raw nested JSON stays in archival/HDFS/Parquet storage. PostgreSQL contains only the
minimal typed event/offset ledger needed for exact deduplication, participation and
window repair, plus aggregate tables. The local demo retains that ledger. Production
retention/compaction, partitioning, TLS/SASL, multi-node replication and capacity
policies need explicit design in deployment work. Kafka retains 24 hours, with bounded
segments and a 512 MiB partition cap; checkpoints cannot recover offsets already
removed by retention.

## Measured recovery proof

The isolated verifier replays the same complete real hour in phases, withholds an
early event and a recent event, adds exactly 1,000 duplicates, and restarts its real
KRaft broker. It checks unchanged partition offsets. It then deliberately crashes
Spark after a committed SQL microbatch and restarts the same checkpoint. It delivers
the withheld records after the watermark, runs continuous mode, and verifies every
minute and repository-minute against an independent offline source calculation.

A replacement checkpoint re-reads all 224,540 retained Kafka messages and leaves all
message/event/duplicate/late counters unchanged. Separate synthetic contract records
exercise invalid JSON, an out-of-hour timestamp and a conflicting event ID; these
never enter the real-hour proof. Native database tests also force a pre-commit SQL
failure and verify rollback of all ledgers and aggregates. Completion requires passing
tests without skips, fresh source/code checksums, actual Airflow task successes and
full real-hour parity.

Official references: [Airflow prerequisites](https://airflow.apache.org/docs/apache-airflow/3.3.2/installation/prerequisites.html),
[Airflow DAGs](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html),
[Kafka 4.2 quickstart](https://kafka.apache.org/42/getting-started/quickstart/),
[Kafka KRaft](https://kafka.apache.org/42/operations/kraft/), and
[Spark 4.0.1 Kafka integration](https://spark.apache.org/docs/4.0.1/streaming/structured-streaming-kafka-integration.html).
