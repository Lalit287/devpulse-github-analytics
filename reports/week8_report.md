# Week 8 — completed

Final native deployment and every required test passed.

## Measured results

Full-day input: 3,909,986 clean events. Every configuration produced identical counts for 683,676 repositories. Three trials per configuration, warm filesystem caches; Spark startup measured separately.

| Configuration | Median query seconds | Min–max seconds |
|---|---:|---:|
| 1-workers | 4.573 | 4.508–4.613 |
| 2-workers | 4.580 | 4.563–4.614 |
| local4 | 3.648 | 3.576–4.020 |
| local4-shuffle200 | 4.103 | 4.059–4.129 |
| pandas | 0.443 | 0.443–0.444 |

The 32-partition query profile reduced median time by 11.1% versus 200 shuffle partitions. Pandas was faster on this in-memory narrow projection; adding a second worker did not provide meaningful speedup. Workers share one Mac. No multi-machine scaling claim is made.

Memory values are per-process maximum RSS for Python and the largest reaped child JVM, plus recorded executor heap metrics. They are not a concurrent aggregate cluster-memory measurement.

| Equivalent projected format | Size MiB | Median query seconds | Candidate files |
|---|---:|---:|---:|
| json | 398.14 | 0.1988 | 1 |
| gzip_json | 49.84 | 0.3213 | 1 |
| flat | 36.51 | 0.0030 | 16 |
| partitioned | 36.29 | 0.0021 | 1 |

All formats contain the same five logical fields from the real day. The query selects UTC hour 12 and returns 223,540 events. Parquet column/row-group filtering and hourly partition pruning both contribute. These sizes are not a full-payload archive compression comparison.

Distributed pipeline: 3,909,986 clean events, 7 duplicate rows removed, 0 quarantined; both standalone executor JVMs processed tasks. All nine core table readback checks passed.
The raw ETL uses 3 GiB executors with one concurrent task each and a 1,000-row cache batch. An earlier 1.5 GiB / two-task profile ran out of heap; its failed output was not published. The successful rerun preserved the original active dataset.

| Replay rate requested | Median end-to-end events/s | Median p95 SQL commit delay, s | Trials |
|---|---:|---:|---:|
| 1,000 | 858 | 3.588 | 2 |
| 5,000 | 1,242 | 17.132 | 2 |
| 10,000 | 1,760 | 10.155 | 2 |

Each rate trial uses the same 20,000 real time-sorted events, concurrent producer/consumer execution, 2-second microbatches and acknowledged delivery. Wall-clock delay runs from Kafka CreateTime to SQL transaction commit; historical event time is preserved separately. Spark startup is excluded consistently.

## Final verification

231 tests passed with no failures, errors or skips. All six native deployment components are healthy. Original current dataset/model pointers and 152 source/evidence files are unchanged.

[Full verification](week8/verification.json) · [Fresh test receipt](week8/test_receipt.json) · [Deployment state](week8/deployment_status.json)

[Architecture](../docs/architecture.md) · [Dataset catalog](../docs/data_catalog.md) · [Demo and recovery runbook](../docs/demo_runbook.md)

Deployment scope: native, loopback-only, single-host development demonstration. Docker and cloud hosting are not executed. The local Spark cluster follows the blueprint’s alternative to Docker Compose. Source repository: [devpulse-github-analytics](https://github.com/Lalit287/devpulse-github-analytics). Large datasets and private service state are excluded from source distribution.
