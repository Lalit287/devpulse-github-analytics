# DevPulse — final project report

Status: completed and verified. Generated 2026-10-09T17:44:09.186425+00:00.

## Purpose and implementation

DevPulse processes historical public GitHub activity through validated ingestion, HDFS, Spark ETL, immutable Parquet analytics and atomic PostgreSQL publication. A six-page dashboard serves repository, language, public-account and pipeline views. Airflow schedules bounded work; Kafka and Structured Streaming maintain idempotent minute analytics with durable late corrections.

## Actual datasets

- Activity: all 24 archive hours on January 1, 2025; 3,909,993 source records, 3,909,986 clean unique events and seven duplicates.
- Prediction: 42 complete UTC days in 2015, 1,008 archives; 19,669,354 unique eligible events after 173 exact duplicate rows.
- Streaming: a complete real 223,540-event historical hour; independent recovery proof added 1,000 duplicates without increasing unique counts.

## Historical model evaluation

Selected by validation PR-AUC: **gradient boosted trees**. Test population: 152,513 examples, 1,305 positives (0.856%).

| Algorithm | Test PR-AUC | Test ROC-AUC | Precision@10 | Test F1 |
|---|---:|---:|---:|---:|
| logistic_regression | 0.128024 | 0.921689 | 0.100 | 0.222679 |
| random_forest | 0.138672 | 0.886635 | 0.200 | 0.223659 |
| gradient_boosted_trees | 0.147934 | 0.924828 | 0.400 | 0.230123 |
| Recent-star ranking baseline | 0.120290 | 0.910645 | 0.100 | 0.072798 |

Frozen validation threshold: 0.1. Test confusion counts: TP 589, FP 3,225, FN 716, TN 147,983. Uncalibrated Brier score: 0.008722. Historical forecast rows: 159,462; actual outcomes for the forecast week are not in the corpus.

The chronology prevents future labels/features from influencing the held-out selection. Scores are uncalibrated probabilities, not current GitHub recommendations. Low target prevalence makes PR-AUC and precision at selected ranks more informative than accuracy alone.

## Performance, deployment and verification

[The Week 8 performance report](week8_report.md) records exact workload parity, per-process memory scope, worker trials, storage query results, streaming-rate latency and current verification status. [Architecture](../docs/architecture.md), [dataset catalog](../docs/data_catalog.md) and [demo runbook](../docs/demo_runbook.md) document the delivered system.

## Limitations and interpretation

The 2025 activity day and 2015 modeling period are historical and selected windows. Public events omit private activity, include bots and do not measure complete developer productivity. Star actions are observed WatchEvents, not cumulative historical stars. Current language metadata has limited coverage and is associated with past activity rather than historically observed. Single-host worker results do not demonstrate multi-machine scalability. Local service configuration does not provide production high availability or public-network security.

## Evidence and handover

Weekly reports retain measured development evidence. Source/manifest hashes, snapshot contracts, atomic publication checks and native recovery receipts support reproducibility. Large archives, models, credentials and runtime storage remain inside the private Desktop project; the source distribution excludes those. The source Git repository includes offline CI configuration. Repository: [devpulse-github-analytics](https://github.com/Lalit287/devpulse-github-analytics). Repository publication does not deploy the local services or upload their private data.

Future extensions include longer current activity windows, calibrated forecasting, broader metadata coverage and resource-governed multi-host deployment. These are extensions rather than measured results of this project.
