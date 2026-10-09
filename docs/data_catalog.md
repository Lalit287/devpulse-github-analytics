# Dataset and artifact catalog

All large datasets remain in the Desktop `DevPulse` folder. Source packages exclude
large data, credentials, database files and runtime environments.

| Dataset | Actual scope | Location |
|---|---|---|
| Activity archives | 24 hourly files, January 1, 2025; 3,909,993 raw records | `data/raw/` |
| HDFS Bronze | Checksum-verified copies of the same 24 files | `/devpulse/bronze/date=2025-01-01/hour=HH/` in the project HDFS cluster |
| Activity Silver | 3,909,986 unique events, seven duplicate rows removed, zero quarantined | `data/silver/snapshots/37f73c2302379bc640363b8a/` |
| Activity Gold | 15 verified tables; 683,676 repositories and 419,709 public accounts | `data/gold/snapshots/6e03839285499224b0403539/` |
| Warehouse | PostgreSQL `devpulse`, logical views in `devpulse` | Project port 15432; `.runtime/postgres/` holds physical storage |
| Prediction archives | 42 full UTC days in 2015, 1,008 hours, 19,669,527 raw records | `data/prediction_history/raw/` |
| Prediction projected data | 19,669,354 unique eligible events; separate from the 2025 activity day | `data/prediction_history/projected/` and corpus manifest |
| Evaluated models | Logistic regression, random forest and gradient boosting; frozen chronological evaluation | `data/models/snapshots/7b1216c89ede73b2b6ac1b25/` |
| Historical forecasts | 159,462 repository rows for the seven days following February 12, 2015 | Current model artifact and PostgreSQL `devpulse_ml` |
| Kafka replay source | 223,540 real Silver events, January 1, 2025, UTC event hour 12 | Verified Silver Parquet; Kafka namespace `devpulse.*` |
| Streaming analytics | Minimal typed deduplication/offset ledger and minute aggregates | PostgreSQL `devpulse_stream` |
| Airflow batch profiles | Isolated source-date raw/Bronze/Silver/core/Gold outputs | `data/orchestrated/YYYY-MM-DD/` |
| Week 8 benchmark projections | Equivalent five-column real-day storage formats | `work/week8/storage/` |
| Distributed verification | Separate full-day Silver and core snapshots | `work/week8/distributed/` |

The current-pointer JSON files identify the active immutable snapshots; do not
rename their snapshot directories. The prediction cutoff is historical, and actual
future labels for the forecast week are not included. Public account activity is
not a complete measure of developer productivity. Star actions are observed
WatchEvents, not cumulative historical repository star counts.

Keep manifests, contracts and final reports: they explain provenance, counts,
limitations and recovery. Weekly reports are useful audit evidence. Temporary logs
and regenerable benchmark scratch data can be archived after verification; avoid
removing current data, model artifacts, checkpoints or database credentials while
using the project. Source distribution does not include those private artifacts.
