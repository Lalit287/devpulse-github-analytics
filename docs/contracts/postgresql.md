# PostgreSQL serving contract — Week 5

Database: `devpulse` on the project cluster at `127.0.0.1:15432`.
Owner: `devpulse_owner`; loader: `devpulse_writer`; dashboard: `devpulse_reader`.
The reader has only CONNECT, schema USAGE and table/view SELECT privileges,
with a 15-second statement timeout and UTC sessions. Its default transaction
mode is read-only; actual table grants separately prohibit writes.

## Logical schema

The `devpulse` schema exposes all 15 Gold tables with identical field names
and primary-key identity semantics from [the Gold contract](gold_analytics.md).
Immutable physical tables reside in `dp_<24-hex-load-id>` schemas; SELECT
grants permit snapshot-pinned dashboard queries. Activation changes logical
views and `current_dataset` atomically.

| Blueprint name | Serving view |
|---|---|
| repositories | repository_metrics joined with selected repository_metadata by repo_id |
| repository_daily_stats | repository_daily |
| developer_daily_stats | account_daily |
| language_daily_stats | language_primary_daily |
| repository_predictions | reserved typed Week 6 output table |
| pipeline_runs | loader run ledger |

The blueprint's illustrative column names are adapted to the already validated
Gold contract (`event_date`, `active_accounts`, and explicit whole-window scores).
Raw JSON events are never inserted into this database.

## Types and keys

Arrow int64 → BIGINT, int32 → INTEGER, string → TEXT, float64 → DOUBLE PRECISION,
boolean → BOOLEAN, date32 → DATE, timestamp → TIMESTAMPTZ in UTC,
list of strings → TEXT[]. Hive `event_date` partitions are explicitly date32.
Primary-key columns are NOT NULL; positive repository/account IDs, nonnegative
event counts and bounded share/score/participation fields are constrained.
Unsupported Arrow types fail before activation. No ID is converted through float.

Primary keys are defined in `database/contracts.py`: stable repository/account ID
alone for whole-snapshot metrics; ID/date/hour for histories; date/category/status
for language/technology tables; repository/account IDs for sampled edges.
Name/login text is a display label, not an identity key.

## Control schema

- `schema_migrations`: control DDL version and SHA-256, checked on every load.
- `dataset_versions`: immutable completed source/schema identity, row counts,
  dataset facts, and measured per-table COPY audit totals/sample-check counts.
- `current_dataset`: exactly one singleton row pointing to the active version.
- `pipeline_runs`: UUID, Gold/load IDs, timestamps, status, copied rows, duration,
  verification flag and failure class. Controlled failure drills are marked.
- `repository_predictions`: model/repository/as-of key, positive forecast horizon,
  probability constrained to [0,1], model evaluation JSON and creation timestamp.
  Empty in Week 5. It requires an evaluated chronological model in Week 6.

Version identity hashes the Gold manifest, control schema version and loader/
contract/DDL hashes. Required tables, counts, sums of metric columns and five
full-field rows per table are validated before activation; verification recomputes
source totals independently. Report completion additionally requires native
integration tests, unchanged earlier-week files, and a healthy dashboard.

Ranked/search queries use parameter binding, allowlisted identifiers and sort
columns, escaped literal ILIKE patterns and limits of at most 200. Public account
and repository histories are bounded by the available dataset. Snapshot cache
keys include load identity; cache TTL is 60 seconds (pointer TTL 15 seconds).
