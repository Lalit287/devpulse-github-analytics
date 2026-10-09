# Week 3 — Spark ETL

Run commands from the Desktop `DevPulse` root with `source scripts/activate.sh`.
The production entry point is `python -m spark.clean_events`. No new download is
needed: the default input is the complete Week 2 collection manifest.

## Run and inspect

```bash
python -m storage.hdfs_cluster status
# If stopped, start the existing isolated development cluster:
python -m storage.hdfs_cluster start
python -m spark.clean_events
python -m pytest tests/test_week3_etl.py -q --junitxml=reports/week3/etl_tests.xml
python -m pytest tests/test_download.py tests/test_exploration.py tests/test_week2_ingestion.py tests/test_week2_storage.py -q --junitxml=reports/week3/previous_week_regression.xml
python -m scripts.verify_week3
python -m scripts.generate_week3_report
```

The verification script is for the measured full day and its saved one-hour/rerun
evidence. The ETL supports other completed Week 2 manifests through `--manifest`.
`--hours` selects those UTC hours on each day in that manifest. Use a separate output
root for an independent experiment. `--backend local --output-root work/local-silver`
reads verified local gzip files; the completed main run uses real HDFS bronze files.

The original measured sequence started from an empty Silver root, selected hour 0,
then extended to all hours and performed an identical rerun. The saved
`one_hour_run.json`, `full_day_run.json`, and `idempotent_rerun.json` document that
sequence. Ongoing runs write `latest_run.json` by default. Once the full day is
current, selecting hour 0 still retains the other historical hours. Use a new
`--output-root work/my-etl-run --run-report work/my-etl-run.json` for an independent
reproduction with an initially empty source inventory.

The default Spark engine is `local[4]`, a 6 GiB driver, 192 shuffle partitions,
adaptive execution, UTC timestamps, Snappy, and Parquet microsecond timestamps.
Shuffle coalescing is disabled and columnar cache batches are limited to 1,000 rows
to keep variable nested payloads from creating oversized tasks on this machine.
It uses Spark's distributed SQL execution model on one Mac. This run is not evidence
of multi-machine performance. `--shuffle-partitions` controls shuffle concurrency.
Explicit URIs prevent accidental reads/writes through an inherited default filesystem.

## Read the current dataset

The `_CURRENT.json` pointer names a snapshot and checksums its manifest. Resolve it
through the helper so incomplete or corrupted output is rejected:

```python
from config.settings import ROOT
from spark.session import create_etl_spark
from spark.snapshots import current_manifest

root = ROOT / "data/silver"
manifest = current_manifest(root)
snapshot = root / "snapshots" / manifest["snapshot_id"]
spark = create_etl_spark()
try:
    events = spark.read.parquet((snapshot / "events").as_uri())
    daily = spark.read.parquet((snapshot / "repository_daily_stats").as_uri())
    events.filter("event_date = '2025-01-01' AND event_hour = 12").show()
finally:
    spark.stop()
```

Full events live in `events/event_date=YYYY-MM-DD/event_hour=H/`; daily repository
totals are partitioned by event date. Read from the `events` root to discover partition
columns. `quarantine` and `duplicates` retain source URI, event ID, raw line, SHA-256,
and either validation reasons or duplicate rank. The manifest stores exact counts,
schema, source checksums, event types, warnings, file hashes, sizes, and checks.
The event writer groups rows by event-date/hour before writing, with a 250,000-row
file cap. Repository/day output uses eight writer buckets. Shuffle task count is
independent of output file count; this avoids thousands of tiny Parquet files.

## Incremental inputs and safe publication

New inputs merge with the current snapshot's source inventory. A changed historical
archive checksum is rejected. An unchanged recipe reuses a verified snapshot without
starting Spark. Adding hours builds a new cumulative snapshot from the combined raw
source set, allowing exact global deduplication. This is incremental source discovery
with bounded batch recomputation; it does not claim row-level incremental execution.
Older snapshots and their historical partitions are never overwritten.

Publication uses a unique staging directory, persisted-data checks, file hashes,
an atomic directory rename, and an atomic pointer replacement. Exceptions clean up
that run's staging directory. A crash may leave an unreferenced staging directory;
it cannot become current. A complete snapshot created just before a crash can be
verified and reused on the next run. One nonblocking file lock serializes writers
to each output root. Do not edit immutable raw files or snapshot files manually.

The 12 GiB default quota covers all Silver snapshots and staging output, checked
before promotion. Disk-backed Spark cache/shuffle uses `.runtime/spark-week3` and
is additional scratch space. The preflight free-space check is an estimate, not a
hard scratch quota. Completed snapshots remain available until explicitly retired.
Raw inputs are SHA-256 verified locally and remotely on every production invocation,
including reruns. Source counts must match the Week 2 line ledger.

## Data contract and validation

See [the contract](contracts/silver_events.md) and the exported
[Spark schema](contracts/silver_schema.json). Common fields use an explicit schema;
the variable nested payload is retained as JSON text. No Python UDF is used.

Each source line is accounted for exactly once as a clean event, quarantined row,
or excluded duplicate. Only valid rows enter deduplication. The winner is the earliest
archive start, then filename, then raw-record SHA-256; identical rows tied on all keys
have identical clean values. Clean event IDs are globally unique within a snapshot.

Unknown well-formed event types are retained with a warning. Events outside their
archive hour are retained and partitioned by actual UTC event time, supporting late
events. Optional organization IDs that cannot be converted are null with a warning.
Malformed structures can still quarantine the row through the defined JSON schema.
Invalid required fields are quarantined with all detected reasons. JSON parser
extensions for single quotes, comments, unquoted names, and NaN are disabled; Week 2
already checks complete-file gzip integrity and JSON-object integrity.

Repository daily counts use stable repository IDs and the latest observed name;
distinct actor IDs measure public account participation. Push records are push
events. `star_events` counts WatchEvent action `started`; it is not current star
inventory. Opening actions are separate from all issue/PR activity. These are
observed activity totals, not a trend score, commit count, or productivity rating.

The full pipeline checks Parquet schema, exact counts and uniqueness, event-type
parity, UTC partitions, and daily repository count parity before switching current.
Separate integration verification compares 480 source records with saved Parquet,
checks action-based daily metrics, verifies snapshot checksums and the real rerun,
and confirms Week 1/2 artifacts were preserved.

Reference: [Spark JSON data source](https://spark.apache.org/docs/4.0.1/sql-data-sources-json.html)
and [Spark Parquet data source](https://spark.apache.org/docs/4.0.1/sql-data-sources-parquet.html).
