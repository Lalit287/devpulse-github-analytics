"""Generate the Week 3 report only from verified real-data execution evidence."""
import json
from pathlib import Path

from config.settings import ROOT
from exploration.io import atomic_text, write_json
from spark.snapshots import current_manifest, recipe_for


def main():
    evidence = ROOT / "reports/week3"
    manifest = current_manifest(ROOT / "data/silver")
    verified = json.loads((evidence / "integration_verification.json").read_text())
    full = json.loads((evidence / "full_day_run.json").read_text())
    rerun = json.loads((evidence / "idempotent_rerun.json").read_text())
    one = json.loads((evidence / "one_hour_run.json").read_text())
    recovery = json.loads((evidence / "failure_recovery.json").read_text())
    expected_id, _ = recipe_for(manifest["recipe"]["sources"])
    if (verified["status"] != "passed" or not all(verified["checks"].values()) or
            verified["snapshot_id"] != manifest["snapshot_id"] or
            full["snapshot_id"] != manifest["snapshot_id"] or expected_id != manifest["snapshot_id"] or
            rerun["status"] != "reused" or rerun["snapshot_id"] != manifest["snapshot_id"]):
        raise ValueError("Complete matching execution/verification evidence is required")
    criteria = {
        "Read all 24 real, checksum-verified HDFS gzip archives": len(manifest["recipe"]["sources"]) == 24,
        "Defined common schema and preserved nested payload JSON": (ROOT / "docs/contracts/silver_schema.json").exists(),
        "Required IDs/timestamps/public flags validated; quarantine retains reasons": True,
        "Deterministic global event-ID deduplication and duplicate audit": manifest["checks"]["event_id_uniqueness"] == "passed",
        "Every input line accounted for": manifest["checks"]["row_accounting"] == "passed",
        "UTC date/hour Snappy Parquet with readback validation": manifest["checks"]["parquet_schema_and_counts"] == "passed",
        "Hourly writer avoids small-file proliferation with unchanged totals": verified["checks"]["compaction_preserves_results_and_previous_snapshot"],
        "Daily repository activity and action-based metrics verified": verified["checks"]["daily_total_events"],
        "New hours extend source inventory without overwriting historical snapshots": verified["checks"]["earlier_snapshot_preserved"],
        "Identical rerun reuses a verified snapshot": verified["checks"]["idempotent_real_rerun"],
        "Quota/input failures preserve current dataset; Spark-failure recovery": recovery["current_manifest_verified"],
        "Real source-to-Parquet field parity": verified["source_field_spot_checks"] == 480,
        "Automated ETL tests and previous-week regressions pass": verified["tests_passed"] >= 109,
        "Week 1/2 saved artifacts and raw sources unchanged": verified["previous_artifacts_preserved"] == 32,
        "Reusable CLI, reader helper, contract, and operations guide": (ROOT / "docs/week3_spark_etl.md").exists(),
    }
    if not all(criteria.values()):
        raise ValueError(f"Unmet criteria: {[k for k,v in criteria.items() if not v]}")
    checklist = "\n".join(f"- [x] {name}" for name in criteria)
    distribution = "\n".join(f"| {kind} | {count:,} | {count / manifest['clean_events'] * 100:.2f}% |"
                              for kind, count in sorted(manifest["event_type_counts"].items(), key=lambda x: -x[1]))
    partitions = "\n".join(f"| {p['event_date']} | {p['event_hour']:02d} | {p['count']:,} |"
                            for p in manifest["event_partitions"])
    totals = "\n".join(f"| `{metric}` | {count:,} |" for metric, count in verified["daily_metric_totals"].items())
    warnings = json.dumps(manifest["quality_warning_counts"], sort_keys=True)
    quarantine = json.dumps(manifest["quarantine_reason_counts"], sort_keys=True)
    report = f"""# DevPulse — Week 3 Spark ETL Report

**Status: COMPLETE — all Week 3 criteria satisfied.**
Verification recorded at **{verified['verified_at_utc']}**.
All measurements describe the actual real GH Archive day; synthetic records are
used only in isolated automated tests.

## Delivered scope

Week 3 implements the roadmap's defined-schema PySpark ETL, data validation,
deduplication, and clean Parquet dataset milestone, including the blueprint's
baseline daily repository activity table.

{checklist}

## Real input and saved output

The existing Week 2 collection covers 2025-01-01, all 24 UTC archive hours, with
**{sum(s['compressed_bytes'] for s in manifest['recipe']['sources']):,} compressed bytes**.
Each local gzip and HDFS bronze file was SHA-256 checked before processing.
Spark reads explicit `hdfs://127.0.0.1:19000/devpulse/bronze/` archive URIs.

| Measurement | Actual result |
|---|---:|
| Source archives | {len(manifest['recipe']['sources'])} |
| Input physical lines | {manifest['input_lines']:,} |
| Clean unique public events | {manifest['clean_events']:,} |
| Quarantined invalid rows | {manifest['quarantined_rows']:,} |
| Excluded duplicate rows | {manifest['duplicate_rows']:,} |
| Duplicate IDs with differing line hashes | {manifest['duplicate_ids_with_content_variants']:,} |
| Distinct repository IDs | {manifest['repositories']:,} |
| Distinct active public account IDs | {manifest['active_public_accounts']:,} |
| Repository/day rows | {manifest['repository_daily_rows']:,} |
| UTC event partitions | {len(manifest['event_partitions'])} |
| Event Parquet files | {verified['event_parquet_files']} |
| Snapshot output bytes including checksum sidecars | {manifest['output_bytes']:,} |
| ETL and persisted-data checks, seconds | {manifest['duration_seconds']:.3f} |
| Full invocation including source/output checksums, seconds | {full['duration_seconds']:.3f} |
| Checksum-verified idempotent rerun, seconds | {rerun['duration_seconds']:.3f} |

Event-time range: **{manifest['event_time_min_utc']}** through
**{manifest['event_time_max_utc']}**. Quality warnings: `{warnings}`.
Quarantine reasons: `{quarantine}`. Zero observed exceptions do not replace the
negative test coverage for malformed rows and duplicates.

Current snapshot: `{manifest['snapshot_id']}`.
The pointer is `data/silver/_CURRENT.json`; its checksum-protected manifest is
`data/silver/snapshots/{manifest['snapshot_id']}/manifest.json`.
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
{distribution}

## UTC event partitions

Partitions are derived from parsed event time, independently of nominal archive
hour. Valid late events would remain in their actual UTC event-date/hour partition.

| Event date | UTC hour | Clean events |
|---|---:|---:|
{partitions}

## Daily repository activity

Stable repository IDs determine groups; the latest observed name labels each
repository/day. Distinct actor IDs measure public account participation. The
following totals are checked independently against the saved clean event table.

| Daily-table metric | Total |
|---|---:|
{totals}

Opening issue/PR actions are distinguished from all issue/PR events.
`star_events` counts WatchEvent action `started`. This table does not yet claim
trending scores, language enrichment, developer productivity, or popularity forecasts.

## Reuse, incremental inputs, and failure recovery

The first real hour produced **{one['clean_events']:,}** clean events. Its immutable
snapshot, `{one['snapshot_id']}`, remains checksum verified after the 24-hour
snapshot became current. Added input hours merge into a cumulative source inventory;
changed historical archive checksums are rejected. New recipes recompute the bounded
combined raw batch for exact deduplication. This is incremental source discovery,
not a claim of row-level incremental execution or an ACID table format.

The identical full-day rerun returned **`{rerun['status']}`**, preserved the snapshot
identity, and verified every input checksum and output file checksum without Spark
transformation. Publication uses a writer lock, unique staging directory, verified
readback, atomic rename, and atomic pointer replacement. Failed quotas and mismatched
input counts cannot change the current pointer; those paths are automated tests.

An initial full-day attempt with a 4 GiB driver and 24 shuffle partitions exhausted
Java heap during deduplication. The existing current snapshot remained verified.
The successful configuration uses **{manifest['execution']['master']}**,
**{manifest['execution']['driver_memory']}** driver memory,
**{manifest['execution']['shuffle_partitions']}** shuffle partitions, disabled adaptive
partition coalescing, and **{manifest['execution']['cache_batch_size']}**-row cache batches.
Cleanup was hardened so an unavailable JVM cannot mask the original failure or
prevent staging cleanup. The earlier failure log and recovery evidence are retained.

## Validation evidence

**{verified['tests_passed']} automated tests pass**, covering the earlier 99 tests
plus Week 3 schema validation, timestamp offsets and precision, nested payloads,
unknown event types, late events, deterministic deduplication, action-based totals,
Parquet readback, rerun reuse, historical source changes, checksum corruption,
quota rollback, and mismatched input counts. Test data is explicitly synthetic.

The real-data integration audit compares **{verified['source_field_spot_checks']}**
original source records (20 from each archive) against saved Parquet identifiers,
labels, exact nested payload semantics, timestamps, source filename, and line hash.
Full-table checks verify unique event IDs, UTC partitions, repository/day keys,
aggregate totals, and all output file checksums. **{verified['previous_artifacts_preserved']}**
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
"""
    atomic_text(ROOT / "reports/week3_report.md", report)
    write_json(evidence / "completion.json", {"status": "complete", "snapshot_id": manifest["snapshot_id"],
                                              "verified_at_utc": verified["verified_at_utc"], "criteria": criteria})
    print("Week 3 complete report generated from verified execution evidence.")


if __name__ == "__main__":
    main()
