"""Run verified GH Archive inputs through the Week 3 Silver ETL."""
import argparse
import json
import shutil
import time
import uuid
import warnings
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import functions as F

from config.settings import ROOT
from exploration.io import write_json
from ingestion.locking import directory_lock
from ingestion.state import utc_now
from spark.schema import CLEAN_COLUMNS
from spark.session import create_etl_spark
from spark.snapshots import current_manifest, inventory, recipe_for, switch_current, verify_snapshot
from spark.sources import merge_sources, plan_sources, verify_sources
from spark.transform_events import normalize_events, repository_daily_stats, split_and_deduplicate


def counts(frame, column):
    return {str(row[column]): row["count"] for row in frame.groupBy(column).count().collect()}


def array_counts(frame, column):
    return counts(frame.select(F.explode(column).alias("reason")), "reason")


def check_accounting(total, clean, quarantine, duplicates):
    if total != clean + quarantine + duplicates:
        raise ValueError("Every source line must be clean, quarantined, or a duplicate")
    if clean == 0:
        raise ValueError("No valid unique events; refusing to publish an empty production snapshot")


def build_snapshot(session, sources, stage, snapshot_id, recipe):
    started = time.monotonic()
    cached = []
    try:
        lines = session.read.text([s["uri"] for s in sources]).select(
            F.col("value").alias("raw_json"), F.input_file_name().alias("source_uri"))
        normalized = normalize_events(lines).persist(StorageLevel.DISK_ONLY)
        cached.append(normalized)
        source_counts = counts(normalized, "source_filename")
        expected = {s["filename"]: s["lines"] for s in sources}
        if source_counts != expected:
            raise ValueError(f"Spark input counts disagree with ingestion: {source_counts} vs {expected}")
        clean, quarantine, duplicates = split_and_deduplicate(normalized)
        clean = clean.persist(StorageLevel.DISK_ONLY)
        cached.append(clean)
        clean_count, quarantine_count, duplicate_count = clean.count(), quarantine.count(), duplicates.count()
        total = sum(source_counts.values())
        check_accounting(total, clean_count, quarantine_count, duplicate_count)
        event_types = counts(clean, "event_type")
        warning_counts = array_counts(clean, "quality_warnings")
        rejection_counts = array_counts(quarantine, "validation_errors")
        duplicate_variants = (normalized.filter(F.size("validation_errors") == 0)
                              .groupBy("event_id").agg(F.count("*").alias("n"),
                                                        F.countDistinct("raw_record_sha256").alias("variants"))
                              .filter("n > 1 and variants > 1").count())
        clean.repartition("event_date", "event_hour").write.mode("errorifexists").option(
            "maxRecordsPerFile", 250000).option("compression", "snappy").partitionBy(
            "event_date", "event_hour").parquet((stage / "events").as_uri())
        quarantine.select("raw_json", "source_uri", "source_filename", "raw_record_sha256",
                          "event_id", "validation_errors").coalesce(4).write.mode("errorifexists").option(
                              "compression", "snappy").parquet((stage / "quarantine").as_uri())
        duplicates.select("raw_json", "source_uri", "source_filename", "raw_record_sha256",
                          "event_id", "duplicate_rank").coalesce(4).write.mode("errorifexists").option(
                              "compression", "snappy").parquet((stage / "duplicates").as_uri())
        daily = repository_daily_stats(clean)
        daily.repartition(8, "event_date", "repo_id").write.mode("errorifexists").option(
            "maxRecordsPerFile", 250000).option("compression", "snappy").partitionBy(
            "event_date").parquet((stage / "repository_daily_stats").as_uri())
        # Validate the persisted datasets, rather than only the pre-write frames.
        readback = session.read.parquet((stage / "events").as_uri())
        if set(readback.columns) != set(CLEAN_COLUMNS):
            raise ValueError("Parquet columns differ from the Silver contract")
        original_types = {f.name: f.dataType for f in clean.schema}
        if {f.name: f.dataType for f in readback.schema} != original_types:
            raise ValueError("Parquet data types differ from the Silver contract")
        checked = readback.agg(F.count("*").alias("rows"), F.countDistinct("event_id").alias("ids"),
                               F.date_format(F.min("event_time"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'").alias("first"),
                               F.date_format(F.max("event_time"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'").alias("last"),
                               F.countDistinct("repo_id").alias("repos"),
                               F.countDistinct("actor_id").alias("accounts")).first()
        if checked["rows"] != clean_count or checked["ids"] != clean_count:
            raise ValueError("Parquet roundtrip count or event uniqueness failed")
        if counts(readback, "event_type") != event_types:
            raise ValueError("Event-type totals changed in Parquet roundtrip")
        if readback.filter((F.to_date("event_time") != F.col("event_date")) |
                           (F.hour("event_time") != F.col("event_hour"))).limit(1).count():
            raise ValueError("UTC event partitions disagree with timestamps")
        for name, count in (("quarantine", quarantine_count), ("duplicates", duplicate_count)):
            if session.read.parquet((stage / name).as_uri()).count() != count:
                raise ValueError(f"{name} Parquet count mismatch")
        daily_read = session.read.parquet((stage / "repository_daily_stats").as_uri())
        daily_summary = daily_read.agg(F.count("*").alias("rows"), F.sum("total_events").alias("events")).first()
        if daily_summary["events"] != clean_count:
            raise ValueError("Daily repository counts do not sum to clean events")
        partitions = [row.asDict() for row in readback.groupBy("event_date", "event_hour").count()
                      .orderBy("event_date", "event_hour").collect()]
        for row in partitions:
            row["event_date"] = str(row["event_date"])
        manifest = {"status": "complete", "snapshot_id": snapshot_id, "recipe": recipe,
                    "execution": {"master": session.sparkContext.master,
                                  "driver_memory": session.sparkContext.getConf().get("spark.driver.memory"),
                                  "shuffle_partitions": session.conf.get("spark.sql.shuffle.partitions"),
                                  "adaptive_coalescing": session.conf.get("spark.sql.adaptive.coalescePartitions.enabled"),
                                  "cache_batch_size": session.conf.get("spark.sql.inMemoryColumnarStorage.batchSize")},
                    "completed_at_utc": utc_now(), "duration_seconds": round(time.monotonic() - started, 3),
                    "input_lines": total, "clean_events": clean_count, "quarantined_rows": quarantine_count,
                    "duplicate_rows": duplicate_count, "duplicate_ids_with_content_variants": duplicate_variants,
                    "repository_daily_rows": daily_summary["rows"], "repositories": checked["repos"],
                    "active_public_accounts": checked["accounts"],
                    "event_time_min_utc": checked["first"],
                    "event_time_max_utc": checked["last"],
                    "source_line_counts": source_counts, "event_type_counts": event_types,
                    "quality_warning_counts": warning_counts, "quarantine_reason_counts": rejection_counts,
                    "event_partitions": partitions, "schema": clean.schema.jsonValue(),
                    "checks": {"source_counts": "passed", "row_accounting": "passed",
                               "parquet_schema_and_counts": "passed", "event_id_uniqueness": "passed",
                               "utc_partitions": "passed", "repository_total_parity": "passed"}}
        manifest["files"] = inventory(stage)
        manifest["output_bytes"] = sum(f["bytes"] for f in manifest["files"])
        return manifest
    finally:
        for frame in cached:
            try:
                frame.unpersist(blocking=True)
            except Exception as exc:
                warnings.warn(f"Spark cache cleanup failed: {type(exc).__name__}", RuntimeWarning)


def run_etl(sources, *, output_root=ROOT / "data/silver", session=None, max_output_gib=12,
            verify_inputs=True, shuffle_partitions=192):
    root = Path(output_root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    with directory_lock(root / ".etl.lock"):
        previous = current_manifest(root)
        sources = merge_sources(previous["recipe"]["sources"] if previous else [], sources)
        if verify_inputs:
            verify_sources(sources)
        snapshot_id, recipe = recipe_for(sources)
        target = root / "snapshots" / snapshot_id
        if target.exists():
            manifest = verify_snapshot(target)
            switch_current(root, snapshot_id)
            return {"status": "reused", "snapshot_id": snapshot_id,
                    "duration_seconds": round(time.monotonic() - started, 3),
                    "manifest": str(target / "manifest.json"), "clean_events": manifest["clean_events"],
                    "source_archives": len(sources), "inputs_checksum_verified": verify_inputs}
        needed = max(1024**3, 6 * sum(s["compressed_bytes"] for s in sources))
        if shutil.disk_usage(root).free < needed:
            raise ValueError(f"Insufficient free disk space; need at least {needed} bytes for ETL scratch")
        stage = root / "snapshots" / f".staging-{snapshot_id}-{uuid.uuid4().hex}"
        stage.mkdir(parents=True)
        owned_session = session is None
        try:
            if session is None:
                session = create_etl_spark(shuffle_partitions=shuffle_partitions)
            if session.version != recipe["spark_version"]:
                raise ValueError(f"Expected Spark {recipe['spark_version']}, got {session.version}")
            if session.conf.get("spark.sql.session.timeZone") != "UTC":
                raise ValueError("ETL requires a UTC Spark session")
            manifest = build_snapshot(session, sources, stage, snapshot_id, recipe)
            existing_bytes = sum(p.stat().st_size for p in root.rglob("*") if p.is_file())
            if existing_bytes > max_output_gib * 1024**3:
                raise ValueError("Silver snapshot quota exceeded; current dataset remains unchanged")
            write_json(stage / "manifest.json", manifest)
            verify_snapshot(stage)
            stage.rename(target)
            switch_current(root, snapshot_id)
            return {"status": "created", "snapshot_id": snapshot_id,
                    "duration_seconds": round(time.monotonic() - started, 3),
                    "manifest": str(target / "manifest.json"), "clean_events": manifest["clean_events"],
                    "source_archives": len(sources), "inputs_checksum_verified": verify_inputs}
        finally:
            try:
                if owned_session and session is not None:
                    try:
                        session.stop()
                    except Exception as exc:
                        warnings.warn(f"Spark shutdown failed: {type(exc).__name__}", RuntimeWarning)
            finally:
                if stage.exists():
                    shutil.rmtree(stage)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=ROOT / "data/metadata/latest_collection.json")
    parser.add_argument("--backend", choices=["hdfs", "local"], default="hdfs")
    parser.add_argument("--hours", type=int, nargs="+", help="Selected UTC hours; merge into the current source set")
    parser.add_argument("--output-root", type=Path, default=ROOT / "data/silver")
    parser.add_argument("--max-output-gib", type=float, default=12)
    parser.add_argument("--shuffle-partitions", type=int, default=192)
    parser.add_argument("--run-report", type=Path, default=ROOT / "reports/week3/latest_run.json")
    args = parser.parse_args()
    if args.max_output_gib <= 0 or args.shuffle_partitions <= 0:
        parser.error("Quota and partition count must be positive")
    result = run_etl(plan_sources(args.manifest, backend=args.backend, hours=args.hours),
                     output_root=args.output_root, max_output_gib=args.max_output_gib,
                     shuffle_partitions=args.shuffle_partitions)
    write_json(args.run_report, result)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
