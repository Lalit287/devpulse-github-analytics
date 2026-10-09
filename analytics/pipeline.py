"""Produce a verified, immutable Gold analytics suite with pinned API metadata."""
import argparse
import json
import shutil
import time
import uuid
from pathlib import Path

from pyspark.sql import functions as F

from config.settings import ROOT
from exploration.io import sha256_file, write_json
from ingestion.locking import directory_lock
from ingestion.state import utc_now
from spark.snapshots import current_manifest, verify_snapshot, switch_current
from enrichment.github import GitHubClient, checksum, enrich
from analytics.artifacts import cleanup_session, code_hashes, finish_snapshot, identity, write_table
from analytics.core import build_core
from analytics.languages import (metadata_frame, primary_language_daily, primary_language_growth,
                                 technology_daily, weighted_language_daily)
from analytics.metrics import EVENT_COLUMNS, METRIC_VERSION
from analytics.session import create_analytics_spark


def run_analytics(*, silver_root=ROOT / "data/silver", core_root=ROOT / "work/week4/core",
                  gold_root=ROOT / "data/gold", cache_dir=ROOT / "data/metadata/github_repositories",
                  max_requests=50, cache_ttl_days=30, max_output_gib=2,
                  enrichment_report=ROOT / "reports/week4/enrichment_latest.json"):
    started = time.monotonic()
    gold_root = Path(gold_root).resolve()
    gold_root.mkdir(parents=True, exist_ok=True)
    with directory_lock(gold_root / ".analytics.lock"):
        core = build_core(silver_root, core_root)
        core_manifest = verify_snapshot(core)
        candidates = json.loads((core / "enrichment_candidates.json").read_text())
        entries, enrichment_run = enrich(candidates, cache_dir=cache_dir,
                                         client=GitHubClient(max_requests=max_requests), ttl_days=cache_ttl_days)
        write_json(enrichment_report, enrichment_run)
        if not any(e["status"] == "ok" for e in entries):
            raise ValueError("No usable public language metadata; preserve current Gold and retry after the API/cache issue is resolved")
        recipe = {"version": METRIC_VERSION, "core_snapshot_id": core_manifest["snapshot_id"],
                  "core_manifest_sha256": sha256_file(core / "manifest.json"),
                  "silver_snapshot_id": core_manifest["recipe"]["silver_snapshot_id"],
                  "window": core_manifest["recipe"]["window"], "metadata_snapshot_sha256": checksum(entries),
                  "code_hashes": code_hashes(), "spark_version": "4.0.1", "language_semantics": "current_metadata_association"}
        identifier = identity(recipe)
        target = gold_root / "snapshots" / identifier
        if target.exists():
            manifest = verify_snapshot(target)
            switch_current(gold_root, identifier)
            return {"status": "reused", "snapshot_id": identifier, "path": str(target),
                    "duration_seconds": round(time.monotonic() - started, 3), "enrichment": enrichment_run,
                    "input_events": manifest["input_events"]}
        stage = gold_root / "snapshots" / f".staging-{identifier}-{uuid.uuid4().hex}"
        stage.mkdir(parents=True)
        session = None
        try:
            for path in core.iterdir():
                if path.is_dir():
                    shutil.copytree(path, stage / path.name)
                elif path.name != "manifest.json":
                    shutil.copy2(path, stage / path.name)
            write_json(stage / "metadata_snapshot.json", entries)
            write_json(stage / "core_manifest.json", core_manifest)
            session = create_analytics_spark()
            silver_path = Path(silver_root).resolve() / "snapshots" / recipe["silver_snapshot_id"]
            events = session.read.parquet((silver_path / "events").as_uri()).select(*EVENT_COLUMNS)
            daily = session.read.parquet((core / "repository_daily").as_uri())
            repos = session.read.parquet((core / "repository_metrics").as_uri())
            metadata = metadata_frame(session, entries)
            end = recipe["window"]["end_utc"]
            metadata = metadata.withColumn("age_days_at_window_end", F.when(
                F.to_timestamp("repository_created_at") <= F.to_timestamp(F.lit(end)),
                (F.unix_seconds(F.to_timestamp(F.lit(end))) - F.unix_seconds(F.to_timestamp("repository_created_at"))) / 86400.0))
            weighted, shares = weighted_language_daily(daily, entries)
            frames = {"repository_metadata": metadata, "repository_language_shares": shares,
                      "language_primary_daily": primary_language_daily(events, metadata),
                      "language_weighted_daily": weighted, "technology_daily": technology_daily(daily, metadata),
                      "language_intraday_growth": primary_language_growth(repos, metadata)}
            measures = dict(core_manifest["tables"])
            checks = dict(core_manifest["checks"])
            for name, frame in frames.items():
                write_table(frame, stage / name, ("event_date",) if "event_date" in frame.columns else ())
                readback = session.read.parquet((stage / name).as_uri())
                measures[name] = {"rows": readback.count()}
                if set(frame.columns) != set(readback.columns):
                    raise ValueError(f"Language table schema mismatch: {name}")
                checks[name] = "passed"
            primary = session.read.parquet((stage / "language_primary_daily").as_uri())
            weighted_read = session.read.parquet((stage / "language_weighted_daily").as_uri())
            if primary.agg(F.sum("total_events")).first()[0] != core_manifest["input_events"]:
                raise ValueError("Primary-language events do not account for all clean events")
            attributed = weighted_read.agg(F.sum("attributed_total_events")).first()[0]
            if abs(attributed - core_manifest["input_events"]) > 0.01:
                raise ValueError("Language byte-share allocation double-counts or loses events")
            if shares.groupBy("repo_id").agg(F.sum("byte_share").alias("s")).filter(F.abs(F.col("s") - 1.0) > 1e-9).count():
                raise ValueError("Per-repository language shares do not sum to one")
            good_ids = [e["repo_id"] for e in entries if e["status"] == "ok"]
            covered = repos.filter(F.col("repo_id").isin(*good_ids)).agg(F.sum("total_events").alias("events"),
                F.sum("star_events").alias("stars"), F.sum("fork_events").alias("forks")).first().asDict()
            coverage = {"selected_repositories": len(entries), "successful_repositories": len(good_ids),
                        "total_repositories": core_manifest["tables"]["repository_metrics"]["rows"],
                        "covered_events": covered["events"], "total_events": core_manifest["input_events"],
                        "covered_event_fraction": covered["events"] / core_manifest["input_events"],
                        "covered_star_events": covered["stars"], "covered_fork_events": covered["forks"],
                        "metadata_status_counts": enrichment_run["status_counts"]}
            write_json(stage / "coverage.json", coverage)
            previews = {}
            for name in ("language_primary_daily", "language_weighted_daily", "technology_daily", "language_intraday_growth"):
                preview = session.read.parquet((stage / name).as_uri())
                if "event_date" in preview.columns:
                    preview = preview.withColumn("event_date", F.col("event_date").cast("string"))
                previews[name] = [row.asDict() for row in preview.collect()]
            write_json(stage / "language_previews.json", previews)
            manifest = {"recipe": recipe, "completed_at_utc": utc_now(), "input_events": core_manifest["input_events"],
                        "tables": measures, "coverage": coverage, "checks": dict(checks, primary_language_row_accounting="passed",
                        byte_weighted_language_accounting="passed", per_repository_language_shares="passed"),
                        "core_duration_seconds": core_manifest["duration_seconds"],
                        "duration_seconds": round(time.monotonic() - started, 3), "enrichment_at_creation": enrichment_run}
            finish_snapshot(stage, gold_root, identifier, manifest, int(max_output_gib * 1024**3))
            return {"status": "created", "snapshot_id": identifier, "path": str(target),
                    "duration_seconds": round(time.monotonic() - started, 3), "enrichment": enrichment_run,
                    "input_events": manifest["input_events"]}
        finally:
            if session is not None:
                cleanup_session(session)
            if stage.exists():
                shutil.rmtree(stage)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--silver-root", type=Path, default=ROOT / "data/silver")
    parser.add_argument("--core-root", type=Path, default=ROOT / "work/week4/core")
    parser.add_argument("--gold-root", type=Path, default=ROOT / "data/gold")
    parser.add_argument("--cache-dir", type=Path, default=ROOT / "data/metadata/github_repositories")
    parser.add_argument("--max-requests", type=int, default=50)
    parser.add_argument("--cache-ttl-days", type=float, default=30)
    parser.add_argument("--max-output-gib", type=float, default=2)
    parser.add_argument("--run-report", type=Path, default=ROOT / "reports/week4/latest_run.json")
    args = parser.parse_args()
    if args.max_output_gib <= 0:
        parser.error("Output quota must be positive")
    result = run_analytics(silver_root=args.silver_root, core_root=args.core_root, gold_root=args.gold_root,
                           cache_dir=args.cache_dir, max_requests=args.max_requests, cache_ttl_days=args.cache_ttl_days,
                           max_output_gib=args.max_output_gib)
    write_json(args.run_report, result)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
