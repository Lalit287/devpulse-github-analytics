"""Compute and readback-verify repository and public-account analytics."""
import json
import shutil
import time
import uuid
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import functions as F

from config.settings import ROOT
from exploration.io import sha256_file, write_json
from ingestion.locking import directory_lock
from ingestion.state import utc_now
from spark.snapshots import current_manifest, verify_snapshot, switch_current
from analytics.artifacts import cleanup_session, finish_snapshot, identity, write_table
from analytics.metrics import (EVENT_COLUMNS, METRIC_VERSION, accounts, calendar, enrichment_candidates,
                               hourly_activity, participation_sample, repositories, repository_daily,
                               repository_hourly, window_spec)
from analytics.session import create_analytics_spark


def build_core(silver_root=ROOT / "data/silver", core_root=ROOT / "work/week4/core", session=None):
    silver_root, core_root = Path(silver_root).resolve(), Path(core_root).resolve()
    silver = current_manifest(silver_root)
    if silver is None:
        raise ValueError("A complete Silver snapshot is required")
    silver_path = silver_root / "snapshots" / silver["snapshot_id"]
    spec = window_spec(silver["recipe"]["sources"])
    recipe = {"version": METRIC_VERSION, "silver_snapshot_id": silver["snapshot_id"],
              "silver_manifest_sha256": sha256_file(silver_path / "manifest.json"), "window": spec,
              "code_hashes": {name: sha256_file(Path(__file__).parent / name)
                              for name in ("metrics.py", "session.py", "artifacts.py", "core.py")},
              "selection_size": 20, "spark_version": "4.0.1"}
    identifier = identity(recipe)
    core_root.mkdir(parents=True, exist_ok=True)
    with directory_lock(core_root / ".core.lock"):
        target = core_root / "snapshots" / identifier
        if target.exists():
            verify_snapshot(target)
            switch_current(core_root, identifier)
            return target
        if shutil.disk_usage(core_root).free < 2 * 1024**3:
            raise ValueError("Need at least 2 GiB free for analytics cache and output")
        stage = core_root / "snapshots" / f".staging-{identifier}-{uuid.uuid4().hex}"
        stage.mkdir(parents=True)
        own_session = session is None
        cached = []
        started = time.monotonic()
        try:
            session = session or create_analytics_spark()
            if session.version != "4.0.1" or session.conf.get("spark.sql.session.timeZone") != "UTC":
                raise ValueError("Analytics requires Spark 4.0.1 in UTC")
            events = session.read.parquet((silver_path / "events").as_uri()).select(*EVENT_COLUMNS).persist(StorageLevel.DISK_ONLY)
            cached.append(events)
            if events.count() != silver["clean_events"]:
                raise ValueError("Silver count mismatch")
            repo, maxima = repositories(events, spec)
            repo = repo.persist(StorageLevel.DISK_ONLY)
            cached.append(repo)
            frames = {"repository_metrics": repo, "repository_daily": repository_daily(events),
                      "repository_hourly": repository_hourly(events), "account_metrics": accounts(events),
                      "account_daily": accounts(events, ("actor_id", "event_date")),
                      "account_hourly": accounts(events, ("actor_id", "event_date", "event_hour")),
                      "activity_calendar": calendar(events), "hourly_activity": hourly_activity(events, spec),
                      "participation_edges_sample": participation_sample(events, repo)}
            table_metrics = {}
            checks = {}
            for name, frame in frames.items():
                partition = ("event_date",) if "event_date" in frame.columns else ()
                write_table(frame, stage / name, partition)
                readback = session.read.parquet((stage / name).as_uri())
                keys = {"repository_metrics": ["repo_id"], "repository_daily": ["repo_id", "event_date"],
                        "repository_hourly": ["repo_id", "event_date", "event_hour"], "account_metrics": ["actor_id"],
                        "account_daily": ["actor_id", "event_date"], "account_hourly": ["actor_id", "event_date", "event_hour"],
                        "activity_calendar": ["event_date"], "hourly_activity": ["event_date", "event_hour"],
                        "participation_edges_sample": ["repo_id", "actor_id"]}[name]
                rows = readback.count()
                if rows != readback.select(*keys).distinct().count():
                    raise ValueError(f"Duplicate analytics key: {name}")
                if set(readback.columns) != set(frame.columns):
                    raise ValueError(f"Parquet schema column mismatch: {name}")
                summary = {"rows": rows}
                if "total_events" in readback.columns:
                    total = readback.agg(F.sum("total_events")).first()[0]
                    summary["events"] = total
                    if name != "hourly_activity" and total != silver["clean_events"]:
                        raise ValueError(f"Event totals differ from Silver: {name}")
                table_metrics[name] = summary
                checks[name] = "passed"
            if table_metrics["repository_metrics"]["rows"] != silver["repositories"] or table_metrics["account_metrics"]["rows"] != silver["active_public_accounts"]:
                raise ValueError("Repository/account count differs from Silver")
            if table_metrics["hourly_activity"]["rows"] != spec["archive_hours"]:
                raise ValueError("Missing dense hourly coverage")
            bounds = repo.agg(F.min("activity_score"), F.max("activity_score"), F.min("trend_score"), F.max("trend_score")).first()
            if any(value is None or not 0 <= value <= 1.00000001 for value in bounds):
                raise ValueError("Normalized ranking score outside [0,1]")
            in_window = repo.agg(F.sum(F.col("previous_events") + F.col("current_events"))).first()[0]
            if in_window != table_metrics["hourly_activity"]["events"]:
                raise ValueError("Equal-period totals differ from covered hourly activity")
            candidates = enrichment_candidates(repo)
            write_json(stage / "enrichment_candidates.json", candidates)
            rankings = {}
            for name, frame in (("most_active", repo.orderBy(F.col("total_events").desc(), "repo_id")),
                                ("most_starred", repo.orderBy(F.col("star_events").desc(), "repo_id")),
                                ("intraday_trending", repo.filter("growth_eligible").orderBy(F.col("trend_score").desc(), "repo_id")),
                                ("fastest_growing_attention", repo.filter("growth_eligible and attention_delta > 0").orderBy(F.col("attention_growth_ratio").desc(), F.col("current_attention").desc(), "repo_id")),
                                ("emerging_attention", repo.filter("emerging_attention_candidate").orderBy(F.col("trend_score").desc(), "repo_id"))):
                columns = ["repo_id", "repo_name", "total_events", "active_accounts", "code_participating_accounts", "star_events", "fork_events",
                           "push_events", "pull_requests_opened", "issues_opened", "previous_attention", "current_attention",
                           "attention_delta", "attention_growth_ratio", "attention_change_percent", "zero_attention_baseline", "activity_score", "trend_score"]
                rankings[name] = [row.asDict() for row in frame.select(*columns).limit(50).collect()]
            write_json(stage / "rankings.json", rankings)
            write_json(stage / "top_accounts.json", [row.asDict() for row in frames["account_metrics"].orderBy(
                F.col("total_events").desc(), "actor_id").limit(50).collect()])
            manifest = {"recipe": recipe, "completed_at_utc": utc_now(), "tables": table_metrics,
                        "input_events": silver["clean_events"], "trend_covered_events": in_window,
                        "trend_excluded_outside_window": silver["clean_events"] - in_window,
                        "normalization_maxima": maxima, "checks": checks,
                        "duration_seconds": round(time.monotonic() - started, 3)}
            finish_snapshot(stage, core_root, identifier, manifest)
            return target
        finally:
            for frame in cached:
                try:
                    frame.unpersist(blocking=True)
                except Exception:
                    pass
            if own_session and session is not None:
                cleanup_session(session)
            if stage.exists():
                shutil.rmtree(stage)


if __name__ == "__main__":
    path = build_core()
    write_json(ROOT / "reports/week4/core_run.json", {"status": "complete", "path": str(path)})
    print(path)
