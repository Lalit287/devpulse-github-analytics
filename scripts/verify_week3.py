"""Audit real Silver outputs, source-field parity, and previous-week preservation."""
import gzip
import hashlib
import json
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

from config.settings import ROOT
from exploration.io import sha256_file, write_json
from ingestion.state import utc_now
from spark.session import create_etl_spark
from spark.snapshots import current_manifest, verify_snapshot


def verify_week3():
    checks = {}

    def check(name, value):
        checks[name] = bool(value)
        if not value:
            raise ValueError(f"Week 3 verification failed: {name}")

    root = ROOT / "data/silver"
    manifest = current_manifest(root)
    check("complete_snapshot", manifest is not None and manifest["status"] == "complete")
    sources = manifest["recipe"]["sources"]
    check("real_full_day_hdfs_inputs", len(sources) == 24 and
          {s["date"] for s in sources} == {"2025-01-01"} and
          {s["hour"] for s in sources} == set(range(24)) and
          all(s["backend"] == "hdfs" and s["uri"].startswith("hdfs://127.0.0.1:19000/") for s in sources))
    check("row_accounting", manifest["input_lines"] == sum(s["lines"] for s in sources) ==
          manifest["clean_events"] + manifest["quarantined_rows"] + manifest["duplicate_rows"])
    check("pipeline_checks", all(value == "passed" for value in manifest["checks"].values()))
    samples = {}
    for source in sources:
        path = Path(source["local_path"])
        check(f"raw_checksum:{path.name}", sha256_file(path) == source["sha256"])
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for _ in range(20):
                line = handle.readline().rstrip("\r\n")
                record = json.loads(line)
                identifier = str(record["id"])
                if identifier in samples:
                    raise ValueError("Spot-check sample repeats event IDs; choose a unique sample")
                samples[identifier] = (record, source, hashlib.sha256(line.encode()).hexdigest())
    session = create_etl_spark(master="local[2]", driver_memory="2g", shuffle_partitions=4)
    snapshot = root / "snapshots" / manifest["snapshot_id"]
    try:
        events = session.read.parquet((snapshot / "events").as_uri())
        actual = events.filter(F.col("event_id").isin(*samples)).withColumn(
            "event_epoch_micros", F.unix_micros("event_time")).collect()
        check("spot_check_event_coverage", len(actual) == len(samples) == 480)
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        for row in actual:
            record, source, digest = samples[row.event_id]
            delta = datetime.fromisoformat(record["created_at"].replace("Z", "+00:00")) - epoch
            micros = (delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds
            check(f"source_fields:{row.event_id}", row.event_type == record["type"] and
                  row.actor_id == int(record["actor"]["id"]) and row.actor_login == record["actor"]["login"] and
                  row.repo_id == int(record["repo"]["id"]) and row.repo_name == record["repo"]["name"] and
                  row.event_epoch_micros == micros and row.public is True and
                  json.loads(row.payload_json) == record["payload"] and
                  row.source_filename == source["filename"] and row.raw_record_sha256 == digest)
        check("readback_rows_and_ids", events.count() == events.select("event_id").distinct().count() == manifest["clean_events"])
        daily = session.read.parquet((snapshot / "repository_daily_stats").as_uri())
        check("daily_repository_key_unique", daily.count() == daily.select("event_date", "repo_id").distinct().count()
              == manifest["repository_daily_rows"])
        totals = daily.agg(*[F.sum(name).alias(name) for name in
                           ("total_events", "push_events", "watch_events", "star_events", "fork_events",
                            "pull_request_events", "pull_requests_opened", "issue_events", "issues_opened")]).first().asDict()
        for column, kind in (("push_events", "PushEvent"), ("watch_events", "WatchEvent"),
                             ("fork_events", "ForkEvent"), ("pull_request_events", "PullRequestEvent"),
                             ("issue_events", "IssuesEvent")):
            check(f"daily_metric:{column}", totals[column] == manifest["event_type_counts"].get(kind, 0))
        for column, kind, action in (("star_events", "WatchEvent", "started"),
                                     ("pull_requests_opened", "PullRequestEvent", "opened"),
                                     ("issues_opened", "IssuesEvent", "opened")):
            check(f"daily_action_metric:{column}", totals[column] == events.filter(
                (F.col("event_type") == kind) & (F.col("payload_action") == action)).count())
        check("daily_total_events", totals["total_events"] == manifest["clean_events"])
        write_json(ROOT / "docs/contracts/silver_schema.json", manifest["schema"])
    finally:
        session.stop()
    one_hour = json.loads((ROOT / "reports/week3/one_hour_run.json").read_text())
    old = verify_snapshot(root / "snapshots" / one_hour["snapshot_id"])
    check("earlier_snapshot_preserved", len(old["recipe"]["sources"]) == 1 and old["clean_events"] == 142347)
    pre_compaction = json.loads((ROOT / "reports/week3/pre_compaction_evidence.json").read_text())
    previous_full = verify_snapshot(root / "snapshots" / pre_compaction["snapshot_id"])
    event_files = sum(f["path"].startswith("events/") and f["path"].endswith(".parquet") for f in manifest["files"])
    check("compaction_preserves_results_and_previous_snapshot",
          previous_full["clean_events"] == manifest["clean_events"] and
          previous_full["event_type_counts"] == manifest["event_type_counts"] and
          previous_full["duplicate_rows"] == manifest["duplicate_rows"] and event_files == 24)
    rerun = json.loads((ROOT / "reports/week3/idempotent_rerun.json").read_text())
    check("idempotent_real_rerun", rerun["status"] == "reused" and
          rerun["snapshot_id"] == manifest["snapshot_id"] and rerun["inputs_checksum_verified"])
    tests = 0
    for name in ("etl_tests.xml", "previous_week_regression.xml"):
        tree = ET.parse(ROOT / "reports/week3" / name).getroot()
        cases = tree.findall(".//testcase")
        check(f"tests_passed:{name}", bool(cases) and not any(tree.findall(f".//{tag}") for tag in
                                                           ("failure", "error", "skipped")))
        tests += len(cases)
    baseline = json.loads((ROOT / "reports/week3/previous_weeks_preservation.json").read_text())
    for relative, digest in baseline.items():
        check(f"preserved:{relative}", sha256_file(ROOT / relative) == digest)
    result = {"status": "passed", "verified_at_utc": utc_now(), "snapshot_id": manifest["snapshot_id"],
              "checks": checks, "tests_passed": tests, "source_field_spot_checks": len(samples),
              "daily_metric_totals": totals, "previous_artifacts_preserved": len(baseline)}
    result["event_parquet_files"] = event_files
    write_json(ROOT / "reports/week3/integration_verification.json", result)
    print(json.dumps({key: value for key, value in result.items() if key != "checks"}, indent=2))


if __name__ == "__main__":
    verify_week3()
