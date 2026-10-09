"""Synthetic edge cases are fixtures only; production runs use verified real archives."""
import gzip
import json

import pytest
from pyspark.sql import functions as F

from exploration.io import sha256_file
from spark.clean_events import check_accounting, run_etl
from spark.session import create_etl_spark
from spark.snapshots import current_manifest, verify_snapshot
from spark.sources import merge_sources, plan_sources, verify_sources
from spark.transform_events import normalize_events, repository_daily_stats, split_and_deduplicate


@pytest.fixture(scope="module")
def etl_spark():
    session = create_etl_spark(master="local[2]", driver_memory="1g", shuffle_partitions=2)
    yield session
    session.stop()


def event(identifier="1", kind="PushEvent", timestamp="2025-01-01T00:00:00Z"):
    return {"id": identifier, "type": kind, "created_at": timestamp, "public": True,
            "actor": {"id": 10, "login": "account"}, "repo": {"id": 20, "name": "owner/repo"},
            "payload": {"action": "started", "commits": [{"sha": "abc", "nested": {"keep": True}}]}}


def frame(session, values):
    return normalize_events(session.createDataFrame(
        [(v if isinstance(v, str) else json.dumps(v), "file:///2025-01-01-0.json.gz") for v in values],
        ["raw_json", "source_uri"]))


def test_validation_and_payload_preservation(etl_spark):
    good = event()
    cases = [good, "{bad", "[]", "{'id':1}"]
    edits = [({"id": "0"}, "invalid_event_id"), ({"created_at": "not-a-date"}, "invalid_timestamp"),
             ({"created_at": "2025-01-01T00:00:00"}, "invalid_timestamp"),
             ({"actor": {"id": 2**70, "login": "a"}}, "invalid_actor_id"),
             ({"actor": {"id": -1, "login": "a"}}, "invalid_actor_id"),
             ({"repo": {"id": 20, "name": "invalid"}}, "invalid_repo_name"),
             ({"public": False}, "not_public"), ({"payload": None}, "missing_payload"),
             ({"actor": {"id": 10, "login": " "}}, "missing_actor_login"),
             ({"public": "true"}, "invalid_json_or_field_type")]
    for update, _ in edits:
        cases.append(dict(good, **update))
    rows = frame(etl_spark, cases).collect()
    assert rows[0].validation_errors == []
    assert json.loads(rows[0].payload_json) == good["payload"]
    assert rows[0].actor_id == 10 and rows[0].repo_id == 20
    assert frame(etl_spark, [good]).select(F.unix_seconds("event_time")).first()[0] == 1735689600
    assert all("invalid_json_or_field_type" in r.validation_errors for r in rows[1:4])
    for row, (_, reason) in zip(rows[4:], edits):
        assert reason in row.validation_errors


def test_offsets_unknown_types_optional_org_and_late_events(etl_spark):
    e = event(kind="FutureEvent", timestamp="2025-01-01T05:30:00.123456789+05:30")
    e["org"] = {"id": "overflow", "login": "org"}
    row = frame(etl_spark, [e]).first()
    assert row.validation_errors == []
    assert frame(etl_spark, [e]).select(F.unix_micros("event_time")).first()[0] == 1735689600123456
    assert set(row.quality_warnings) == {"unknown_event_type", "invalid_optional_org_id"}
    late = frame(etl_spark, [event(timestamp="2024-12-31T23:59:59Z")]).first()
    assert late.validation_errors == [] and "outside_archive_hour" in late.quality_warnings
    assert str(late.event_date) == "2024-12-31" and late.event_hour == 23


def test_deduplication_deterministic_and_validity_first(etl_spark):
    a, b = event(), event()
    b["repo"]["name"] = "owner/renamed"
    invalid = dict(a, public=False)
    rows = [(json.dumps(a), "file:///2025-01-01-1.json.gz"),
            (json.dumps(b), "file:///2025-01-01-0.json.gz"),
            (json.dumps(b), "file:///2025-01-01-0.json.gz"),
            (json.dumps(invalid), "file:///2024-12-31-23.json.gz")]
    for ordering in (rows, list(reversed(rows))):
        normalized = normalize_events(etl_spark.createDataFrame(ordering, ["raw_json", "source_uri"]).repartition(2))
        clean, quarantine, duplicates = split_and_deduplicate(normalized)
        assert clean.first().repo_name == "owner/renamed"
        assert clean.count() == 1 and quarantine.count() == 1 and duplicates.count() == 2


def test_daily_metrics_are_actions_and_push_events(etl_spark):
    values = [event(str(i + 1), kind) for i, kind in enumerate(
        ["PushEvent", "WatchEvent", "WatchEvent", "ForkEvent", "IssuesEvent", "PullRequestEvent"])]
    values[2]["payload"]["action"] = "other"
    values[4]["payload"]["action"] = "opened"
    values[5]["payload"]["action"] = "closed"
    clean, _, _ = split_and_deduplicate(frame(etl_spark, values))
    row = repository_daily_stats(clean).first()
    assert row.total_events == 6 and row.active_accounts == 1
    assert row.push_events == 1 and row.watch_events == 2 and row.star_events == 1
    assert row.issues_opened == 1 and row.pull_requests_opened == 0


@pytest.mark.parametrize("numbers", [(4, 2, 1, 0), (0, 0, 0, 0)])
def test_bad_accounting_refuses_commit(numbers):
    with pytest.raises(ValueError):
        check_accounting(*numbers)


def archive(tmp_path, hour, values):
    path = tmp_path / f"2025-01-01-{hour}.json.gz"
    with gzip.open(path, "wt") as output:
        for value in values:
            output.write((value if isinstance(value, str) else json.dumps(value)) + "\n")
    return {"filename": path.name, "date": "2025-01-01", "hour": hour, "uri": path.as_uri(),
            "local_path": str(path), "sha256": sha256_file(path), "compressed_bytes": path.stat().st_size,
            "records": len(values), "lines": len(values), "backend": "local"}


def test_snapshots_incremental_rerun_and_failure_rollback(etl_spark, tmp_path):
    root = tmp_path / "silver"
    first = archive(tmp_path, 0, [event(), event(), "{bad"])
    run = run_etl([first], output_root=root, session=etl_spark)
    assert run["status"] == "created"
    manifest = current_manifest(root)
    assert (manifest["clean_events"], manifest["duplicate_rows"], manifest["quarantined_rows"]) == (1, 1, 1)
    pointer = (root / "_CURRENT.json").read_bytes()
    old_snapshot = root / "snapshots" / run["snapshot_id"]
    assert run_etl([first], output_root=root, session=etl_spark)["status"] == "reused"
    assert (root / "_CURRENT.json").read_bytes() == pointer
    second = archive(tmp_path, 1, [event("2", timestamp="2025-01-01T01:00:00Z")])
    with pytest.raises(ValueError, match="quota"):
        run_etl([second], output_root=root, session=etl_spark, max_output_gib=1e-12)
    assert (root / "_CURRENT.json").read_bytes() == pointer
    assert not list((root / "snapshots").glob(".staging-*"))
    result = run_etl([second], output_root=root, session=etl_spark)
    assert result["clean_events"] == 2 and result["source_archives"] == 2
    assert verify_snapshot(old_snapshot)["clean_events"] == 1
    assert current_manifest(root)["input_lines"] == 4
    part = next(old_snapshot.rglob("*.parquet"))
    part.write_bytes(part.read_bytes() + b"changed")
    with pytest.raises(ValueError, match="inventory"):
        verify_snapshot(old_snapshot)


def test_source_checksum_and_historical_change_rejected(tmp_path):
    first = archive(tmp_path, 0, [event()])
    verify_sources([first])
    changed = dict(first, sha256="f" * 64)
    with pytest.raises(ValueError, match="Historical archive changed"):
        merge_sources([first], [changed])
    with pytest.raises(ValueError, match="checksum"):
        verify_sources([changed])


def test_source_line_count_mismatch_does_not_publish(etl_spark, tmp_path):
    source = archive(tmp_path, 0, [event()])
    source["lines"] = 2
    with pytest.raises(ValueError, match="counts disagree"):
        run_etl([source], output_root=tmp_path / "silver", session=etl_spark)
    assert not (tmp_path / "silver/_CURRENT.json").exists()


def test_planner_rejects_incomplete_collection(tmp_path):
    path = tmp_path / "collection.json"
    path.write_text(json.dumps({"status": "failed"}))
    with pytest.raises(ValueError, match="complete"):
        plan_sources(path)


def test_dead_jvm_shutdown_keeps_original_failure_and_cleans_stage(tmp_path, monkeypatch):
    import spark.clean_events as module

    class DeadSession:
        version = "4.0.1"
        class conf:
            @staticmethod
            def get(name):
                return "UTC"

        def stop(self):
            raise ConnectionRefusedError("JVM already exited")

    def fail(*args):
        raise ValueError("Original pipeline failure")

    monkeypatch.setattr(module, "create_etl_spark", lambda **kwargs: DeadSession())
    monkeypatch.setattr(module, "build_snapshot", fail)
    source = archive(tmp_path, 0, [event()])
    root = tmp_path / "silver"
    with pytest.warns(RuntimeWarning, match="shutdown failed"):
        with pytest.raises(ValueError, match="Original pipeline failure"):
            run_etl([source], output_root=root)
    assert not (root / "_CURRENT.json").exists()
    assert not list((root / "snapshots").glob(".staging-*"))
