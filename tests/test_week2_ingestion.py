import gzip
import json
import math
from datetime import datetime, timezone

import pytest

from exploration.io import sha256_file
from ingestion import collect
from ingestion.audit import audit_collection
from ingestion.download_gharchive import download_archive
from ingestion.hour_range import delayed_completed_hour, plan_hours
from ingestion.locking import directory_lock
from ingestion.state import StateStore
from ingestion.validate_downloads import validate_archive
from storage.backends import LocalBronze
from tests.test_download import response_for
from unittest.mock import Mock


def test_week2_ranges():
    assert len(plan_hours("2025-01-01", hours=24)) == 24
    assert len(plan_hours("2025-01-01", end_date="2025-01-07")) == 168
    assert len(plan_hours("2025-01-01", hours=744)) == 744
    hours = plan_hours("2024-12-31", hour=23, hours=2)
    assert hours[1]["date"] == "2025-01-01" and hours[1]["hour"] == 0


@pytest.mark.parametrize("kwargs", [{"hours": 0}, {"hours": -1}, {"hours": 745}, {"hours": True},
                                  {"hour": 24}, {"end_date": "2024-12-31"}, {"end_date": "20250102"}])
def test_invalid_week2_windows(kwargs):
    with pytest.raises(ValueError):
        plan_hours("2025-01-01", **kwargs)


def test_utc_scheduler_buffer():
    now = datetime(2025, 1, 2, 3, 59, tzinfo=timezone.utc)
    assert delayed_completed_hour(now, 6) == datetime(2025, 1, 1, 20, tzinfo=timezone.utc)
    with pytest.raises(ValueError):
        delayed_completed_hour(now.replace(tzinfo=None))
    with pytest.raises(ValueError):
        delayed_completed_hour(now, -1)


def test_raw_writer_lock(tmp_path):
    lock = tmp_path / "lock"
    with directory_lock(lock):
        with pytest.raises(RuntimeError, match="Another DevPulse"):
            with directory_lock(lock):
                pass
    with directory_lock(lock):
        pass


def test_tampered_valid_gzip_is_redownloaded(tmp_path):
    session = Mock()
    original = gzip.compress(b'{"id":"original"}\n')
    session.get.return_value = response_for(original)
    first = download_archive("2025-01-01", 12, output_dir=tmp_path, session=session)
    (tmp_path / first["filename"]).write_bytes(gzip.compress(b'{"id":"tampered"}\n'))
    session.get.return_value = response_for(original)
    second = download_archive("2025-01-01", 12, output_dir=tmp_path, session=session)
    assert not second["reused"] and second["sha256"] == first["sha256"]
    assert session.get.call_count == 2


def test_missing_checksum_cannot_preserve_verified_download_provenance(tmp_path):
    filename = "2025-01-01-12.json.gz"
    (tmp_path / filename).write_bytes(gzip.compress(b'{}\n'))
    (tmp_path / "download_manifest.json").write_text(json.dumps({"files": {filename: {"provenance": "HTTPS download from GH Archive"}}}))
    session = Mock()
    result = download_archive("2025-01-01", 12, output_dir=tmp_path, session=session)
    assert result["reused"]
    assert result["provenance"].startswith("existing local file")
    session.get.assert_not_called()


@pytest.mark.parametrize("value", [0, -1, math.nan, math.inf])
def test_invalid_budget(value):
    with pytest.raises(ValueError):
        collect.budget_bytes(value)


def test_full_file_quality_scan(archive):
    result = validate_archive(archive)
    assert result["record_count"] == 4
    assert result["event_type_counts"]["WatchEvent"] == 2
    assert result["missing_fields"]["actor.id"] == 1
    assert result["invalid_timestamps"] == 2
    assert result["sha256"] == sha256_file(archive)


@pytest.mark.parametrize("body", [b"", b"[]\n", b"{}\nnot-json\n", b"{}\n\n"])
def test_invalid_archives_not_accepted(tmp_path, body):
    archive = tmp_path / "bad.json.gz"
    archive.write_bytes(gzip.compress(body))
    with pytest.raises(ValueError):
        validate_archive(archive)


def test_truncated_validation(archive):
    archive.write_bytes(archive.read_bytes()[:-8])
    with pytest.raises((OSError, EOFError)):
        validate_archive(archive)


def test_state_survives_new_connection(tmp_path):
    path = tmp_path / "state.sqlite3"
    store = StateStore(path)
    store.save_archive("fixture.json.gz", "validated", {"validation": {"record_count": 4}})
    other = StateStore(path)
    assert other.archive("fixture.json.gz")["validation"]["record_count"] == 4
    assert other.archive("unknown") is None
    with pytest.raises(ValueError):
        other.save_archive("fixture", "unrecognized", {})


@pytest.fixture
def fake_downloader(monkeypatch, events):
    calls = {"network": 0, "fail_hour": None}

    def download(day, hour, *, output_dir, force_download=False, **kwargs):
        if calls["fail_hour"] == hour:
            raise OSError("synthetic temporary download failure")
        filename = f"{day}-{hour}.json.gz"
        path = output_dir / filename
        reused = path.exists() and not force_download
        if not reused:
            calls["network"] += 1
            path.write_bytes(gzip.compress(b"".join(json.dumps(item).encode() + b"\n" for item in events)))
        entry = {"filename": filename, "sha256": sha256_file(path), "compressed_bytes": path.stat().st_size,
                 "reused": reused, "provenance": "HTTPS download from GH Archive"}
        manifest_path = output_dir / "download_manifest.json"
        manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {"files": {}}
        manifest["files"][filename] = entry
        manifest_path.write_text(json.dumps(manifest))
        return entry

    monkeypatch.setattr(collect, "download_archive", download)
    return calls


def run_fixture(tmp_path, **kwargs):
    return collect.run_collection(plan_hours("2025-01-01", hours=2), raw_dir=tmp_path / "raw",
                                  metadata_dir=tmp_path / "metadata",
                                  backend=LocalBronze(tmp_path / "bronze", 1024**2), **kwargs)


def test_collection_resume_reuses_download_validation_and_storage(tmp_path, fake_downloader):
    first = run_fixture(tmp_path)
    assert first["status"] == "complete" and first["validated_records"] == 8
    second = run_fixture(tmp_path)
    assert second["status"] == "complete"
    assert second["reused_downloads"] == second["reused_validations"] == second["reused_publications"] == 2
    assert fake_downloader["network"] == 2


def test_failed_hour_does_not_erase_successful_hours(tmp_path, fake_downloader):
    fake_downloader["fail_hour"] = 0
    first = run_fixture(tmp_path)
    assert first["status"] == "failed" and first["completed_hours"] == 1
    assert first["missing_hours"] == ["2025-01-01-0.json.gz"]
    fake_downloader["fail_hour"] = None
    resumed = run_fixture(tmp_path)
    assert resumed["status"] == "complete" and resumed["reused_downloads"] == 1


def test_storage_failure_preserves_validated_raw_data(tmp_path, fake_downloader, monkeypatch):
    original = LocalBronze.publish
    monkeypatch.setattr(LocalBronze, "publish", lambda *a, **k: (_ for _ in ()).throw(OSError("storage offline")))
    failed = run_fixture(tmp_path)
    assert failed["status"] == "failed"
    store = StateStore(tmp_path / "metadata/ingestion.sqlite3")
    assert store.archive("2025-01-01-0.json.gz")["status"] == "validated"
    monkeypatch.setattr(LocalBronze, "publish", original)
    resumed = run_fixture(tmp_path)
    assert resumed["status"] == "complete" and resumed["reused_validations"] == 2
    assert fake_downloader["network"] == 2


def test_fail_fast_records_remaining_hours_as_not_processed(tmp_path, fake_downloader):
    fake_downloader["fail_hour"] = 0
    result = run_fixture(tmp_path, fail_fast=True)
    assert result["results"][1]["status"] == "not_processed"
    assert len(result["missing_hours"]) == 2


def test_keyboard_interrupt_is_recorded_for_recovery(tmp_path, monkeypatch):
    def interrupt(*args, **kwargs):
        raise KeyboardInterrupt()
    monkeypatch.setattr(collect, "download_archive", interrupt)
    with pytest.raises(KeyboardInterrupt):
        run_fixture(tmp_path)
    result = json.loads((tmp_path / "metadata/latest_collection.json").read_text())
    assert result["status"] == "interrupted"


def test_storage_preflight_failure_downloads_nothing(tmp_path, fake_downloader, monkeypatch):
    monkeypatch.setattr(LocalBronze, "prepare", lambda *a: (_ for _ in ()).throw(OSError("offline")))
    result = run_fixture(tmp_path)
    assert result["status"] == "failed" and result["completed_hours"] == 0
    assert fake_downloader["network"] == 0


def test_dry_plan_does_not_create_files(tmp_path):
    path = tmp_path / "not_created"
    plan = collect.collection_plan(plan_hours("2025-01-01", hours=2), path, 4096, "local")
    assert plan["new_files"] == 2 and not path.exists()


def test_coverage_audit_finds_raw_tampering(tmp_path, fake_downloader):
    run_fixture(tmp_path)
    backend = LocalBronze(tmp_path / "bronze", 1024**2)
    arguments = {"raw_dir": tmp_path / "raw", "metadata_dir": tmp_path / "metadata",
                 "backend": backend, "verify_storage": True}
    manifest = tmp_path / "metadata/latest_collection.json"
    assert audit_collection(manifest, **arguments)["status"] == "passed"
    (tmp_path / "raw/2025-01-01-0.json.gz").write_bytes(gzip.compress(b'{}\n'))
    result = audit_collection(manifest, **arguments)
    assert result["status"] == "failed"
    assert any("checksum" in issue["reason"] for issue in result["issues"])


def test_coverage_audit_marks_missing_hour(tmp_path, fake_downloader):
    fake_downloader["fail_hour"] = 0
    run_fixture(tmp_path)
    result = audit_collection(tmp_path / "metadata/latest_collection.json", raw_dir=tmp_path / "raw",
                              metadata_dir=tmp_path / "metadata")
    assert result["status"] == "failed" and result["expected_hours"] == 2
    assert result["verified_hours"] == 1


def test_corrupt_validation_sidecar_is_not_reported_as_verified(tmp_path, fake_downloader):
    run_fixture(tmp_path)
    (tmp_path / "metadata/validation/2025-01-01-0.json.gz.json").write_text('{}')
    result = audit_collection(tmp_path / "metadata/latest_collection.json", raw_dir=tmp_path / "raw",
                              metadata_dir=tmp_path / "metadata")
    assert result["status"] == "failed"
    assert any("sidecar" in issue["reason"] for issue in result["issues"])


def test_zero_hours_cli_is_rejected(monkeypatch):
    monkeypatch.setattr('sys.argv', ['collect', '--date', '2025-01-01', '--hours', '0', '--dry-run'])
    with pytest.raises(SystemExit) as error:
        collect.main()
    assert error.value.code == 1
