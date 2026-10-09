import gzip
from datetime import date
from unittest.mock import Mock

import pytest
import requests

from ingestion.download_gharchive import archive_filename, build_hours, download_archive, validate_gzip


def response_for(body, *, status=200, known_length=True):
    response = Mock()
    response.__enter__ = Mock(return_value=response)
    response.__exit__ = Mock(return_value=False)
    response.headers = {"Content-Length": str(len(body))} if known_length else {}
    response.status_code = status
    response.iter_content = Mock(return_value=iter([body[:10], b"", body[10:]]))
    if status >= 400:
        response.raise_for_status.side_effect = requests.HTTPError(f"HTTP {status}", response=response)
    return response


def test_filename_without_zero_padded_hour():
    assert archive_filename("2025-01-01", 3) == "2025-01-01-3.json.gz"
    assert archive_filename(date(2024, 2, 29), 23) == "2024-02-29-23.json.gz"


@pytest.mark.parametrize("day,hour", [("wrong", 0), ("2025-02-29", 0), ("2010-01-01", 0),
                                      ("2099-01-01", 0), ("2025-01-01", -1), ("2025-01-01", 24),
                                      ("2025-01-01", True), ("2025-01-01", 1.5)])
def test_invalid_dates_and_hours(day, hour):
    with pytest.raises(ValueError):
        archive_filename(day, hour)


def test_hour_sequence_crosses_midnight():
    assert build_hours("2025-01-01", 23, 2) == [(date(2025, 1, 1), 23), (date(2025, 1, 2), 0)]
    assert len(build_hours("2025-01-01", all_day=True)) == 24
    assert len(build_hours("2025-01-01", end_date="2025-01-02")) == 48


@pytest.mark.parametrize("kwargs", [{"hours": 0}, {"hours": 169}, {"end_date": "2024-12-31"}, {"hour": 24}])
def test_bad_sequences(kwargs):
    with pytest.raises(ValueError):
        build_hours("2025-01-01", **kwargs)


def test_gzip_integrity(archive, tmp_path):
    assert validate_gzip(archive) > 0
    damaged = tmp_path / "damaged.gz"
    damaged.write_bytes(archive.read_bytes()[:-8])
    with pytest.raises((EOFError, OSError)):
        validate_gzip(damaged)
    empty = tmp_path / "empty.gz"
    empty.write_bytes(gzip.compress(b""))
    with pytest.raises(ValueError, match="zero bytes"):
        validate_gzip(empty)


def test_download_manifest_and_reuse(tmp_path):
    body = gzip.compress(b'{"id":"fixture"}\n')
    session = Mock()
    session.get.return_value = response_for(body)
    result = download_archive("2025-01-01", 12, output_dir=tmp_path, session=session)
    assert result["compressed_bytes"] == len(body)
    assert result["gzip_validated"] and not result["reused"]
    assert (tmp_path / "download_manifest.json").exists()
    reused = download_archive("2025-01-01", 12, output_dir=tmp_path, session=session)
    assert reused["reused"] and reused["sha256"] == result["sha256"]
    assert session.get.call_count == 1


def test_retry_transient_http_failure(tmp_path):
    session = Mock()
    session.get.side_effect = [response_for(b"", status=503), response_for(gzip.compress(b'{}\n'))]
    download_archive("2025-01-01", 12, output_dir=tmp_path, session=session, backoff=0)
    assert session.get.call_count == 2


def test_timeout_retry(tmp_path):
    session = Mock()
    session.get.side_effect = [requests.Timeout("fixture timeout"), response_for(gzip.compress(b'{}\n'))]
    download_archive("2025-01-01", 12, output_dir=tmp_path, session=session, backoff=0)
    assert session.get.call_count == 2


def test_permanent_http_error_is_not_retried(tmp_path):
    session = Mock()
    session.get.return_value = response_for(b"", status=404)
    with pytest.raises(requests.HTTPError):
        download_archive("2025-01-01", 12, output_dir=tmp_path, session=session, backoff=0)
    assert session.get.call_count == 1
    assert not list(tmp_path.glob("*.part"))


@pytest.mark.parametrize("known_length", [True, False])
def test_quota_enforced_even_without_content_length(tmp_path, known_length):
    session = Mock()
    session.get.return_value = response_for(gzip.compress(b'{}\n'), known_length=known_length)
    with pytest.raises(ValueError, match="quota|limit"):
        download_archive("2025-01-01", 12, output_dir=tmp_path, session=session, max_disk_mb=0.00001)
    assert not list(tmp_path.glob("*.part"))
    assert not list(tmp_path.glob("*.json.gz"))


def test_corrupt_replacement_is_not_promoted(tmp_path):
    session = Mock()
    session.get.return_value = response_for(b"not gzip")
    with pytest.raises(OSError):
        download_archive("2025-01-01", 12, output_dir=tmp_path, session=session, retries=0)
    assert not list(tmp_path.glob("*.part"))
    assert not list(tmp_path.glob("*.json.gz"))


def test_existing_raw_files_count_toward_quota(tmp_path):
    (tmp_path / "another-file.gz").write_bytes(b"x" * 1024)
    session = Mock()
    with pytest.raises(ValueError, match="limit"):
        download_archive("2025-01-01", 12, output_dir=tmp_path, session=session, max_disk_mb=.0001)
    session.get.assert_not_called()
