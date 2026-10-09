import os
import json
import subprocess
import sys
import xml.etree.ElementTree as ET
from unittest.mock import Mock

import pytest
import requests

from exploration.io import sha256_file
from storage.backends import LocalBronze, WebHDFSBronze, partition_path, require_success
from storage import hdfs_cluster
from storage.hdfs_cluster import PORTS, write_configuration


def test_partition_hour_is_zero_padded_and_filename_is_not():
    assert str(partition_path("2025-01-01", 3, "2025-01-01-3.json.gz")) == "date=2025-01-01/hour=03/2025-01-01-3.json.gz"
    with pytest.raises(ValueError):
        partition_path("2025-01-01", 3, "2025-01-01-4.json.gz")


def test_local_atomic_publish_and_reuse(tmp_path, archive):
    backend = LocalBronze(tmp_path / "bronze", 1024**2)
    backend.prepare()
    relative = "date=2025-01-01/hour=12/fixture.json.gz"
    first = backend.publish(archive, relative, sha256_file(archive))
    second = backend.publish(archive, relative, sha256_file(archive))
    assert first["status"] == "published" and second["status"] == "reused"
    assert (backend.root / relative).stat().st_ino == archive.stat().st_ino


def test_local_conflict_preserves_existing_file(tmp_path, archive):
    backend = LocalBronze(tmp_path / "bronze", 1024**2)
    backend.prepare()
    target = backend.root / "fixture.json.gz"
    target.write_bytes(b"unrelated existing file")
    with pytest.raises(ValueError, match="conflicting"):
        backend.publish(archive, target.name, sha256_file(archive))
    assert target.read_bytes() == b"unrelated existing file"


def test_staging_hash_failure_leaves_no_committed_file(tmp_path, archive):
    backend = LocalBronze(tmp_path / "bronze", 1024**2)
    backend.prepare()
    with pytest.raises(ValueError, match="checksum"):
        backend.publish(archive, "fixture.json.gz", "bad-hash")
    assert not list(backend.root.rglob("*.uploading"))
    assert not (backend.root / "fixture.json.gz").exists()


def test_local_budget_enforced(tmp_path, archive):
    backend = LocalBronze(tmp_path / "bronze", 1)
    backend.prepare()
    with pytest.raises(ValueError, match="budget"):
        backend.publish(archive, "fixture.json.gz", sha256_file(archive))


def test_cross_device_link_falls_back_to_streaming_copy(tmp_path, archive, monkeypatch):
    backend = LocalBronze(tmp_path / "bronze", 1024**2)
    backend.prepare()
    real_link = os.link
    count = 0
    def fail_once(source, destination):
        nonlocal count
        count += 1
        if count == 1:
            raise OSError("synthetic cross-device link")
        real_link(source, destination)
    monkeypatch.setattr(os, "link", fail_once)
    backend.publish(archive, "fixture.json.gz", sha256_file(archive))
    assert sha256_file(backend.root / "fixture.json.gz") == sha256_file(archive)


@pytest.mark.parametrize("path", ["../escaped", "/absolute/outside"])
def test_local_path_traversal_rejected(tmp_path, archive, path):
    backend = LocalBronze(tmp_path / "bronze", 1024**2)
    with pytest.raises(ValueError, match="escapes"):
        backend.publish(archive, path, sha256_file(archive))


def test_hdfs_config_contains_isolated_loopback_paths(tmp_path):
    conf = tmp_path / "conf"
    runtime = tmp_path / "runtime"
    write_configuration(conf, runtime)
    root = ET.parse(conf / "hdfs-site.xml").getroot()
    values = {prop.findtext("name"): prop.findtext("value") for prop in root}
    assert values["dfs.replication"] == "1"
    assert values["dfs.namenode.rpc-address"] == "127.0.0.1:19000"
    assert values["dfs.namenode.name.dir"] == (runtime / "namenode").as_uri()
    assert values["dfs.datanode.http.address"] == f"127.0.0.1:{PORTS['datanode_http']}"


@pytest.mark.parametrize("root", ["/", "/elsewhere/raw", "/devpulse/../outside", "/devpulse/x?param=y"])
def test_hdfs_root_restricted(root):
    with pytest.raises(ValueError):
        WebHDFSBronze(root)


@pytest.mark.parametrize("location", ["http://example.org:19864/file", "https://127.0.0.1:19864/file",
                                      "http://127.0.0.1:9999/file", "http://user:pass@127.0.0.1:19864/file"])
def test_external_or_wrong_datanode_redirect_rejected(location):
    response = Mock(status_code=307, headers={"Location": location})
    with pytest.raises(RuntimeError, match="loopback"):
        WebHDFSBronze().redirect(response)


def test_good_datanode_redirect():
    location = "http://127.0.0.1:19864/webhdfs/v1/devpulse/bronze/file"
    assert WebHDFSBronze().redirect(Mock(status_code=307, headers={"Location": location})) == location


def test_hdfs_forbidden_does_not_mean_file_missing():
    session = Mock()
    response = Mock(status_code=403)
    response.raise_for_status.side_effect = requests.HTTPError("forbidden")
    session.request.return_value = response
    with pytest.raises(requests.HTTPError):
        WebHDFSBronze(session=session).status("/devpulse/bronze/file")


def test_hadoop_remote_exception_is_retained():
    response = Mock()
    response.raise_for_status.side_effect = requests.HTTPError("HTTP 403", response=response)
    response.json.return_value = {"RemoteException": {"exception": "DSQuotaExceededException", "message": "quota exceeded"}}
    with pytest.raises(requests.HTTPError, match="DSQuotaExceededException.*quota exceeded"):
        require_success(response)


def test_process_identity_must_match_before_stopping(monkeypatch):
    monkeypatch.setattr(hdfs_cluster, "jmx", lambda *a: {"Name": "1234@host"})
    assert hdfs_cluster.matching_process({"http_port": 0, "pid": 1234})
    assert not hdfs_cluster.matching_process({"http_port": 0, "pid": 9999})


def test_nonempty_unknown_hdfs_storage_is_never_formatted(tmp_path, monkeypatch):
    runtime = tmp_path / "runtime"
    (runtime / "namenode").mkdir(parents=True)
    (runtime / "datanode").mkdir()
    (runtime / "namenode/unrelated-data").write_text("preserve")
    monkeypatch.setattr(hdfs_cluster, "RUNTIME", runtime)
    command = Mock()
    monkeypatch.setattr(hdfs_cluster, "run_hdfs", command)
    with pytest.raises(RuntimeError, match="Refusing to format"):
        hdfs_cluster.format_if_new()
    command.assert_not_called()


def test_verification_profile_cannot_use_development_ports_or_storage():
    code = ('import json; from storage.hdfs_cluster import CONF_DIR, RUNTIME, PORTS, HDFS_URI; '
            'print(json.dumps({"conf":str(CONF_DIR),"runtime":str(RUNTIME),"ports":PORTS,"uri":HDFS_URI}))')
    profiles = {}
    for name in ('development', 'verification'):
        result = subprocess.run([sys.executable, '-c', code],
                                env={**os.environ, 'DEVPULSE_HDFS_PROFILE': name},
                                capture_output=True, text=True, check=True)
        profiles[name] = json.loads(result.stdout)
    assert set(profiles['development']['ports'].values()).isdisjoint(profiles['verification']['ports'].values())
    assert profiles['development']['runtime'] != profiles['verification']['runtime']
    assert profiles['development']['conf'] != profiles['verification']['conf']


def test_unrecognized_hdfs_profile_is_rejected():
    result = subprocess.run([sys.executable, '-c', 'import storage.hdfs_cluster'],
                            env={**os.environ, 'DEVPULSE_HDFS_PROFILE': '../unexpected'},
                            capture_output=True, text=True)
    assert result.returncode != 0
    assert 'must be development or verification' in result.stderr


def test_owned_verification_cluster_is_cleaned_up_after_failure(monkeypatch):
    from scripts import verify_week2
    monkeypatch.setattr(sys, 'argv', ['verify_week2', '--start-cluster'])
    monkeypatch.setattr(verify_week2, 'start_cluster', lambda: {'reused_running_cluster': False})
    cleanup = Mock()
    monkeypatch.setattr(verify_week2, 'stop_cluster', cleanup)
    def fail():
        raise RuntimeError('synthetic audit failure')
    monkeypatch.setattr(verify_week2, 'verify_running_cluster', fail)
    with pytest.raises(RuntimeError, match='synthetic audit failure'):
        verify_week2.main()
    cleanup.assert_called_once()


def test_existing_cluster_is_not_signalled_by_failed_start_wrapper(monkeypatch):
    from scripts import verify_week2
    monkeypatch.setattr(sys, 'argv', ['verify_week2', '--start-cluster'])
    monkeypatch.setattr(verify_week2, 'start_cluster', lambda: {'reused_running_cluster': True})
    cleanup = Mock()
    monkeypatch.setattr(verify_week2, 'stop_cluster', cleanup)
    def fail():
        raise RuntimeError('synthetic audit failure')
    monkeypatch.setattr(verify_week2, 'verify_running_cluster', fail)
    with pytest.raises(RuntimeError):
        verify_week2.main()
    cleanup.assert_not_called()
