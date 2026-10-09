"""Atomic, checksum-verified bronze publication; flat raw downloads stay unchanged."""
import getpass
import hashlib
import os
import uuid
from pathlib import Path, PurePosixPath
from urllib.parse import quote, urlparse

import requests

from exploration.io import sha256_file
from storage.hdfs_cluster import HDFS_URI, PORTS, run_hdfs


def require_success(response):
    """Retain Hadoop's actual RemoteException rather than only an HTTP code."""
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        try:
            details = response.json().get("RemoteException", {})
        except (ValueError, AttributeError):
            details = {}
        if isinstance(details, dict) and details.get("message"):
            message = str(details['message']).splitlines()[0]
            raise requests.HTTPError(f"{exc}; {details.get('exception')}: {message}", response=response) from exc
        raise


def partition_path(day, hour, filename):
    from ingestion.download_gharchive import archive_filename
    if filename != archive_filename(day, hour):
        raise ValueError("filename does not match its UTC partition")
    return PurePosixPath(f"date={day}/hour={hour:02d}/{filename}")


class LocalBronze:
    name = "local"

    def __init__(self, root, max_bytes):
        self.root, self.max_bytes = Path(root).resolve(), max_bytes
        if max_bytes <= 0:
            raise ValueError("storage budget must be positive")

    def prepare(self):
        self.root.mkdir(parents=True, exist_ok=True)

    def file_hash(self, relative):
        target = self.root / str(relative)
        if not target.resolve().is_relative_to(self.root):
            raise ValueError("destination escapes the bronze root")
        return sha256_file(target) if target.is_file() else None

    def publish(self, source, relative, expected_sha):
        target = self.root / str(relative)
        if not target.resolve().is_relative_to(self.root):
            raise ValueError("destination escapes the bronze root")
        if target.exists():
            if sha256_file(target) != expected_sha:
                raise ValueError(f"Existing bronze file has a conflicting checksum: {target}")
            return {"status": "reused", "uri": target.as_uri(), "sha256": expected_sha}
        target.parent.mkdir(parents=True, exist_ok=True)
        used = sum(path.stat().st_size for path in self.root.rglob("*") if path.is_file())
        if used + Path(source).stat().st_size > self.max_bytes:
            raise ValueError("local bronze logical-byte budget exceeded")
        temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.uploading")
        try:
            # Hard links avoid duplicating bytes. No mutable source writes are used.
            # Cross-device layouts use a bounded streaming copy instead.
            try:
                os.link(source, temporary)
            except OSError:
                with open(source, "rb") as incoming, temporary.open("xb") as outgoing:
                    for chunk in iter(lambda: incoming.read(1024 * 1024), b""):
                        outgoing.write(chunk)
            if sha256_file(temporary) != expected_sha:
                raise ValueError("local bronze staging checksum mismatch")
            # link() is atomic and never overwrites a concurrently created target.
            os.link(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)
        return {"status": "published", "uri": target.as_uri(), "sha256": expected_sha}


class WebHDFSBronze:
    name = "hdfs"

    def __init__(self, root="/devpulse/bronze", max_bytes=4 * 1024**3, session=None):
        if (not root.startswith("/devpulse/") or ".." in PurePosixPath(root).parts
                or "?" in root or "#" in root or max_bytes <= 0):
            raise ValueError("HDFS bronze root must be an absolute path below /devpulse and budget positive")
        self.root, self.max_bytes = root.rstrip("/"), max_bytes
        self.session = session or requests.Session()
        self.session.trust_env = False

    def path(self, relative):
        relative = PurePosixPath(relative)
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("destination escapes the HDFS bronze root")
        return f"{self.root}/{relative}"

    def request(self, operation, path, *, method="GET", **params):
        url = f"http://127.0.0.1:{PORTS['namenode_http']}/webhdfs/v1{quote(path, safe='/')}"
        return self.session.request(method, url, params={"op": operation, "user.name": getpass.getuser(), **params},
                                    timeout=(5, 90), allow_redirects=False)

    def boolean_operation(self, operation, path, **params):
        response = self.request(operation, path, method="PUT", **params)
        require_success(response)
        if response.json().get("boolean") is not True:
            raise RuntimeError(f"WebHDFS {operation} returned false for {path}")

    def prepare(self):
        self.boolean_operation("MKDIRS", self.root)
        run_hdfs(["dfsadmin", "-setSpaceQuota", str(self.max_bytes), HDFS_URI + self.root])

    def status(self, path):
        response = self.request("GETFILESTATUS", path)
        if response.status_code == 404:
            details = response.json().get("RemoteException", {})
            if details.get("exception") == "FileNotFoundException":
                return None
        require_success(response)
        return response.json()["FileStatus"]

    def redirect(self, response):
        if response.status_code != 307:
            require_success(response)
            raise RuntimeError(f"Expected a WebHDFS DataNode redirect, got {response.status_code}")
        location = response.headers["Location"]
        parsed = urlparse(location)
        if (parsed.scheme != "http" or parsed.hostname != "127.0.0.1"
                or parsed.port != PORTS["datanode_http"] or parsed.username or parsed.password):
            raise RuntimeError("WebHDFS redirect did not point to the configured loopback DataNode")
        return location

    def file_hash(self, relative):
        path = self.path(relative)
        if self.status(path) is None:
            return None
        location = self.redirect(self.request("OPEN", path))
        digest = hashlib.sha256()
        with self.session.get(location, stream=True, timeout=(5, 90), allow_redirects=False) as response:
            require_success(response)
            for chunk in response.iter_content(1024 * 1024):
                digest.update(chunk)
        return digest.hexdigest()

    def delete_temporary(self, path):
        response = self.request("DELETE", path, method="DELETE", recursive="false")
        require_success(response)

    def publish(self, source, relative, expected_sha):
        destination = self.path(relative)
        known = self.file_hash(relative)
        if known is not None:
            if known != expected_sha:
                raise ValueError(f"Existing HDFS file has a conflicting checksum: {destination}")
            return {"status": "reused", "uri": HDFS_URI + destination, "sha256": known}
        self.boolean_operation("MKDIRS", str(PurePosixPath(destination).parent))
        temporary = destination + f".{uuid.uuid4().hex}.uploading"
        temp_relative = temporary[len(self.root) + 1:]
        try:
            location = self.redirect(self.request("CREATE", temporary, method="PUT",
                                                  overwrite="false", replication=1,
                                                  blocksize=64 * 1024 * 1024))
            with open(source, "rb") as handle:
                response = self.session.put(location, data=handle, timeout=(5, 90), allow_redirects=False)
                require_success(response)
                if response.status_code != 201:
                    raise RuntimeError("DataNode did not confirm file creation")
            staged_status = self.status(temporary)
            if not staged_status or staged_status["length"] != Path(source).stat().st_size:
                raise ValueError("HDFS staging length mismatch")
            if self.file_hash(temp_relative) != expected_sha:
                raise ValueError("HDFS staging checksum mismatch")
            self.boolean_operation("RENAME", temporary, destination=destination)
        except BaseException:
            try:
                self.delete_temporary(temporary)
            except requests.RequestException:
                pass  # Do not hide the original upload error if cleanup cannot connect.
            raise
        return {"status": "published", "uri": HDFS_URI + destination, "sha256": expected_sha}

    def inventory(self):
        response = self.request("GETCONTENTSUMMARY", self.root)
        require_success(response)
        return response.json()["ContentSummary"]
