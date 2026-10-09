"""Only checksum-verified, completed Week 2 archives enter production ETL."""
import json
import re
from pathlib import Path

from config.settings import RAW_DIR, ROOT
from exploration.io import sha256_file
from storage.backends import WebHDFSBronze, partition_path
from storage.hdfs_cluster import HDFS_URI


def plan_sources(manifest_path=ROOT / "data/metadata/latest_collection.json", *,
                 backend="hdfs", hours=None, raw_dir=RAW_DIR):
    manifest = json.loads(Path(manifest_path).read_text())
    if manifest.get("status") != "complete":
        raise ValueError("Collection must be complete before ETL")
    if backend not in {"hdfs", "local"}:
        raise ValueError("backend must be hdfs or local")
    selections = manifest["hours"]
    if hours is not None:
        selections = [item for item in selections if item["hour"] in hours]
        if not selections or set(hours) != {item["hour"] for item in selections}:
            raise ValueError("Requested hours are absent from the collection")
    results = {item["filename"]: item for item in manifest["results"]}
    sources = []
    for item in selections:
        name = item["filename"]
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}-\d{1,2}\.json\.gz", name):
            raise ValueError(f"Unsafe archive filename: {name}")
        result = results[name]
        download, validation = result["download"], result["validation"]
        if (result["status"] != "complete" or validation["status"] != "validated"
                or download.get("provenance") != "HTTPS download from GH Archive"
                or not download.get("gzip_validated")):
            raise ValueError(f"Unverified archive: {name}")
        digest = download["sha256"]
        if validation["sha256"] != digest:
            raise ValueError(f"Conflicting source checksums: {name}")
        path = Path(raw_dir) / name
        relative = partition_path(item["date"], item["hour"], name)
        uri = path.resolve().as_uri() if backend == "local" else f"{HDFS_URI}/devpulse/bronze/{relative}"
        sources.append({"filename": name, "date": item["date"], "hour": item["hour"],
                        "uri": uri, "local_path": str(path.resolve()), "sha256": digest,
                        "compressed_bytes": download["compressed_bytes"],
                        "records": validation["record_count"],
                        "lines": validation["read_stats"]["lines"], "backend": backend})
    if not sources or len({s["filename"] for s in sources}) != len(sources):
        raise ValueError("Sources must be a nonempty unique archive list")
    return sorted(sources, key=lambda s: s["filename"])


def merge_sources(existing, incoming):
    combined = {s["filename"]: s for s in existing}
    for source in incoming:
        old = combined.get(source["filename"])
        if old and (old["sha256"] != source["sha256"] or old["lines"] != source["lines"]):
            raise ValueError(f"Historical archive changed: {source['filename']}")
        if old and old["backend"] != source["backend"]:
            raise ValueError("Use a separate output root when switching input backend")
        combined[source["filename"]] = source
    return sorted(combined.values(), key=lambda s: s["filename"])


def verify_sources(sources):
    backend = WebHDFSBronze("/devpulse/bronze") if any(s["backend"] == "hdfs" for s in sources) else None
    for source in sources:
        path = Path(source["local_path"])
        if path.stat().st_size != source["compressed_bytes"] or sha256_file(path) != source["sha256"]:
            raise ValueError(f"Local source checksum mismatch: {source['filename']}")
        if source["backend"] == "hdfs":
            relative = partition_path(source["date"], source["hour"], source["filename"])
            expected_uri = f"{HDFS_URI}/devpulse/bronze/{relative}"
            if source["uri"] != expected_uri or backend.file_hash(relative) != source["sha256"]:
                raise ValueError(f"HDFS source checksum mismatch: {source['filename']}")
