"""Shared immutable analytics publication helpers."""
import hashlib
import json
import shutil
from pathlib import Path

from exploration.io import sha256_file, write_json
from spark.snapshots import inventory, verify_snapshot, switch_current


def identity(recipe):
    return hashlib.sha256(json.dumps(recipe, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:24]


def code_hashes():
    root = Path(__file__).parent.parent
    return {str(path.relative_to(root)): sha256_file(path) for folder in ("analytics", "enrichment")
            for path in sorted((root / folder).glob("*.py"))}


def write_table(frame, target, partitions=()):
    writer = frame.coalesce(8).write.mode("errorifexists").option("compression", "snappy").option("maxRecordsPerFile", 250000)
    if partitions:
        writer = writer.partitionBy(*partitions)
    writer.parquet(Path(target).as_uri())


def finish_snapshot(stage, root, snapshot_id, manifest, max_bytes=2 * 1024**3):
    root, stage = Path(root), Path(stage)
    manifest.update(status="complete", snapshot_id=snapshot_id, files=inventory(stage))
    manifest["output_bytes"] = sum(item["bytes"] for item in manifest["files"])
    if sum(p.stat().st_size for p in root.rglob("*") if p.is_file()) > max_bytes:
        raise ValueError("Analytics output quota exceeded; current snapshot is preserved")
    write_json(stage / "manifest.json", manifest)
    verify_snapshot(stage)
    target = root / "snapshots" / snapshot_id
    stage.rename(target)
    switch_current(root, snapshot_id)
    return target


def cleanup_session(session):
    import warnings
    try:
        session.stop()
    except Exception as exc:
        warnings.warn(f"Analytics Spark cleanup failed: {type(exc).__name__}", RuntimeWarning)
