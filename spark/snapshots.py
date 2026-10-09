"""Immutable datasets and a validated, atomically switched current pointer."""
import hashlib
import json
from pathlib import Path

from exploration.io import sha256_file, write_json
from spark.schema import CONTRACT_VERSION


def recipe_for(sources):
    code_dir = Path(__file__).parent
    code_hashes = {name: sha256_file(code_dir / name) for name in
                   ("schema.py", "transform_events.py", "clean_events.py", "snapshots.py", "session.py", "sources.py")}
    recipe = {"contract_version": CONTRACT_VERSION, "spark_version": "4.0.1",
              "code_hashes": code_hashes, "sources": sources,
              "timezone": "UTC", "compression": "snappy", "timestamp_type": "TIMESTAMP_MICROS"}
    digest = hashlib.sha256(json.dumps(recipe, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return digest[:24], recipe


def inventory(root):
    root = Path(root)
    return [{"path": str(path.relative_to(root)), "bytes": path.stat().st_size,
             "sha256": sha256_file(path)} for path in sorted(root.rglob("*"))
            if path.is_file() and path.name != "manifest.json"]


def verify_snapshot(root):
    root = Path(root)
    manifest = json.loads((root / "manifest.json").read_text())
    if manifest.get("status") != "complete" or inventory(root) != manifest["files"]:
        raise ValueError(f"Snapshot inventory mismatch: {root}")
    # Older code versions are readable; their recorded recipe remains the identity.
    recorded = hashlib.sha256(json.dumps(manifest["recipe"], sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:24]
    if recorded != manifest["snapshot_id"]:
        raise ValueError("Snapshot recipe identity mismatch")
    return manifest


def current_manifest(output_root):
    root = Path(output_root)
    pointer = root / "_CURRENT.json"
    if not pointer.exists():
        return None
    current = json.loads(pointer.read_text())
    identifier = current["snapshot_id"]
    if len(identifier) != 24 or any(c not in "0123456789abcdef" for c in identifier):
        raise ValueError("Invalid snapshot pointer")
    snapshot = root / "snapshots" / identifier
    if sha256_file(snapshot / "manifest.json") != current["manifest_sha256"]:
        raise ValueError("Current manifest checksum mismatch")
    return verify_snapshot(snapshot)


def switch_current(output_root, snapshot_id):
    root = Path(output_root)
    manifest = root / "snapshots" / snapshot_id / "manifest.json"
    write_json(root / "_CURRENT.json", {"snapshot_id": snapshot_id,
                                       "manifest_sha256": sha256_file(manifest)})
