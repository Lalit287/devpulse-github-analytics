"""Audit collection coverage and checksums against the requested UTC window."""
import argparse
import csv
import json
from pathlib import Path

from config.settings import RAW_DIR, ROOT
from exploration.io import sha256_file, write_json
from ingestion.state import StateStore, utc_now
from storage.backends import LocalBronze, WebHDFSBronze, partition_path


def audit_collection(manifest_path, *, raw_dir=RAW_DIR, metadata_dir=ROOT / "data/metadata",
                     backend=None, verify_storage=False):
    manifest_path, raw_dir = Path(manifest_path), Path(raw_dir)
    manifest = json.loads(manifest_path.read_text())
    expected = [item["filename"] for item in manifest["hours"]]
    if len(expected) != len(set(expected)):
        raise ValueError("collection manifest repeats hour selections")
    results = {item["filename"]: item for item in manifest["results"]}
    issues, inventory = [], []
    state = StateStore(Path(metadata_dir) / "ingestion.sqlite3")
    for item in manifest["hours"]:
        filename = item["filename"]
        result = results.get(filename, {})
        row = {"date": item["date"], "hour_utc": item["hour"], "filename": filename,
               "status": "missing", "compressed_bytes": 0, "records": 0,
               "sha256": None, "storage_uri": None}
        if result.get("status") != "complete":
            issues.append({"filename": filename, "reason": result.get("error", "hour not completed")})
            inventory.append(row)
            continue
        path = raw_dir / filename
        if not path.is_file():
            issues.append({"filename": filename, "reason": "raw file missing"})
        else:
            digest = sha256_file(path)
            download, validation = result["download"], result["validation"]
            known = state.archive(filename)
            if not known or known["status"] != "validated":
                issues.append({"filename": filename, "reason": "SQLite archive not validated"})
            elif known.get("validation", {}).get("sha256") != digest:
                issues.append({"filename": filename, "reason": "SQLite validation checksum differs from raw file"})
            sidecar_path = Path(metadata_dir) / "validation" / f"{filename}.json"
            sidecar = json.loads(sidecar_path.read_text()) if sidecar_path.exists() else {}
            if sidecar.get("sha256") != digest or sidecar.get("record_count") != validation["record_count"]:
                issues.append({"filename": filename, "reason": "validation JSON sidecar is missing or inconsistent"})
            if digest != download["sha256"] or digest != validation["sha256"]:
                issues.append({"filename": filename, "reason": "raw/validation/manifest checksum mismatch"})
            if validation["record_count"] != sum(validation["event_type_counts"].values()):
                issues.append({"filename": filename, "reason": "event-type counts do not sum to records"})
            if download.get("provenance") != "HTTPS download from GH Archive":
                issues.append({"filename": filename, "reason": "source provenance is unverified"})
            if path.stat().st_size != download["compressed_bytes"]:
                issues.append({"filename": filename, "reason": "raw length differs from manifest"})
            if verify_storage:
                if backend is None:
                    raise ValueError("provide a backend when verifying storage")
                relative = partition_path(item["date"], item["hour"], filename)
                try:
                    if backend.file_hash(relative) != digest:
                        issues.append({"filename": filename, "reason": "published storage checksum mismatch or file missing"})
                except Exception as exc:
                    issues.append({"filename": filename, "reason": f"storage verification failed: {exc}"})
            row.update({"status": "verified" if not any(issue["filename"] == filename for issue in issues) else "failed",
                        "compressed_bytes": path.stat().st_size, "records": validation["record_count"],
                        "sha256": digest, "storage_uri": result["publication"]["uri"]})
        inventory.append(row)
    records = sum(row["records"] for row in inventory)
    compressed = sum(row["compressed_bytes"] for row in inventory)
    if records != manifest["validated_records"] or compressed != manifest["compressed_bytes"]:
        issues.append({"filename": "<collection>", "reason": "collection totals differ from inventory"})
    return {"status": "passed" if not issues and manifest["status"] == "complete" else "failed",
            "run_id": manifest["run_id"], "audited_at_utc": utc_now(), "expected_hours": len(expected),
            "verified_hours": sum(row["status"] == "verified" for row in inventory),
            "validated_records": records, "compressed_bytes": compressed,
            "storage_checksum_verification": verify_storage, "issues": issues, "inventory": inventory}


def save_audit(result, output):
    output = Path(output)
    write_json(output, result)
    with output.with_suffix(".csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(result["inventory"][0]))
        writer.writeheader()
        writer.writerows(result["inventory"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=ROOT / "data/metadata/latest_collection.json")
    parser.add_argument("--raw-dir", type=Path, default=RAW_DIR)
    parser.add_argument("--metadata-dir", type=Path, default=ROOT / "data/metadata")
    parser.add_argument("--backend", choices=["local", "hdfs"], default="hdfs")
    parser.add_argument("--bronze-dir", type=Path, default=ROOT / "data/bronze")
    parser.add_argument("--hdfs-root", default="/devpulse/bronze")
    parser.add_argument("--verify-storage", action="store_true")
    parser.add_argument("--output", type=Path, default=ROOT / "reports/week2/coverage_audit.json")
    args = parser.parse_args()
    backend = LocalBronze(args.bronze_dir, 4 * 1024**3) if args.backend == "local" else WebHDFSBronze(args.hdfs_root)
    result = audit_collection(args.manifest, raw_dir=args.raw_dir, metadata_dir=args.metadata_dir,
                              backend=backend, verify_storage=args.verify_storage)
    save_audit(result, args.output)
    print(json.dumps({key: value for key, value in result.items() if key != "inventory"}, indent=2))
    if result["status"] != "passed":
        parser.exit(1, "Collection audit failed; inspect the output issues.\n")


if __name__ == "__main__":
    main()
