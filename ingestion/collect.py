"""Restartable, bounded hourly collection with validation and bronze publication."""
import argparse
import json
import logging
import math
import shutil
import uuid
from collections import Counter
from pathlib import Path

import requests

from config.settings import DEFAULT_DATE, RAW_DIR, ROOT, TIMEOUT
from exploration.io import write_json
from ingestion.download_gharchive import download_archive
from ingestion.hour_range import delayed_completed_hour, plan_hours
from ingestion.locking import directory_lock
from ingestion.state import StateStore, utc_now
from ingestion.validate_downloads import VALIDATOR_VERSION, validate_archive
from storage.backends import LocalBronze, WebHDFSBronze, partition_path

LOG = logging.getLogger(__name__)
METADATA_DIR = ROOT / "data/metadata"


def budget_bytes(megabytes):
    if not math.isfinite(megabytes) or megabytes <= 0:
        raise ValueError("disk budget must be positive and finite")
    return int(megabytes * 1024**2)


def collection_plan(hours, raw_dir, max_disk_mb, backend_name):
    """Read-only planning; size estimates are estimates, never measured totals."""
    raw_dir = Path(raw_dir)
    existing = [item for item in hours if (raw_dir / item["filename"]).exists()]
    missing = [item for item in hours if item not in existing]
    known_sizes = [(raw_dir / item["filename"]).stat().st_size for item in existing]
    estimate = sum(known_sizes) / len(known_sizes) if known_sizes else 150 * 1024**2
    return {"planned_hours": len(hours), "existing_files": len(existing), "new_files": len(missing),
            "estimated_new_compressed_bytes": int(len(missing) * estimate),
            "estimate_note": "Planning estimate based on existing selected files or 150 MiB/hour; actual quota enforced while streaming.",
            "raw_budget_bytes": budget_bytes(max_disk_mb), "storage_backend": backend_name,
            "hours": hours}


def run_collection(hours, *, raw_dir=RAW_DIR, metadata_dir=METADATA_DIR, backend=None,
                   max_disk_mb=4096, timeout=TIMEOUT, retries=3, fail_fast=False):
    if not hours:
        raise ValueError("collection window is empty")
    if len({item['filename'] for item in hours}) != len(hours):
        raise ValueError("collection window contains repeated hours")
    budget_bytes(max_disk_mb)
    if not math.isfinite(timeout) or timeout <= 0 or isinstance(retries, bool) or not isinstance(retries, int) or retries < 0:
        raise ValueError("timeout must be positive and retries a nonnegative integer")
    raw_dir, metadata_dir = Path(raw_dir), Path(metadata_dir)
    backend = backend or LocalBronze(ROOT / "data/bronze", budget_bytes(max_disk_mb))
    raw_dir.mkdir(parents=True, exist_ok=True)
    metadata_dir.mkdir(parents=True, exist_ok=True)
    with directory_lock(metadata_dir / ".collection.lock"):
        state = StateStore(metadata_dir / "ingestion.sqlite3")
        run_id = uuid.uuid4().hex
        summary = {"run_id": run_id, "status": "running", "started_at_utc": utc_now(),
                   "backend": backend.name, "planned_hours": len(hours), "raw_budget_bytes": budget_bytes(max_disk_mb),
                   "storage_budget_bytes": backend.max_bytes, "hours": hours, "results": [],
                   "error": None, "dataset_origin": "real GH Archive"}
        state.save_run(run_id, "running", summary)
        results = {}
        try:
            backend.prepare()  # Fail before downloads if the storage destination is unavailable.
            with requests.Session() as session:
                for index, item in enumerate(hours, 1):
                    filename = item["filename"]
                    LOG.info("Hour %d/%d: %s", index, len(hours), filename)
                    previous = state.archive(filename)
                    state.save_archive(filename, "downloading", {"run_id": run_id})
                    try:
                        # At least 256 MiB free remain for filesystem/metadata headroom.
                        if shutil.disk_usage(raw_dir).free < 256 * 1024**2:
                            raise ValueError("Less than 256 MiB free disk space; collection stopped")
                        manifest_path = raw_dir / "download_manifest.json"
                        manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {"files": {}}
                        known = manifest.get("files", {}).get(filename, {})
                        force = (raw_dir / filename).exists() and (
                            known.get("provenance") != "HTTPS download from GH Archive"
                            or not known.get("sha256")
                            or (previous and previous["status"] == "failed"))
                        download = download_archive(item["date"], item["hour"], output_dir=raw_dir,
                                                    max_disk_mb=max_disk_mb, timeout=timeout, retries=retries,
                                                    session=session, force_download=force)
                        old_validation = previous.get("validation", {}) if previous else {}
                        cached = (download["reused"] and old_validation.get("sha256") == download["sha256"]
                                  and old_validation.get("validator_version") == VALIDATOR_VERSION)
                        validation = old_validation if cached else validate_archive(raw_dir / filename)
                        if validation["sha256"] != download["sha256"]:
                            raise ValueError("Archive changed between download validation and JSON validation")
                        record = {"filename": filename, "download": download, "validation": validation,
                                  "validation_reused": cached, "run_id": run_id}
                        state.save_archive(filename, "validated", record)
                        write_json(metadata_dir / "validation" / f"{filename}.json", validation)
                        relative = partition_path(item["date"], item["hour"], filename)
                        publication = backend.publish(raw_dir / filename, relative, download["sha256"])
                        state.save_publication(filename, backend.name, publication["uri"], publication)
                        record.update({"status": "complete", "publication": publication})
                        results[filename] = record
                        LOG.info("Complete: %s (%s records, storage %s)", filename,
                                 validation["record_count"], publication["status"])
                    except Exception as exc:
                        error = f"{type(exc).__name__}: {exc}"
                        results[filename] = {"filename": filename, "status": "failed", "error": error}
                        # A successfully validated archive remains reusable even when storage fails.
                        if state.archive(filename)["status"] != "validated":
                            state.save_archive(filename, "failed", {"error": error, "run_id": run_id})
                        LOG.error("Failed %s: %s", filename, error)
                        if fail_fast:
                            break
        except BaseException as exc:
            summary["error"] = f"{type(exc).__name__}: {exc}"
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                summary["status"] = "interrupted"
                raise
            LOG.error("Collection setup failed: %s", summary["error"])
        finally:
            summary["results"] = [results.get(item["filename"], {"filename": item["filename"], "status": "not_processed"})
                                  for item in hours]
            complete = [result for result in summary["results"] if result["status"] == "complete"]
            summary.update({"completed_hours": len(complete), "missing_hours": [item["filename"] for item in summary["results"] if item["status"] != "complete"],
                            "compressed_bytes": sum(result["download"]["compressed_bytes"] for result in complete),
                            "validated_records": sum(result["validation"]["record_count"] for result in complete),
                            "downloaded_files": sum(not result["download"]["reused"] for result in complete),
                            "reused_downloads": sum(result["download"]["reused"] for result in complete),
                            "reused_validations": sum(result["validation_reused"] for result in complete),
                            "reused_publications": sum(result["publication"]["status"] == "reused" for result in complete),
                            "finished_at_utc": utc_now()})
            types = Counter()
            for result in complete:
                types.update(result["validation"]["event_type_counts"])
            summary["event_type_counts"] = dict(sorted(types.items()))
            if summary["status"] != "interrupted":
                summary["status"] = "complete" if len(complete) == len(hours) and not summary["error"] else "failed"
            state.save_run(run_id, summary["status"], summary)
            write_json(metadata_dir / "collections" / f"{run_id}.json", summary)
            write_json(metadata_dir / "latest_collection.json", summary)
        return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument("--date", help=f"UTC start date; defaults to {DEFAULT_DATE}")
    selection.add_argument("--latest", action="store_true", help="one completed hour with publication buffer")
    parser.add_argument("--lag-hours", type=int, default=6)
    parser.add_argument("--hour", type=int, default=0)
    window = parser.add_mutually_exclusive_group()
    window.add_argument("--hours", type=int, default=None)
    window.add_argument("--end-date", help="inclusive complete UTC days")
    parser.add_argument("--backend", choices=["local", "hdfs"], default="local")
    parser.add_argument("--raw-dir", type=Path, default=RAW_DIR)
    parser.add_argument("--metadata-dir", type=Path, default=METADATA_DIR)
    parser.add_argument("--bronze-dir", type=Path, default=ROOT / "data/bronze")
    parser.add_argument("--hdfs-root", default="/devpulse/bronze")
    parser.add_argument("--max-disk-mb", type=float, default=4096)
    parser.add_argument("--max-storage-mb", type=float, default=4096)
    parser.add_argument("--timeout", type=float, default=TIMEOUT)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--fail-fast", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        if args.latest:
            if args.hours is not None or args.end_date is not None:
                raise ValueError("--latest selects exactly one hour; do not combine with --hours/--end-date")
            latest = delayed_completed_hour(lag_hours=args.lag_hours)
            hours = plan_hours(latest.date().isoformat(), latest.hour, 1)
        else:
            hours = plan_hours(args.date or DEFAULT_DATE, args.hour, 24 if args.hours is None else args.hours, args.end_date)
        storage_budget = budget_bytes(args.max_storage_mb)
        if args.dry_run:
            print(json.dumps(collection_plan(hours, args.raw_dir, args.max_disk_mb, args.backend), indent=2))
            return
        backend = (LocalBronze(args.bronze_dir, storage_budget) if args.backend == "local"
                   else WebHDFSBronze(args.hdfs_root, storage_budget))
        summary = run_collection(hours, raw_dir=args.raw_dir, metadata_dir=args.metadata_dir,
                                 backend=backend, max_disk_mb=args.max_disk_mb, timeout=args.timeout,
                                 retries=args.retries, fail_fast=args.fail_fast)
        print(json.dumps({key: value for key, value in summary.items() if key not in {"hours", "results"}}, indent=2))
        if summary["status"] != "complete":
            parser.exit(1, "Collection incomplete; inspect data/metadata/latest_collection.json and rerun the same window.\n")
    except (ValueError, OSError, RuntimeError) as exc:
        parser.exit(1, f"Collection failed: {type(exc).__name__}: {exc}\n")


if __name__ == "__main__":
    main()
