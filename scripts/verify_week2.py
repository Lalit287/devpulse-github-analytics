"""Real-data recovery, quota, restart, and integrity drills (no external downloads)."""
import argparse
import json
import logging
import signal
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from contextlib import closing

from config.settings import RAW_DIR, ROOT
from exploration.io import sha256_file, write_json
from ingestion.audit import audit_collection, save_audit
from ingestion.collect import run_collection
from ingestion.state import utc_now
from storage.backends import LocalBronze, WebHDFSBronze
from storage.hdfs_cluster import PROFILE, RUNTIME, HDFS_URI, hadoop_home, health, jmx, PORTS, run_hdfs, start_cluster, stop_cluster

REPORTS = ROOT / "reports/week2"


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def recovery_drill(hours):
    metadata = ROOT / "work/week2/recovery_metadata"
    bronze = ROOT / "data/bronze"
    log_path = REPORTS / "interruption_drill.log"
    # A fresh drill ledger makes the first run perform real JSON validation.
    metadata = metadata / f"drill-{time.time_ns()}"
    command = [sys.executable, "-m", "ingestion.collect", "--date", hours[0]["date"],
               "--hour", str(hours[0]["hour"]), "--hours", "2", "--backend", "local",
               "--metadata-dir", str(metadata)]
    with log_path.open("w") as handle:
        process = subprocess.Popen(command, cwd=ROOT, stdout=handle, stderr=subprocess.STDOUT)
    deadline = time.monotonic() + 30
    interrupted = False
    while process.poll() is None and time.monotonic() < deadline:
        if "Validated " + hours[0]["filename"] in log_path.read_text():
            process.send_signal(signal.SIGINT)
            interrupted = True
            break
        time.sleep(.05)
    if not interrupted:
        if process.poll() is None:
            process.terminate()
        process.wait(timeout=10)
        raise RuntimeError("Could not trigger the controlled validation interruption")
    process.wait(timeout=30)
    initial = json.loads((metadata / "latest_collection.json").read_text())
    require(initial["status"] == "interrupted", "SIGINT did not record interrupted state")
    backend = LocalBronze(bronze, 4 * 1024**3)
    resumed = run_collection(hours[:2], metadata_dir=metadata, backend=backend)
    repeated = run_collection(hours[:2], metadata_dir=metadata, backend=backend)
    require(resumed["status"] == "complete" and resumed["downloaded_files"] == 0,
            "Interrupted-run recovery did not reuse the two raw archives")
    require(repeated["reused_publications"] == 2 and repeated["reused_validations"] == 2,
            "Local recovery rerun was not idempotent")
    audit = audit_collection(metadata / "latest_collection.json", metadata_dir=metadata,
                             backend=backend, verify_storage=True)
    require(audit["status"] == "passed", "Recovered local bronze audit failed")
    evidence = {"status": "passed", "intentional_signal": "SIGINT", "first_run_status": initial["status"],
                "resume_status": resumed["status"], "resume_downloads": resumed["downloaded_files"],
                "resumed_hours": resumed["completed_hours"], "local_audit": audit["status"],
                "repeat_reused_publications": repeated["reused_publications"], "metadata_dir": str(metadata)}
    write_json(REPORTS / "recovery_drill.json", evidence)
    return evidence


def quota_drill(hours):
    metadata = ROOT / "work/week2/quota_metadata" / f"drill-{time.time_ns()}"
    backend = WebHDFSBronze("/devpulse/quota-drill", max_bytes=1)
    result = run_collection(hours[:1], metadata_dir=metadata, backend=backend)
    error = result["results"][0].get("error", "")
    require(result["status"] == "failed" and "Quota" in error, "One-byte HDFS quota did not produce its expected quota failure")
    inventory = backend.inventory()
    require(inventory["fileCount"] == 0 and inventory["length"] == 0,
            "Quota failure left a published or partial HDFS file")
    evidence = {"status": "passed", "expected_failure": "HDFS space quota exceeded",
                "collection_status": result["status"], "actual_error": error,
                "remaining_files": inventory["fileCount"], "remaining_bytes": inventory["length"]}
    write_json(REPORTS / "quota_drill.json", evidence)
    return evidence


def verify_running_cluster():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    REPORTS.mkdir(parents=True, exist_ok=True)
    initial_path = REPORTS / "initial_collection.json"
    if not initial_path.exists():
        current = json.loads((ROOT / "data/metadata/latest_collection.json").read_text())
        require(current["status"] == "complete" and current["planned_hours"] == 24 and current["backend"] == "hdfs",
                "First complete a 24-hour HDFS collection")
        write_json(initial_path, current)
    initial = json.loads(initial_path.read_text())
    hours = initial["hours"]
    metadata = ROOT / "data/metadata"
    if PROFILE == "verification":
        metadata = ROOT / "work/week2/verification_metadata"
        metadata.mkdir(parents=True, exist_ok=True)
        if not (metadata / "ingestion.sqlite3").exists():
            # SQLite backup takes a consistent snapshot of the measured validations.
            # The original development ledger and latest manifest are never changed.
            with closing(sqlite3.connect(ROOT / "data/metadata/ingestion.sqlite3")) as source:
                with closing(sqlite3.connect(metadata / "ingestion.sqlite3")) as destination:
                    source.backup(destination)
    started = time.monotonic()
    backend = WebHDFSBronze()
    if PROFILE == "verification":
        bootstrap = run_collection(hours, backend=backend, metadata_dir=metadata)
        require(bootstrap["status"] == "complete" and bootstrap["downloaded_files"] == 0,
                "Verification cluster did not publish the existing real sources")
        write_json(REPORTS / "verification_cluster_collection.json", bootstrap)
    repeated = run_collection(hours, backend=backend, metadata_dir=metadata)
    write_json(REPORTS / "idempotency.json", repeated)
    require(repeated["status"] == "complete" and repeated["downloaded_files"] == 0
            and repeated["reused_publications"] == 24 and repeated["reused_validations"] == 24,
            "Full-day rerun was not idempotent")
    audit = audit_collection(metadata / "latest_collection.json", metadata_dir=metadata, backend=backend, verify_storage=True)
    save_audit(audit, REPORTS / "coverage_audit.json")
    require(audit["status"] == "passed" and audit["verified_hours"] == 24, "Full-day integrity audit failed")
    fsck = run_hdfs(["fsck", "/devpulse/bronze", "-files", "-blocks", "-locations"])
    (REPORTS / "hdfs_fsck.txt").write_text(fsck.stdout + "\n" + fsck.stderr)
    require("Status: HEALTHY" in fsck.stdout, "HDFS fsck did not report HEALTHY")
    version_hash = sha256_file(RUNTIME / "namenode/current/VERSION")
    stopped = stop_cluster()
    restarted = start_cluster()
    require(not restarted.get("formatted_new_cluster") and version_hash == sha256_file(RUNTIME / "namenode/current/VERSION"),
            "Restart altered the NameNode identity")
    post_restart = audit_collection(metadata / "latest_collection.json", metadata_dir=metadata, backend=backend, verify_storage=True)
    require(post_restart["status"] == "passed", "Data integrity failed after HDFS restart")
    write_json(REPORTS / "restart_verification.json", {"status": "passed", "profile": PROFILE,
                                                      "verified_hdfs_uri": HDFS_URI, "stop": stopped, "restart": restarted,
                                                      "namenode_version_sha256": version_hash,
                                                      "post_restart_verified_files": post_restart["verified_hours"]})
    recovery = recovery_drill(hours)
    quota = quota_drill(hours)
    original = json.loads((REPORTS / "week1_preservation.json").read_text())
    preserved = {name: sha256_file(ROOT / name) == checksum for name, checksum in original.items()}
    require(all(preserved.values()), "A preserved Week 1 artifact changed")
    version = subprocess.run([str(hadoop_home() / "bin/hadoop"), "version"], capture_output=True, text=True, check=True).stdout.splitlines()[0]
    heaps = {name: jmx(PORTS[port], "java.lang:type=Memory")["HeapMemoryUsage"]["max"]
             for name, port in (("namenode", "namenode_http"), ("datanode", "datanode_http"))}
    summary = {"status": "passed", "verified_at_utc": utc_now(), "duration_seconds": round(time.monotonic() - started, 2),
               "verification_profile": PROFILE, "verified_hdfs_uri": HDFS_URI,
               "verification_metadata_dir": str(metadata),
               "hadoop_version": version, "full_day_rerun_downloads": repeated["downloaded_files"],
               "full_day_reused_publications": repeated["reused_publications"], "coverage_verified_hours": audit["verified_hours"],
               "hdfs_fsck": "HEALTHY", "hdfs_restart": "passed", "recovery_drill": recovery["status"],
               "quota_drill": quota["status"], "week1_artifacts_unchanged": preserved,
               "hdfs_inventory": backend.inventory(), "daemon_heap_max_bytes": heaps,
               "cluster_health": health()}
    summary["final_cluster_stop"] = stop_cluster()
    write_json(REPORTS / "integration_verification.json", summary)
    print(json.dumps(summary, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-cluster", action="store_true", help="start HDFS inside this verification process before checks")
    args = parser.parse_args()
    owns_cluster = False
    try:
        if args.start_cluster:
            result = start_cluster()
            owns_cluster = not result.get("reused_running_cluster", False)
        verify_running_cluster()
    finally:
        # A failed audit must not leave newly launched, orphaned test daemons.
        if owns_cluster:
            stop_cluster()


if __name__ == "__main__":
    main()
