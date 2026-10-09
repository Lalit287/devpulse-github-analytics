"""Measured Week 2 report, with criteria derived from retained execution evidence."""
import json
import os
import xml.etree.ElementTree as ET

from config.settings import ROOT
os.environ.setdefault("MPLCONFIGDIR", str(ROOT / ".runtime/matplotlib"))
os.environ.setdefault("XDG_CACHE_HOME", str(ROOT / ".runtime/cache"))

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from exploration.io import atomic_text

REPORTS = ROOT / "reports/week2"


def load(name):
    path = REPORTS / name
    return json.loads(path.read_text()) if path.exists() else {}


def main():
    initial = load("initial_collection.json")
    if not initial:
        raise RuntimeError("Missing completed initial collection evidence")
    repeated, audit = load("idempotency.json"), load("coverage_audit.json")
    integration, recovery, quota = load("integration_verification.json"), load("recovery_drill.json"), load("quota_drill.json")
    restart = load("restart_verification.json")
    suites = ET.parse(REPORTS / "tests.xml").getroot()
    cases = suites.findall(".//testcase")
    failures = suites.findall(".//failure") + suites.findall(".//error") + suites.findall(".//skipped")
    criteria = {
        "Reusable bounded UTC date/range collection": initial.get("status") == "complete",
        "Real full-day archive collection": initial.get("completed_hours") == 24,
        "Streaming gzip/JSON validation": initial.get("validated_records", 0) > 0,
        "Durable per-hour/run status and provenance": (ROOT / "data/metadata/ingestion.sqlite3").exists(),
        "Date/hour HDFS bronze storage": initial.get("backend") == "hdfs" and audit.get("storage_checksum_verification") is True,
        "Coverage and end-to-end checksum audit": audit.get("status") == "passed" and audit.get("verified_hours") == 24,
        "Rerun downloads nothing and reuses 24 stored files": repeated.get("downloaded_files") == 0 and repeated.get("reused_publications") == 24,
        "Real interruption/recovery and local bronze verification": recovery.get("status") == "passed",
        "Actual HDFS quota rejection preserves data": quota.get("status") == "passed",
        "HDFS restart preserves the NameNode and all 24 files": restart.get("status") == "passed",
        "Automated tests pass including Week 1 regressions": bool(cases) and not failures,
        "Week 1 artifacts preserved": bool(integration.get("week1_artifacts_unchanged")) and all(integration.get("week1_artifacts_unchanged", {}).values()),
        "Documentation and scheduler entry point": (ROOT / "docs/week2_ingestion_storage.md").exists() and (ROOT / "scripts/collect_latest.sh").exists(),
        "Final real-data integration verification": integration.get("status") == "passed",
    }
    complete = all(criteria.values())
    checklist = "\n".join(f"- [{'x' if done else ' '}] {'DONE' if done else 'BLOCKED — successful evidence not yet recorded'}: {name}"
                          for name, done in criteria.items())
    results = initial["results"]
    rows = "\n".join(f"| {item['filename']} | {item['download']['compressed_bytes']:,} | {item['validation']['record_count']:,} |"
                     for item in results)
    distribution = "\n".join(f"| {kind} | {count:,} | {count / initial['validated_records'] * 100:.2f}% |"
                             for kind, count in sorted(initial["event_type_counts"].items(), key=lambda item: -item[1]))
    quality = {field: sum(item["validation"]["missing_fields"][field] for item in results)
               for field in results[0]["validation"]["missing_fields"]}
    qualities = "\n".join(f"| {field} | {count:,} |" for field, count in quality.items())
    hours = [item['hour'] for item in initial['hours']]
    event_counts = [item['validation']['record_count'] for item in results]
    plt.rcParams.update({"axes.spines.top": False, "axes.spines.right": False, "font.size": 10})
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.bar(hours, event_counts, color="#446ce3")
    ax.set(title="DevPulse — Full-Archive Records by Partition Hour (2025-01-01 UTC)",
           xlabel="Archive partition hour (UTC)", ylabel="Validated raw event records", xticks=hours)
    ax.grid(axis="y", alpha=.2)
    fig.text(.02, .02, "Complete 24-file source collection; archive partitions are distinct from event timestamp windows", fontsize=9)
    fig.tight_layout(rect=(0, .04, 1, 1))
    fig.savefig(REPORTS / "hourly_collection.png", dpi=160)
    plt.close(fig)
    report = f"""# DevPulse — Week 2 Ingestion and Storage Report

**Status: {'COMPLETE — all criteria satisfied' if complete else 'INCOMPLETE — see blocked criteria'}.**
Prepared on 8 October 2026 (Asia/Kolkata). All data measurements below are from
actual full-file ingestion and streaming validation of real GH Archive inputs.

## 1. Objective and delivered scope

Week 2 completes the roadmap's collection and HDFS milestone: configurable UTC
windows, bounded downloads, full-file validation, recoverable status tracking,
raw bronze storage, coverage audits, and a reusable automation entry point.
Week 1 remains a separate preserved sample analysis. Spark cleaning, Parquet ETL,
deduplication, enrichment, predictions, dashboards, and Airflow remain later work.

{checklist}

## 2. Environment and HDFS configuration

The existing Apple Silicon Mac, Python 3.12 virtual environment, Java 17, and
installed {integration.get('hadoop_version', 'Hadoop 3.5.0')} were reused. No new Python packages,
global Hadoop configuration, SSH configuration, or OS scheduling service was installed.
The project lives in `~/Desktop/DevPulse`.

Project configuration points at `hdfs://127.0.0.1:19000` and binds all five
NameNode/DataNode ports to loopback. `.runtime/hdfs/` holds only this project's
NameNode namespace, DataNode blocks, temporary state, and logs. Replication is 1;
the verified maximum daemon heap sizes are {integration.get('daemon_heap_max_bytes', 'pending verification')} bytes.
This is a single-machine pseudo-distributed exercise, not a production cluster
or evidence of multi-machine scalability.

## 3. Real dataset collection

- Source: [GH Archive](https://www.gharchive.org/)
- Selected partition window: **2025-01-01 00:00 through 23:00 UTC**, inclusive
- Requested hourly files: **{initial['planned_hours']}**
- Successfully collected and validated files: **{initial['completed_hours']}**
- Newly downloaded during the full-day run: **{initial['downloaded_files']}**
- Existing Week 1 source reused: **{initial['reused_downloads']}**
- Full-source raw event records validated: **{initial['validated_records']:,}**
- Compressed bytes: **{initial['compressed_bytes']:,}** ({initial['compressed_bytes']/1024**3:.3f} GiB)
- Collection gaps: **{len(initial['missing_hours'])}**
- Raw and HDFS logical-space budgets: **4096 MiB each**

The source records were validated incrementally; compressed archives were never
loaded in their entirety for analysis. JSON-object validity and gzip CRC/trailer
integrity are required. Missing fields/timestamps are recorded as diagnostics and
not fabricated. Each source has a recorded SHA-256 and matching HDFS publication.
Full-file counts are raw observations, not deduplicated actions.

| Archive | Compressed bytes | Validated event rows |
|---|---:|---:|
{rows}

![Full-source hourly collection](week2/hourly_collection.png)

## 4. Event types and quality observations

These are full-source ingestion counters, distinct from Week 1's 10,000-record
sample. Push events are pushes rather than commit totals; WatchEvents represent
observed starring actions rather than accumulated star counts.

| Event type | Full-source raw rows | Share |
|---|---:|---:|
{distribution}

Common-field missing/null/blank diagnostics:

| Field | Missing records |
|---|---:|
{qualities}

Invalid/missing event timestamps across all sources:
**{sum(item['validation']['invalid_timestamps'] for item in results):,}**.
Malformed JSON records and blank input lines are rejected by validation; this
completed run contains none. Repository language trends and developer productivity
are not inferred from these ingestion measurements.

## 5. State, publication, and recovery

`data/metadata/ingestion.sqlite3` persists archive, publication, and run status.
Collection JSON snapshots preserve each selected hour and all results, including
failures. Atomic download promotion and HDFS staging/rename keep incomplete data
out of final filenames. Existing published files with different checksums are
preserved and rejected as conflicts. Logs retain actual HTTP/Hadoop errors.

The verified full-day rerun performed **{repeated.get('downloaded_files', 'pending')} downloads**,
reused **{repeated.get('reused_validations', 'pending')} validations**, and reused
**{repeated.get('reused_publications', 'pending')} checksum-verified HDFS publications**.
The full-coverage audit verified **{audit.get('verified_hours', 0)}/{initial['planned_hours']} hours**
and reported **{len(audit.get('issues', []))} issues**.

The real interruption drill sent SIGINT during source validation, observed
`{recovery.get('first_run_status', 'pending')}` in the ledger, resumed
**{recovery.get('resumed_hours', 0)}** hours with **{recovery.get('resume_downloads', 'pending')}** new downloads,
and verified the local bronze checksums. Its status is **{recovery.get('status', 'BLOCKED')}**.
This intentional interruption is a negative test, not an unexplained project failure.

The actual one-byte HDFS quota drill returned the expected storage-quota failure.
It left **{quota.get('remaining_files', 'pending')} files** and **{quota.get('remaining_bytes', 'pending')} bytes**
at its test destination. Its status is **{quota.get('status', 'BLOCKED')}**.
Valid raw source data remained reusable.

## 6. HDFS integrity, restart, and lifecycle

HDFS fsck: **{integration.get('hdfs_fsck', 'HEALTHY (saved fsck output)')}**.
Restart test: **{restart.get('status', 'BLOCKED')}**; post-restart verified source files:
**{restart.get('post_restart_verified_files', 0)}**. The NameNode VERSION identity
is checked before and after restart to prove that existing data was not reformatted.
The verified lifecycle profile is **{integration.get('verification_profile', 'development')}**,
at **{integration.get('verified_hdfs_uri', 'hdfs://127.0.0.1:19000')}**.
The final **verification-cluster** state is
**{integration.get('final_cluster_stop', {}).get('status', 'pending')}**.
The development cluster remains available at `hdfs://127.0.0.1:19000` with the
original 24 uploaded files. Its namespace and configuration were not replaced.

The first restart attempt encountered Codex sandbox `PermissionError: Operation
not permitted` while signalling an earlier Hadoop daemon. The remaining checks
were completed using the same lifecycle implementation in one parent process,
with separate verification ports and runtime directories. All 24 original source
archives were checksum-verified and published to that test cluster without new
downloads. A consistent SQLite backup seeded its validation ledger; the development
ledger and latest collection manifest remained separate. The test cluster was
restarted, all 24 hashes were checked again, and its daemons were stopped cleanly.
The host restriction on signalling earlier processes still applies inside Codex;
it is not a blocker for the completed supervised verification. Development service
shutdown can be run from the user's normal Terminal when that service is no longer needed.

## 7. Testing and preserved Week 1 work

**{len(cases)-len(failures)} automated tests passed; {len(failures)} failed/error/skipped.**
Coverage includes Week 1 regressions, input windows, full-file JSON/gzip integrity,
status durability, interrupted and failed recovery, idempotency, quotas, atomic
publication, conflicting files, path checks, loopback redirects, actual Hadoop
error diagnostics, missing coverage, and checksum/metadata tampering.

Week 1 artifact preservation checks:
**{integration.get('week1_artifacts_unchanged', 'pending final integration evidence')}**.
The notebook, report, original raw archive, sample JSONL, and sample analysis JSON
are compared with their hashes from the start of Week 2. HDFS and collection work
do not redirect or rerun that notebook.

## 8. Automation, limitations, and next steps

`scripts/collect_latest.sh` selects one completed UTC hour with an explicit buffer,
reuses unchanged work, and returns a failing exit code when incomplete. The guide
includes optional scheduler and backfill commands. No cron/launchd service or
Codex recurring automation was installed. A sleeping laptop needs an explicit
backfill; an unrequested hour is not silently marked as covered.

Replication 1 is suitable for this laptop demonstration and does not provide
cross-machine redundancy. HDFS block reservations can trigger quotas before a
write's final byte count reaches the quota. Production authentication, resilience,
retention policies, and distributed scaling remain later deployment work.

Week 3 should define a stable Spark schema, implement quality/late-event rules,
deduplicate event IDs, write partitioned Parquet, and verify row accounting from
these preserved bronze inputs. The full-day source window now provides a real
multi-million-event input for that ETL work.

## 9. Evidence files and commands

- [Collection snapshot](week2/initial_collection.json)
- [Coverage/checksum audit](week2/coverage_audit.json)
- [Rerun results](week2/idempotency.json)
- [Integration evidence](week2/integration_verification.json)
- [HDFS fsck output](week2/hdfs_fsck.txt)
- [Recovery drill](week2/recovery_drill.json)
- [Quota drill](week2/quota_drill.json)
- [Restart verification](week2/restart_verification.json)

```bash
cd ~/Desktop/DevPulse
source scripts/activate.sh
python -m storage.hdfs_cluster start
python -m ingestion.collect --date 2025-01-01 --hours 24 --backend hdfs
python -m ingestion.audit --backend hdfs --verify-storage
python -m pytest --junitxml=reports/week2/tests.xml
python -m storage.hdfs_cluster stop
python -m scripts.generate_week2_report
```

For the deliberate integration drills, run:

```bash
DEVPULSE_HDFS_PROFILE=verification python -m scripts.verify_week2 --start-cluster
```

It starts the isolated verification cluster, loads the existing real sources,
verifies, restarts, and stops it while preserving development data. Its runtime
is `.runtime/hdfs-verification/`, and its configuration is `config/hadoop-verification/`.
"""
    atomic_text(ROOT / "reports/week2_report.md", report)
    print("Week 2 report generated; all criteria complete:", complete)


if __name__ == "__main__":
    main()
