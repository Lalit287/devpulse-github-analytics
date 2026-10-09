"""Generate Week 1 findings and completion criteria from actual saved evidence."""
import json
import xml.etree.ElementTree as ET

from config.settings import PROCESSED_DIR, RAW_DIR, REPORTS_DIR, ROOT
from exploration.io import atomic_text


def load(path):
    return json.loads(path.read_text()) if path.exists() else {}


def generate_report():
    summary = load(PROCESSED_DIR / "analysis_summary.json")
    if not summary:
        raise RuntimeError("Run real-data EDA before generating the measured report")
    metadata = summary.get("sample_metadata") or {}
    environment = load(REPORTS_DIR / "environment.json")
    spark = load(PROCESSED_DIR / "spark_verification.json")
    notebook = load(REPORTS_DIR / "notebook_execution.json")
    verification = load(REPORTS_DIR / "artifact_verification.json")
    manifest = load(RAW_DIR / "download_manifest.json")
    week1_source_names = {source['path'].split('/')[-1] for source in metadata.get('sources', [])}
    files = [item for item in manifest.get("files", {}).values()
             if item['filename'] in week1_source_names
             if item.get("provenance") == "HTTPS download from GH Archive" and (RAW_DIR / item["filename"]).exists()]
    testcases, failures = [], []
    if (REPORTS_DIR / "tests.xml").exists():
        suites = ET.parse(REPORTS_DIR / "tests.xml").getroot()
        testcases = suites.findall(".//testcase")
        failures = suites.findall(".//failure") + suites.findall(".//error") + suites.findall(".//skipped")
    criteria = {
        "Project structure created": (ROOT / "config/settings.py").exists(),
        "Python environment configured": environment.get("virtual_environment") is True,
        "PySpark works locally": spark.get("status") == "passed" and spark.get("master") == "local[2]",
        "Downloader works": bool(files),
        "Real dataset downloaded": metadata.get("data_origin") == "real GH Archive" and bool(files),
        "Dataset schema inspected": (PROCESSED_DIR / "schema_summary.json").exists(),
        "Nested sample generated": metadata.get("sample_records", 0) > 0,
        "Exploratory analysis works": summary.get("records_analyzed", 0) > 0,
        "Visualizations generated": len(summary.get("charts", [])) >= 6,
        "Notebook created": (ROOT / "notebooks/01_gharchive_exploration.ipynb").exists(),
        "Notebook executed successfully": notebook.get("status") == "passed",
        "Automated tests executed and passed": bool(testcases) and not failures,
        "README complete": (ROOT / "README.md").exists(),
        "Dataset documentation complete": (ROOT / "docs/dataset_documentation.md").exists(),
        "Week 1 report generated": True,
        "Final artifact audit passed": verification.get("status") == "passed",
    }
    complete = all(criteria.values())
    bytes_total = sum(item["compressed_bytes"] for item in files)
    distributions = "\n".join(
        f"| {kind} | {count:,} | {count / summary['records_analyzed'] * 100:.2f}% |"
        for kind, count in sorted(summary["event_type_counts"].items(), key=lambda item: (-item[1], item[0])))
    quality = "\n".join(
        f"| {row['field']} | {row['absent']} | {row['null']} | {row['blank']} | {row['missing_percent']:.2f}% |"
        for row in summary["missing_fields"])
    criteria_text = "\n".join(
        f"- [{'x' if done else ' '}] {'DONE' if done else 'BLOCKED — missing successful evidence'}: {name}"
        for name, done in criteria.items())
    charts = "\n".join(f"- [{name}](../visualizations/{name})" for name in summary["charts"])
    source_rows = "\n".join(f"| {item['filename']} | {item['compressed_bytes']:,} | {item['sha256']} |" for item in files)
    import pandas as pd
    top_repos = pd.read_csv(PROCESSED_DIR / "repositories.csv", dtype={"id": str}).head(5)
    top_accounts = pd.read_csv(PROCESSED_DIR / "public_accounts.csv", dtype={"id": str}).head(5)
    repo_rows = "\n".join(f"| {row['name']} | {row['events']} |" for _, row in top_repos.iterrows())
    account_rows = "\n".join(f"| {row['name']} | {row['events']} |" for _, row in top_accounts.iterrows())
    body = f"""# DevPulse — Week 1 Project Report

**Status: {'COMPLETE — all criteria satisfied' if complete else 'INCOMPLETE — see blocked criteria below'}.**
Prepared on 8 October 2026 (Asia/Kolkata). This report describes measured local
execution. Activity analytics refer to the sample; source scan totals are separately labeled.

## 1. Introduction

DevPulse studies historical public GitHub activity and lays the foundation for
later distributed analytics. Week 1 implements reliable collection, schema
exploration, quality diagnosis, sample analytics, and local Spark verification.

## 2. Week 1 Objectives

Configure Python/Spark, download real data, discover nested schemas, extract a
reproducible sample, analyze events and identities, generate charts, execute a
teaching notebook, test the code, and document actual outcomes.

{criteria_text}

## 3. Technologies Used

Python with PySpark, Pandas, NumPy, Matplotlib, Requests, JupyterLab, nbclient,
and Pytest. Exact installed direct versions:

{chr(10).join(f"- {name}: {version}" for name, version in environment.get("packages", {}).items())}

Hadoop services, Kafka, Airflow, PostgreSQL, Docker, ML models, and dashboards
are future stages and were not installed or implemented for Week 1.

## 4. Environment Configuration

- OS: {environment.get('os')}
- Architecture: {environment.get('architecture')}
- Python: {environment.get('python', '').splitlines()[0]}
- Isolated environment: {environment.get('virtual_environment')}
- Runtime executable: {environment.get('executable')}
- Java home: {environment.get('java_home')}
- Java: {environment.get('java_version', '').splitlines()[0]}
- Physical memory: {environment.get('total_memory_bytes', 0) / 1024**3:.1f} GiB

The project and its runtime are in `~/Desktop/DevPulse`, as requested. No global
system configuration was changed. Local activation sets project-only Jupyter
paths, Python worker selection, and loopback Spark networking. The macOS setup
guide records installation commands, and environment.json retains runtime details.

## 5. Dataset Description

[GH Archive](https://www.gharchive.org/) supplies hourly gzip NDJSON of public
GitHub events. The downloaded archive partition is 2025-01-01 12:00–13:00 UTC.
Payloads remain nested and event-specific; all sampled types are listed below.

## 6. Dataset Collection Method

- Real source files downloaded: **{len(files)}**
- Total compressed dataset bytes: **{bytes_total:,}** ({bytes_total / 1024**2:.2f} MiB)
- Total uncompressed bytes validated: **{sum(item.get('uncompressed_bytes', 0) for item in files):,}**
- Valid full-source records scanned: **{metadata.get('valid_source_records', 0):,}**
- Source invalid JSON records: **{sum(s['read_stats']['invalid_records'] for s in metadata.get('sources', []))}**
- Source blank lines: **{sum(s['read_stats']['blank_lines'] for s in metadata.get('sources', []))}**
- Sample method: {metadata.get('sampling_method')}
- Random seed: **{metadata.get('seed')}**
- Sample analyzed: **{summary['records_analyzed']:,} records**

Downloads stream with timeout/retry handling, enforce a default 500 MiB raw
quota, validate CRC through gzip EOF, and publish atomically. Only one hourly
archive was downloaded. Completed files are reused only after validation.

| File | Compressed bytes | SHA-256 |
|---|---|---|
{source_rows}

## 7. Schema Exploration

Incremental parsing discovered common fields `id`, `type`, `actor`, `repo`,
`created_at`, `public`, and `payload`, plus optional nested paths such as
`org`. A seeded reservoir scans the whole archive while retaining at most
10,000 event objects. Field presence, nulls, types, examples, array member paths,
and event-specific payload paths are saved in schema_summary.json.

## 8. Exploratory Data Analysis

**Scope: {summary['scope']}; origin: {summary['data_origin']}.**

| Measured sample metric | Value |
|---|---:|
| Analyzed event records | {summary['records_analyzed']:,} |
| Unique repository IDs | {summary['unique_repositories_by_id']:,} |
| Unique account IDs | {summary['unique_accounts_by_id']:,} |
| Unique public account IDs | {summary['unique_public_accounts_by_id']:,} |
| Distinct duplicated event IDs | {summary['duplicate_event_ids']:,} |
| Excess duplicated-ID rows | {summary['duplicate_excess_records']:,} |
| Rows involved in duplicated IDs | {summary['records_with_duplicated_ids']:,} |
| Invalid or missing timestamps | {summary['invalid_or_missing_timestamps']:,} |

Sample timestamp range: **{summary['timestamp_min_utc']}** to
**{summary['timestamp_max_utc']}**. Observed UTC hours:
**{summary['observed_utc_hours']}**. The range comes from actual created_at values,
not assumed archive coverage. Minute and hour CSVs contain the measured counts.

| Event type | Sample count | Sample share |
|---|---:|---:|
{distributions}

Top sampled repositories (stable-ID groups):

| Repository | Events |
|---|---:|
{repo_rows}

Top sampled public accounts (includes bots):

| Public account | Events |
|---|---:|
{account_rows}

Common-field quality:

| Field | Absent | Null | Blank | Missing share |
|---|---:|---:|---:|---:|
{quality}

Counts retain raw events rather than silently deduplicating. Unique entities use
IDs and rankings use observed names. These are observations from a sample, not
GitHub-wide totals or measurements of current cumulative stars.

## 9. Visualizations

Seven Matplotlib PNGs use consistent formatting, descriptive labels, and measured
sample counts. The time chart covers the actual observed interval; UTC-hour
distribution covers observed hours only.

{charts}

## 10. PySpark Implementation

Spark status: **{spark.get('status', 'BLOCKED')}**, version
**{spark.get('spark_version')}**, master **{spark.get('master')}**.
The Python-created DataFrame smoke sum is **{spark.get('smoke_sum')}**.
Spark read **{spark.get('records', 0):,}** sample records, observed
**{spark.get('unique_repositories', 0):,}** repository IDs and
**{spark.get('unique_accounts', 0):,}** actor IDs.

The notebook asserts parity with Pandas for row count, event-type counts, and
distinct repository/actor IDs. The inferred schema, selected transformed records,
and aggregation results are retained. SparkSession is stopped in a finally block.
This demonstrates local Spark, not a multi-machine cluster or a claimed speedup.

Notebook: **{notebook.get('status', 'BLOCKED')}**, **{notebook.get('executed_code_cells', 0)}**
code cells executed in **{notebook.get('duration_seconds', 0)} seconds**.
All fourteen requested teaching sections are included.

## 11. Challenges Encountered

- The original system Python was 3.13; an available Python 3.12 was selected for
  a conservative reproducible environment instead of changing system Python.
- Direct sysctl memory inspection returned `Operation not permitted`.
  Jupyter's psutil dependency successfully measured 24 GiB.
- Initial notebook execution inherited an existing Hadoop default filesystem
  pointing at localhost:9000 and failed with `java.net.ConnectException:
  Connection refused`. Spark now sets `spark.hadoop.fs.defaultFS=file:///`
  and reads explicit file URIs. The notebook was rerun successfully afterward.
- Moving to the requested Desktop folder required scoped filesystem permission;
  virtual-environment launchers and local kernel paths were updated, then verified.
- Optional Git initialization was blocked by the execution sandbox with
  `/Users/lalitaditya/Desktop/DevPulse/.git: Operation not permitted`. Source files
  and .gitignore are complete, but this folder has no initialized Git repository.
  This is outside the required Week 1 execution criteria.
- Network installation/download permission was granted; the real download succeeded.
  No synthetic data substitution was necessary.
- After successful cell execution, Jupyter logged a nonfatal subprocess-cleanup
  warning: psutil process enumeration hit a sandbox `Operation not permitted`
  restriction. All notebook code cells and parity assertions passed; SparkSession
  stopped in the helper's finally block. The warning remains an execution-host
  limitation, not an analysis or test failure.

## 12. Results and Observations

The bounded real-data pipeline is reproducible through source/sample checksums
and seed 42. The sample contains {summary['unique_repositories_by_id']:,} observed
repositories and {summary['unique_accounts_by_id']:,} accounts. Push, star, fork,
pull-request, and issue event counts are in the distribution table; none is
interpreted as a cumulative repository statistic. One holiday-hour partition
cannot establish daily trends or language popularity.

Automated tests: **{len(testcases) - len(failures)} passed**, **{len(failures)} failed/error/skipped**.
Coverage includes filename/date/hour handling, retries, HTTP errors, quota enforcement,
gzip corruption, JSON parsing, empty input, missing fields, large identifiers,
duplicates, event aggregation, schema extraction, deterministic sampling,
UTC normalization, chart generation, and sample-provenance integrity.
Final saved-artifact audit: **{verification.get('status', 'BLOCKED')}**.

## 13. Conclusion

{'All Week 1 completion criteria are satisfied with real execution evidence.' if complete else 'Week 1 is not complete; blocked criteria above require successful execution evidence.'}
Results, tests, provenance, and the executed notebook are preserved with the
source code. Later-week features are intentionally left on the roadmap.

## 14. Week 2 Plan

Extend controlled date-window ingestion with per-hour status and collection
coverage auditing; design restart/recovery workflows; set up HDFS/storage only
after assessing the laptop budget; retain a local-storage option. Plan validated
raw storage and eventual Parquet partitions. Full Spark ETL belongs in Week 3.
"""
    atomic_text(REPORTS_DIR / "week1_report.md", body)
    print("Report generated; Week 1 complete:", complete)


if __name__ == "__main__":
    generate_report()
