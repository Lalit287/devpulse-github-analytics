# DevPulse — Week 1 Project Report

**Status: COMPLETE — all criteria satisfied.**
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

- [x] DONE: Project structure created
- [x] DONE: Python environment configured
- [x] DONE: PySpark works locally
- [x] DONE: Downloader works
- [x] DONE: Real dataset downloaded
- [x] DONE: Dataset schema inspected
- [x] DONE: Nested sample generated
- [x] DONE: Exploratory analysis works
- [x] DONE: Visualizations generated
- [x] DONE: Notebook created
- [x] DONE: Notebook executed successfully
- [x] DONE: Automated tests executed and passed
- [x] DONE: README complete
- [x] DONE: Dataset documentation complete
- [x] DONE: Week 1 report generated
- [x] DONE: Final artifact audit passed

## 3. Technologies Used

Python with PySpark, Pandas, NumPy, Matplotlib, Requests, JupyterLab, nbclient,
and Pytest. Exact installed direct versions:

- pyspark: 4.0.1
- pandas: 2.3.3
- numpy: 2.2.6
- matplotlib: 3.10.7
- jupyterlab: 4.4.10
- requests: 2.32.5
- pytest: 8.4.2
- nbclient: 0.10.2

Hadoop services, Kafka, Airflow, PostgreSQL, Docker, ML models, and dashboards
are future stages and were not installed or implemented for Week 1.

## 4. Environment Configuration

- OS: macOS-27.0.1-arm64-arm-64bit
- Architecture: arm64
- Python: 3.12.14 (main, Aug 25 2026, 13:50:33) [Clang 22.1.3 ]
- Isolated environment: True
- Runtime executable: /Users/lalitaditya/Desktop/DevPulse/.venv/bin/python
- Java home: /opt/homebrew/opt/openjdk@17
- Java: openjdk version "17.0.20.1" 2026-08-18
- Physical memory: 24.0 GiB

The project and its runtime are in `~/Desktop/DevPulse`, as requested. No global
system configuration was changed. Local activation sets project-only Jupyter
paths, Python worker selection, and loopback Spark networking. The macOS setup
guide records installation commands, and environment.json retains runtime details.

## 5. Dataset Description

[GH Archive](https://www.gharchive.org/) supplies hourly gzip NDJSON of public
GitHub events. The downloaded archive partition is 2025-01-01 12:00–13:00 UTC.
Payloads remain nested and event-specific; all sampled types are listed below.

## 6. Dataset Collection Method

- Real source files downloaded: **1**
- Total compressed dataset bytes: **76,173,404** (72.64 MiB)
- Total uncompressed bytes validated: **551,466,045**
- Valid full-source records scanned: **223,540**
- Source invalid JSON records: **0**
- Source blank lines: **0**
- Sample method: uniform reservoir without replacement, retained in source order
- Random seed: **42**
- Sample analyzed: **10,000 records**

Downloads stream with timeout/retry handling, enforce a default 500 MiB raw
quota, validate CRC through gzip EOF, and publish atomically. Only one hourly
archive was downloaded. Completed files are reused only after validation.

| File | Compressed bytes | SHA-256 |
|---|---|---|
| 2025-01-01-12.json.gz | 76,173,404 | db6cbf53e3c9d40590a45414d42869d22b6fe0f9faa20fe6bfc65be085a99e3e |

## 7. Schema Exploration

Incremental parsing discovered common fields `id`, `type`, `actor`, `repo`,
`created_at`, `public`, and `payload`, plus optional nested paths such as
`org`. A seeded reservoir scans the whole archive while retaining at most
10,000 event objects. Field presence, nulls, types, examples, array member paths,
and event-specific payload paths are saved in schema_summary.json.

## 8. Exploratory Data Analysis

**Scope: sample; origin: real GH Archive.**

| Measured sample metric | Value |
|---|---:|
| Analyzed event records | 10,000 |
| Unique repository IDs | 8,247 |
| Unique account IDs | 3,738 |
| Unique public account IDs | 3,738 |
| Distinct duplicated event IDs | 0 |
| Excess duplicated-ID rows | 0 |
| Rows involved in duplicated IDs | 0 |
| Invalid or missing timestamps | 0 |

Sample timestamp range: **2025-01-01T12:00:00+00:00** to
**2025-01-01T12:59:59+00:00**. Observed UTC hours:
**[12]**. The range comes from actual created_at values,
not assumed archive coverage. Minute and hour CSVs contain the measured counts.

| Event type | Sample count | Sample share |
|---|---:|---:|
| PushEvent | 7,585 | 75.85% |
| CreateEvent | 812 | 8.12% |
| PullRequestEvent | 495 | 4.95% |
| WatchEvent | 396 | 3.96% |
| IssueCommentEvent | 225 | 2.25% |
| DeleteEvent | 187 | 1.87% |
| ForkEvent | 72 | 0.72% |
| PullRequestReviewEvent | 67 | 0.67% |
| IssuesEvent | 66 | 0.66% |
| ReleaseEvent | 27 | 0.27% |
| PullRequestReviewCommentEvent | 24 | 0.24% |
| PublicEvent | 15 | 0.15% |
| GollumEvent | 13 | 0.13% |
| MemberEvent | 9 | 0.09% |
| CommitCommentEvent | 7 | 0.07% |

Top sampled repositories (stable-ID groups):

| Repository | Events |
|---|---:|
| brand22/d3 | 86 |
| frdpzk2/ppub | 74 |
| frdpzk3/ppub | 49 |
| CelestiaNFT/Welcome-NFT | 47 |
| iniadittt/iniadittt | 34 |

Top sampled public accounts (includes bots):

| Public account | Events |
|---|---:|
| github-actions[bot] | 3966 |
| dependabot[bot] | 274 |
| swa-runner-app[bot] | 155 |
| Hall-1910 | 86 |
| frdpzk2 | 74 |

Common-field quality:

| Field | Absent | Null | Blank | Missing share |
|---|---:|---:|---:|---:|
| id | 0 | 0 | 0 | 0.00% |
| type | 0 | 0 | 0 | 0.00% |
| actor.id | 0 | 0 | 0 | 0.00% |
| actor.login | 0 | 0 | 0 | 0.00% |
| repo.id | 0 | 0 | 0 | 0.00% |
| repo.name | 0 | 0 | 0 | 0.00% |
| created_at | 0 | 0 | 0 | 0.00% |
| public | 0 | 0 | 0 | 0.00% |
| payload | 0 | 0 | 0 | 0.00% |

Counts retain raw events rather than silently deduplicating. Unique entities use
IDs and rankings use observed names. These are observations from a sample, not
GitHub-wide totals or measurements of current cumulative stars.

## 9. Visualizations

Seven Matplotlib PNGs use consistent formatting, descriptive labels, and measured
sample counts. The time chart covers the actual observed interval; UTC-hour
distribution covers observed hours only.

- [event_type_distribution.png](../visualizations/event_type_distribution.png)
- [top_repositories.png](../visualizations/top_repositories.png)
- [developer_activity_distribution.png](../visualizations/developer_activity_distribution.png)
- [events_over_time.png](../visualizations/events_over_time.png)
- [missing_data.png](../visualizations/missing_data.png)
- [event_type_comparison.png](../visualizations/event_type_comparison.png)
- [utc_hour_distribution.png](../visualizations/utc_hour_distribution.png)

## 10. PySpark Implementation

Spark status: **passed**, version
**4.0.1**, master **local[2]**.
The Python-created DataFrame smoke sum is **6**.
Spark read **10,000** sample records, observed
**8,247** repository IDs and
**3,738** actor IDs.

The notebook asserts parity with Pandas for row count, event-type counts, and
distinct repository/actor IDs. The inferred schema, selected transformed records,
and aggregation results are retained. SparkSession is stopped in a finally block.
This demonstrates local Spark, not a multi-machine cluster or a claimed speedup.

Notebook: **passed**, **10**
code cells executed in **7.15 seconds**.
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
and seed 42. The sample contains 8,247 observed
repositories and 3,738 accounts. Push, star, fork,
pull-request, and issue event counts are in the distribution table; none is
interpreted as a cumulative repository statistic. One holiday-hour partition
cannot establish daily trends or language popularity.

Automated tests: **39 passed**, **0 failed/error/skipped**.
Coverage includes filename/date/hour handling, retries, HTTP errors, quota enforcement,
gzip corruption, JSON parsing, empty input, missing fields, large identifiers,
duplicates, event aggregation, schema extraction, deterministic sampling,
UTC normalization, chart generation, and sample-provenance integrity.
Final saved-artifact audit: **passed**.

## 13. Conclusion

All Week 1 completion criteria are satisfied with real execution evidence.
Results, tests, provenance, and the executed notebook are preserved with the
source code. Later-week features are intentionally left on the roadmap.

## 14. Week 2 Plan

Extend controlled date-window ingestion with per-hour status and collection
coverage auditing; design restart/recovery workflows; set up HDFS/storage only
after assessing the laptop budget; retain a local-storage option. Plan validated
raw storage and eventual Parquet partitions. Full Spark ETL belongs in Week 3.
