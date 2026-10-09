# Week 4 — Repository, account, and language analytics

All project files are under `~/Desktop/DevPulse`. Run these commands from that
folder after `source scripts/activate.sh`. The Week 4 pipeline reads the verified
current Silver snapshot; no raw download or HDFS service is needed for this phase.

```bash
python -m analytics.pipeline
python -m scripts.verify_week4
python -m scripts.generate_week4_report
```

The default run report is `reports/week4/latest_run.json`. Saved `initial_run.json`,
`idempotent_rerun.json`, and `enrichment_run.json` are measured execution evidence.
The verifier checks that measured day and its retained metadata cache. An independent
experiment can use `--silver-root`, `--core-root`, `--gold-root`, `--cache-dir`, and
`--run-report` to keep separate inputs, caches, output, and evidence.

## Dataset reader and comparison

Gold contains 15 Parquet tables. Resolve the checksum-protected pointer before
reading. This verifies the file inventory and manifest rather than guessing a path:

```python
from config.settings import ROOT
from spark.snapshots import current_manifest
from analytics.session import create_analytics_spark

root = ROOT / "data/gold"
manifest = current_manifest(root)
snapshot = root / "snapshots" / manifest["snapshot_id"]
spark = create_analytics_spark()
try:
    repositories = spark.read.parquet((snapshot / "repository_metrics").as_uri())
    accounts = spark.read.parquet((snapshot / "account_metrics").as_uri())
    languages = spark.read.parquet((snapshot / "language_primary_daily").as_uri())
    repositories.orderBy(repositories.total_events.desc(), "repo_id").show(20)
finally:
    spark.stop()
```

Use stable repository IDs to compare two or more repositories:

```bash
python -m analytics.query --repo-id REPOSITORY_ID_A REPOSITORY_ID_B
```

The report contains actual IDs. A missing ID or fewer than two distinct IDs fails
explicitly. This is a command-line comparison; the interactive dashboard is Week 5.

`rankings.json` has bounded top-50 views for overall activity, starring activity,
intraday scores, fastest-growing attention, and emerging attention candidates.
`top_accounts.json` has 50 accounts ordered by observed events. Neither is a
productivity measure. Bots and automation can dominate these counts.

## Metric rules

Repository and account identities come from IDs, with the latest event-time label
breaking timestamp ties by raw-record SHA-256. Names can change. Global distinct
counts are calculated across the entire event window; daily distinct counts are
never summed to infer global unique participation.

- `star_events`: WatchEvent with action `started`.
- `push_events`: PushEvent records, not individual commits.
- `pull_requests_opened`, `issues_opened`: event action `opened` only.
- `active_accounts`: distinct actor IDs with any observed event.
- `code_participating_accounts`: distinct actor IDs with PushEvent, an opened PR,
  or PullRequestReviewEvent. This operational definition does not prove authorship.
- `review_events`: PullRequestReviewEvent and PullRequestReviewCommentEvent.
- `participation_ratio`: active accounts divided by total events, a proposed
  descriptive ratio rather than a standard collaboration metric.

The participation sample contains the 20 repositories with most observed star
events, retaining their 50 most active account/repository edges. The edge weight
counts public events. It is a bounded bipartite participation sample, not a complete
social network, evidence of direct account-to-account collaboration, or a community
detection result.

Daily, hourly, and calendar tables use UTC. Repository/account hourly tables contain
only observed groups. Global `hourly_activity` includes zero-event covered hours.
Calendar rows store weekday/weekend and Monday week start. One Wednesday does not
support a weekend comparison or weekly trend claim.

## Equal-period attention and scores

The complete measured day is split into equal half-open windows:
`[2025-01-01 00:00, 12:00)` and `[2025-01-01 12:00, 2025-01-02 00:00)`, UTC.
The planner requires contiguous coverage and an even number of at least two archive
hours. Events outside the archive coverage remain in overall metrics but are
excluded from window comparisons, with that count recorded in the core manifest.

Attention is stars plus forks. Smoothed growth is `(current + 1) / (previous + 1)`;
the star-only ratio is also saved. Absolute delta accompanies growth. Percentage
change is null for a zero baseline, which has an explicit flag. Comparisons qualify
for ranked growth views at five current attention events and two current accounts.
The fastest-growth view additionally requires positive attention delta. An emerging
attention candidate has at most two previous attention events and qualifies for
current support; it is not necessarily a newly created repository.

The activity score normalizes four current metrics using `log1p(metric) / log1p(max)`
across this dataset, with a zero maximum mapped to zero. Stars, forks, active
accounts, and opened PRs have weights 0.4, 0.3, 0.2, 0.1. The trend score is
`0.7 * activity_score + 0.3 * log(clip(attention_growth_ratio, 1, 10)) / log(10)`.
Scores lie in [0, 1] and rankings break ties by repository ID. These are exploratory
design choices, not a validated prediction model. The audit checks balanced,
star-heavy, and participation-heavy alternatives and saves top-20 overlap.

## Public GitHub metadata and cache

Selection is reproducible: up to ten repositories by star activity, six by intraday
score, and four by overall activity, deduplicated and filled from star activity to
20 repositories. This targeted subset is not a representative GitHub sample.

The client uses GitHub REST API version `2022-11-28`, repository metadata, and
the languages endpoint. The returned numeric repository ID must match the archived
ID before language collection. A reused owner/name cannot silently relabel an old
repository. Current full name and archived event name are stored separately.
Private metadata is not retained. HTTPS redirects are restricted to `api.github.com`.

`data/metadata/github_repositories/ID.json` stores checksummed raw API bodies,
ETags, fetch and validation times, normalized metadata, language bytes, and status.
Successful cache entries have a 30-day TTL; not-found/identity/schema outcomes have
a 24-hour TTL. Rate-limit outcomes expire at the reported reset (or a one-minute
fallback); transient network/server errors have a five-minute TTL. Per-invocation
request-budget outcomes do not prevent the next invocation from trying again.
Expired entries use conditional requests where possible. A 304 retains the original
body fetch timestamp and records revalidation. Corrupt cache data fails explicitly.
`metadata_snapshot.json` pins exact cache contents in each immutable Gold snapshot.

The client caps actual HTTP requests, including redirects and retries, at 50 by
default and reserves five remaining API requests. It stops on rate limiting, records
the reset/retry information, and preserves partial outcomes. It does not sleep until
an hourly quota resets. The optional `GITHUB_TOKEN` environment variable enables
authenticated public requests and is never written to artifacts or logs.

GitHub documents an unauthenticated primary quota of 60 requests per hour;
other activity sharing that IP can consume it. The measured enrichment made 37
requests and obtained 16 complete records, three not-found outcomes, and one
identity mismatch. Subsequent measured runs used all 20 cache records with zero
requests. [GitHub rate-limit documentation](https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api).

## Language and technology interpretation

Primary-language tables associate archived activity with **current** repository
metadata. They retain `not_selected`, unavailable metadata, and missing primary
language as separate buckets. Language-specific active account counts are exact,
but an account can participate in several languages, so those counts are not additive.

Language statistics are code bytes. The weighted view allocates repository event
totals in proportion to the current byte mix; it estimates associations rather than
observing the language of each event. Every repository has allocation shares summing
to one. Unknown/unavailable/no-language buckets preserve complete event accounting.
[GitHub repository/languages documentation](https://docs.github.com/en/rest/repos/repos?apiVersion=2022-11-28#list-repository-languages).

Technology rules use exact case-insensitive topics and bounded description phrases
for AI, cloud, data engineering, and mobile. See `analytics/languages.py` for the
versioned rules. Categories can overlap; technology totals must not be summed into
a global event count. Unclassified and unenriched buckets remain visible.

Metadata covers 63,615 events (1.63%); a named current primary language covers
9,213 events (0.24%) in this day. Repository creation time can provide an age at
the event-window end when the verified ID matches. Current star inventories,
current languages, and current topics must not be treated as historical observations
or fed into a historical prediction as though available at its cutoff.

## Publication, resources, and validation

Core aggregates cache in `work/week4/core`; complete Gold snapshots are under
`data/gold/snapshots`. The core cache avoids repeated full aggregation while metadata
is collected or refreshed. Each recipe includes code, source-manifest, and pinned
metadata hashes. A matching invocation verifies inventories and reuses output without
Spark computation. Writer locks, staging, readback, checksums, atomic rename, and
atomic current-pointer replacement preserve prior snapshots on failure. A process
crash can leave unreferenced staging data for inspection.

Default Spark is local[4] with a 4 GiB driver and 64 shuffle partitions. A 2 GiB Gold
quota covers retained snapshots and staging; core output has a separate 2 GiB quota.
Spark scratch space is additional. This is a single-machine run, not a distributed
performance benchmark. Main output comprises 15 Parquet tables and bounded JSON
previews, plus a separate measured report and reviewed PNG charts.

```bash
python -m pytest tests/test_week4_analytics.py tests/test_week4_enrichment.py tests/test_week4_publication.py -q --junitxml=reports/week4/analytics_tests.xml
python -m pytest tests/test_download.py tests/test_exploration.py tests/test_week2_ingestion.py tests/test_week2_storage.py tests/test_week3_etl.py -q --junitxml=reports/week4/previous_week_regression.xml
```

The integration audit checks table keys and totals, every Week 3 daily repository
metric row, global account/repository pair counts, equal-window counters, cache
identities, allocation totals, rerun reuse, alternate ranking weights, and retained
Week 1–3 artifacts. Read [the table contract](contracts/gold_analytics.md) and
[the measured report](../reports/week4_report.md).
