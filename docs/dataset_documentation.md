# GH Archive dataset contract — Week 1

## Source and file format

[GH Archive](https://www.gharchive.org/) archives GitHub public activity as hourly
gzip-compressed JSON records. Use `https://data.gharchive.org/YYYY-MM-DD-H.json.gz`,
where `H` is an unpadded UTC hour from 0 to 23. Each decompressed line is one JSON
object, not one element of a surrounding JSON array. Events remain nested in the
raw archive and extracted sample. Archive-hour selection and event timestamps
are distinct concepts: event collection latency can produce earlier timestamps.

The inspected input is `2025-01-01-12.json.gz`. Download and sample manifests
contain file size, SHA-256, source URL, gzip validation, scan diagnostics, sampling
seed, and record counts. No synthetic events are used for its analysis. The tiny
events generated in `tests/conftest.py` exist only for offline tests.

## Important common fields

| Path | Expected JSON type | Interpretation |
|---|---|---|
| `id` | Usually string; integer accepted | Event identity; retain lossless text for quality checks |
| `type` | string | PascalCase event type |
| `actor` | object | Account responsible for the event |
| `actor.id` | integer | Stable account identifier; may be a bot |
| `actor.login` | string | Observed account name; can change |
| `repo` | object | Repository associated with the event |
| `repo.id` | integer | Stable repository identifier |
| `repo.name` | string | Observed `owner/repository` name; can change |
| `created_at` | ISO 8601 string | Event timestamp, normalized to UTC for analysis |
| `public` | boolean | Public visibility flag |
| `payload` | object | Event-specific content; never assume a universal payload schema |
| `org` | optional object | Organization context, present only when applicable |
| `actor.url`, `repo.url` | string | API links, not fetched during Week 1 |
| `actor.avatar_url`, `actor.display_login` | optional string | Display metadata, not analytical identities |

The table describes expected common semantics. The **observed** type sets and
presence/null counts are saved to `data/processed/schema_summary.json`; inferred
Spark types are in `data/processed/spark_schema.txt`. Missing paths remain missing
in the nested sample rather than being fabricated. Pandas extracts only the small
common-field projection, keeping original payloads in the JSONL input.

## Event-specific payloads

| Type | Meaning | Examples of historically observed payload paths |
|---|---|---|
| PushEvent | One push to a reference | `ref`, `head`, `before`, sometimes `size`, `commits[]` |
| WatchEvent | Starring activity | `action` (commonly `started`) |
| ForkEvent | A repository fork | `forkee` |
| IssuesEvent | Issue lifecycle activity | `action`, `issue` |
| PullRequestEvent | Pull request lifecycle activity | `action`, `number`, `pull_request` |
| CreateEvent | Repository, branch, or tag creation | `ref_type`, `ref`, `master_branch` |
| IssueCommentEvent | Issue/PR comment activity | `action`, `issue`, `comment` |
| PullRequestReviewEvent | Pull request review activity | `action`, `review`, `pull_request` |

These are examples, not required fields or an exhaustive schema. Payload fields
can evolve; do not infer commits from PushEvent counts or derive issue openings
from all IssuesEvents. Event type plus `payload.action` is needed for action-level
analytics in later weeks. Event counts can include actions such as closing or
editing. See [GitHub's event reference](https://docs.github.com/en/rest/using-the-rest-api/github-event-types).

## Quality and counting policy

- Streaming JSON validation counts invalid records and blank lines. `--strict`
  rejects malformed JSON. Non-object JSON is invalid. Truncated gzip/CRC errors
  always propagate, and download promotion requires a nonempty valid gzip stream.
- Important fields report absent, explicit null, and blank-string values separately.
  Missing-data charts combine those categories. UTC parsing also reports invalid
  or missing timestamps, which are excluded from time aggregates.
- Recursive discovery records objects, arrays, and member paths using `[]`. A
  repeated field in multiple array elements counts once per containing event for
  presence; multiple type labels within an array can each count once in that event.
- Payload-path absence is evaluated across sampled records and broken out by event
  type. Absence of `payload.forkee` on a PushEvent is expected, not corrupt data.
- Duplicate IDs exclude absent/null/empty IDs and report duplicated distinct IDs,
  all involved rows, and excess rows after the first occurrence. EDA retains
  duplicates so the diagnostic is visible. Deduplication belongs in later ETL.
- Unique repositories/accounts use IDs. Names are display labels; ranking labels
  use the latest observed nonnull name. Public account rankings use `public=true`.
- Time outputs include minute counts in the observed interval, hour-start counts,
  and UTC hour-of-day distribution. Only internal empty minute bins are filled
  with zero. Unobserved hours are not presented as measured zeros.

## Sampling and limitations

The sampler scans every source record sequentially and selects a uniform reservoir
without replacement, with seed 42 by default. Memory depends on the sample size,
not the source record count. The source order is restored within the sample.
Repeatability requires identical input bytes, input ordering, seed, and sample size.
Its 10,000 records are not the complete hour; the full-source count is separately
reported in sample metadata. A type absent from the sample may exist in the source.

Public events exclude private work and do not provide a full account-productivity
measure. Bot activity can dominate rankings. Sampling and selecting one holiday
hour introduce coverage and selection limitations. WatchEvents are observed
starring actions, not historical cumulative star counts. Repository language is
not consistently available across events; later GitHub API enrichment must record
metadata collection time and rate-limit behavior. GH Archive history spans API
changes, so future windows need schema checks before reuse of transformations.

GitHub describes its public event API as having collection latency; see
[the official Events API documentation](https://docs.github.com/en/rest/activity/events).
Archive partition hours should therefore never replace `created_at` in time analysis.
