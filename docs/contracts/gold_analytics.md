# Gold analytics — `devpulse-analytics-v1`

Each complete snapshot has an immutable checksum inventory, recipe, source Silver
ID, equal-window definition, table row counts, metadata coverage, and readback checks.
The `_CURRENT.json` pointer stores its snapshot ID and manifest SHA-256. Table
schema is carried by Parquet; names below are directory roots to read with Spark.

| Table | Unique key | Meaning |
|---|---|---|
| `repository_metrics` | repo_id | Exact whole-window counts, distinct participants, equal-period fields and scores |
| `repository_daily` | event_date, repo_id | Daily counts and exact daily participants |
| `repository_hourly` | event_date, event_hour, repo_id | Sparse hourly repository metrics |
| `account_metrics` | actor_id | Observed events, exact distinct repositories and days, action-specific counts |
| `account_daily` | event_date, actor_id | Exact account/day counts and repository participation |
| `account_hourly` | event_date, event_hour, actor_id | Sparse account/hour metrics |
| `activity_calendar` | event_date | Global daily events, exact account/repository IDs, UTC calendar labels |
| `hourly_activity` | event_date, event_hour | Dense covered-hour activity, including zero hours |
| `participation_edges_sample` | repo_id, actor_id | Top 20 star-active repositories × at most 50 accounts; public-event edge weights |
| `repository_metadata` | repo_id | Selected current API metadata, identity/status, collection times, current name, age context |
| `repository_language_shares` | repo_id, language | Selected code bytes and allocation shares, including unavailable/no-language fallbacks |
| `language_primary_daily` | event_date, primary_language, enrichment_status | Additive event counts and exact, overlapping account/repository IDs by current language association |
| `language_weighted_daily` | event_date, language | Fractional activity allocated by current repository code-byte shares |
| `technology_daily` | event_date, technology_category | Overlapping topic/description-based associations |
| `language_intraday_growth` | primary_language, enrichment_status | Equal-period event/attention totals associated with current primary languages |

IDs and event counts are signed 64-bit integers, date is Spark `date`, hour is `int`,
scores/ratios/weighted activity are `double`, and flags are boolean. Event times are
UTC Spark timestamps stored as Parquet microseconds. Optional metadata/percentage
changes may be null. Topic/category fields are arrays of strings; metadata fetch
times retain ISO UTC strings. Per-table keys must be unique. Repository/account,
daily/calendar, and primary-language event counts conserve clean event rows.
Weighted allocated counts conserve clean events within floating-point tolerance.
Technology events can occur in multiple categories and are not globally additive.

Rows with no selected metadata keep an explicit `Not enriched` language bucket and
`not_selected` status. A selected unavailable response becomes `Metadata unavailable`
with its actual API status; successful metadata with null primary language becomes
`No primary language reported`. A successful empty language-byte map allocates to
`No language reported`. No name-based inference fills missing programming languages.

Repository labels and public-account logins are latest observed event labels; API
current names are separate. Activity includes bots. Push records do not count commits,
star events do not measure current star inventory, and public activity does not measure
total developer work. Ranking scores are exploratory; intraday growth on a single
day does not demonstrate weekly trends. Current metadata has a separate collection
time and must not be backdated into historical model features.
