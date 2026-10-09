# Silver events — `devpulse-silver-v1`

The machine-readable Spark schema is `silver_schema.json`. Parquet may mark fields
nullable on read; required values are enforced by validation before publication.

| Fields | Type | Meaning |
|---|---|---|
| `event_id` | string | Positive decimal event identifier; stays a string to avoid precision loss |
| `event_type` | string | GitHub event name; unknown names matching the event-name pattern are retained |
| `actor_id`, `repo_id` | long | Required positive signed 64-bit identifiers |
| `actor_login`, `repo_name` | string | Nonempty login and owner/repository label observed in this event |
| `event_time` | timestamp | Parsed instant, Spark session UTC, stored at microsecond precision |
| `public` | boolean | Required true for the public activity dataset |
| `org_id`, `org_login` | long, string | Optional organization; invalid convertible-ID values warn and become null |
| `payload_json` | string | Full variable nested payload as JSON text; object key order/whitespace are not a contract |
| `payload_action`, `payload_ref`, `payload_ref_type` | string | Nullable common payload attributes; interpreted with event type |
| `event_date`, `event_hour` | date, int | Actual UTC event partition keys, hour 0–23 |
| `source_uri`, `source_filename` | string | Verified archive provenance |
| `source_archive_date`, `source_archive_hour`, `source_archive_start` | string, int, timestamp | Nominal archive UTC period |
| `archive_hour_offset_seconds` | long | Difference from archive start; out-of-window values warn |
| `raw_record_sha256` | string | Hash of the original decoded line, excluding newline |
| `quality_warnings` | array of string | Nonfatal schema-evolution, late-event, or optional-ID warnings |

Required validation: positive decimal event ID; event type pattern
`^[A-Z][A-Za-z0-9]*Event$`; positive actor/repository IDs with lossless integer
conversion; nonblank actor login; repository name with one slash and nonblank
components; parseable ISO timestamp including `Z` or a numeric offset; public true;
nonnull object payload; no schema parse error. Fractional seconds beyond six digits
are truncated to Spark/Parquet microseconds. Source filenames come from the verified
collection planner. Lines above 8 MiB are rejected by ingestion; the ETL also labels
them invalid if encountered by direct test fixtures.

Deduplication runs after validation. Winner order: earliest source archive start,
lexical source filename, raw-record SHA-256. A duplicate-ID group with differing line
hashes is counted as a content variant, which may include formatting differences;
it is not automatically classified as a semantic conflict. Excluded rows retain
their raw line and rank for audit.

Daily repository table key: `(event_date, repo_id)`. Columns: latest `repo_name`,
`total_events`, distinct `active_accounts`, `push_events`, `watch_events`,
`star_events`, `fork_events`, `pull_request_events`, `pull_requests_opened`,
`issue_events`, `issues_opened`. Counts cover only this snapshot's observed UTC
window and clean unique public events.
