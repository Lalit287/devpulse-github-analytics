"""SQL-only validation and deterministic deduplication; no Python UDFs."""
from pyspark.sql import Window, functions as F

from spark.schema import CLEAN_COLUMNS, JSON_OPTIONS, KNOWN_EVENTS, RAW_SCHEMA


def reason_array(conditions):
    return F.filter(F.array(*[F.when(condition, F.lit(label)) for label, condition in conditions]),
                    lambda value: value.isNotNull())


def normalize_events(lines):
    """Input columns: raw_json and source_uri. Every line remains accountable."""
    parsed = lines.withColumn("_event", F.from_json("raw_json", RAW_SCHEMA, JSON_OPTIONS))
    frame = parsed.select(
        "raw_json", "source_uri", F.sha2("raw_json", 256).alias("raw_record_sha256"),
        F.regexp_extract("source_uri", r"([^/]+)$", 1).alias("source_filename"),
        F.trim("_event.id").alias("event_id"), F.trim("_event.type").alias("event_type"),
        F.trim("_event.actor.id").alias("actor_id_raw"), F.trim("_event.actor.login").alias("actor_login"),
        F.trim("_event.repo.id").alias("repo_id_raw"), F.trim("_event.repo.name").alias("repo_name"),
        F.trim("_event.created_at").alias("created_at_raw"), F.col("_event.public").alias("public"),
        F.trim("_event.org.id").alias("org_id_raw"), F.trim("_event.org.login").alias("org_login"),
        F.get_json_object("raw_json", "$.payload").alias("payload_json"),
        F.col("_event.payload.action").alias("payload_action"),
        F.col("_event.payload.ref").alias("payload_ref"),
        F.col("_event.payload.ref_type").alias("payload_ref_type"),
        F.col("_event._corrupt_record").alias("corrupt_record"),
    )
    # IDs remain lossless; conversion errors return null even with Spark ANSI mode.
    frame = (frame.withColumn("actor_id", F.expr("try_cast(actor_id_raw AS BIGINT)"))
             .withColumn("repo_id", F.expr("try_cast(repo_id_raw AS BIGINT)"))
             .withColumn("org_id", F.expr("try_cast(org_id_raw AS BIGINT)"))
             .withColumn("event_time", F.try_to_timestamp("created_at_raw", F.lit("yyyy-MM-dd'T'HH:mm:ss[.SSSSSSSSS]XXX")))
             .withColumn("source_archive_date", F.regexp_extract("source_filename", r"^(\d{4}-\d{2}-\d{2})-", 1))
             .withColumn("_source_hour", F.regexp_extract("source_filename", r"-(\d{1,2})\.json\.gz$", 1))
             .withColumn("source_archive_hour", F.expr("try_cast(_source_hour AS INT)"))
             .withColumn("source_archive_start", F.try_to_timestamp(
                 F.concat("source_archive_date", F.lit("T"), F.lpad("_source_hour", 2, "0"), F.lit(":00:00Z")),
                 F.lit("yyyy-MM-dd'T'HH:mm:ssXXX")))
             .withColumn("event_date", F.to_date("event_time"))
             .withColumn("event_hour", F.hour("event_time"))
             .withColumn("archive_hour_offset_seconds", F.unix_seconds("event_time") - F.unix_seconds("source_archive_start")))
    conditions = [
        ("invalid_json_or_field_type", F.col("corrupt_record").isNotNull() | ~F.trim("raw_json").startswith("{")),
        ("record_exceeds_8_mib", F.octet_length("raw_json") > 8 * 1024**2),
        ("invalid_event_id", F.col("event_id").isNull() | ~F.col("event_id").rlike(r"^[1-9][0-9]*$")),
        ("invalid_event_type", F.col("event_type").isNull() | ~F.col("event_type").rlike(r"^[A-Z][A-Za-z0-9]*Event$")),
        ("invalid_actor_id", F.col("actor_id").isNull() | (F.col("actor_id") <= 0) | ~F.col("actor_id_raw").rlike(r"^[0-9]+$")),
        ("missing_actor_login", F.col("actor_login").isNull() | (F.col("actor_login") == "")),
        ("invalid_repo_id", F.col("repo_id").isNull() | (F.col("repo_id") <= 0) | ~F.col("repo_id_raw").rlike(r"^[0-9]+$")),
        ("invalid_repo_name", F.col("repo_name").isNull() | ~F.col("repo_name").rlike(r"^[^\s/]+/[^\s/]+$")),
        ("invalid_timestamp", F.col("event_time").isNull() | ~F.col("created_at_raw").rlike(r"(Z|[+-]\d{2}:\d{2})$")),
        ("not_public", ~F.col("public").eqNullSafe(True)),
        ("missing_payload", F.col("payload_json").isNull()),
    ]
    frame = frame.withColumn("validation_errors", reason_array(conditions))
    warnings = [
        ("unknown_event_type", F.col("event_type").isNotNull() & ~F.col("event_type").isin(*KNOWN_EVENTS)),
        ("outside_archive_hour", (F.col("archive_hour_offset_seconds") < 0) | (F.col("archive_hour_offset_seconds") >= 3600)),
        ("invalid_optional_org_id", F.col("org_id_raw").isNotNull() & (F.col("org_id").isNull() | (F.col("org_id") <= 0))),
    ]
    return frame.withColumn("quality_warnings", reason_array(warnings))


def split_and_deduplicate(normalized):
    invalid = normalized.filter(F.size("validation_errors") > 0)
    valid = normalized.filter(F.size("validation_errors") == 0)
    order = Window.partitionBy("event_id").orderBy(F.col("source_archive_start").asc_nulls_last(),
                                                   "source_filename", "raw_record_sha256")
    ranked = valid.withColumn("duplicate_rank", F.row_number().over(order))
    clean = ranked.filter("duplicate_rank = 1").select(*CLEAN_COLUMNS)
    duplicates = ranked.filter("duplicate_rank > 1")
    return clean, invalid, duplicates


def repository_daily_stats(events):
    def count_when(condition):
        return F.sum(F.when(condition, 1).otherwise(0)).cast("long")
    kind = F.col("event_type")
    return events.groupBy("event_date", "repo_id").agg(
        F.max_by("repo_name", F.struct("event_time", "raw_record_sha256")).alias("repo_name"),
        F.count("*").alias("total_events"), F.countDistinct("actor_id").alias("active_accounts"),
        count_when(kind == "PushEvent").alias("push_events"),
        count_when(kind == "WatchEvent").alias("watch_events"),
        count_when((kind == "WatchEvent") & (F.col("payload_action") == "started")).alias("star_events"),
        count_when(kind == "ForkEvent").alias("fork_events"),
        count_when(kind == "PullRequestEvent").alias("pull_request_events"),
        count_when((kind == "PullRequestEvent") & (F.col("payload_action") == "opened")).alias("pull_requests_opened"),
        count_when(kind == "IssuesEvent").alias("issue_events"),
        count_when((kind == "IssuesEvent") & (F.col("payload_action") == "opened")).alias("issues_opened"),
    )
