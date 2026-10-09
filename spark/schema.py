"""Stable common-field projection; full event-specific payload JSON is retained."""
from pyspark.sql.types import BooleanType, StringType, StructField, StructType

CONTRACT_VERSION = "devpulse-silver-v1"
KNOWN_EVENTS = ("PushEvent", "WatchEvent", "ForkEvent", "IssuesEvent", "PullRequestEvent",
                "CreateEvent", "IssueCommentEvent", "PullRequestReviewEvent", "DeleteEvent",
                "CommitCommentEvent", "GollumEvent", "MemberEvent", "PublicEvent", "ReleaseEvent",
                "PullRequestReviewCommentEvent", "DiscussionEvent")

RAW_SCHEMA = StructType([
    StructField("id", StringType()), StructField("type", StringType()),
    StructField("actor", StructType([StructField("id", StringType()), StructField("login", StringType())])),
    StructField("repo", StructType([StructField("id", StringType()), StructField("name", StringType())])),
    StructField("created_at", StringType()), StructField("public", BooleanType()),
    StructField("org", StructType([StructField("id", StringType()), StructField("login", StringType())])),
    StructField("payload", StructType([StructField("action", StringType()), StructField("ref", StringType()),
                                       StructField("ref_type", StringType())])),
    StructField("_corrupt_record", StringType()),
])

JSON_OPTIONS = {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record",
                "allowSingleQuotes": "false", "allowComments": "false",
                "allowUnquotedFieldNames": "false", "allowNonNumericNumbers": "false"}

CLEAN_COLUMNS = ("event_id", "event_type", "actor_id", "actor_login", "repo_id", "repo_name",
                 "event_time", "public", "org_id", "org_login", "payload_json", "payload_action",
                 "payload_ref", "payload_ref_type", "event_date", "event_hour", "source_uri",
                 "source_filename", "source_archive_date", "source_archive_hour",
                 "source_archive_start", "archive_hour_offset_seconds", "raw_record_sha256", "quality_warnings")
