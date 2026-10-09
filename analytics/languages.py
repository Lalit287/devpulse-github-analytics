"""Current metadata associations; no claim of historical language observation."""
import re

from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, LongType, StringType, ArrayType)

from analytics.metrics import activity_aggregates

TOPIC_RULES = {
    "AI": {"artificial-intelligence", "machine-learning", "deep-learning", "llm", "generative-ai", "nlp", "ai"},
    "Cloud": {"cloud", "cloud-native", "kubernetes", "aws", "azure", "gcp", "devops"},
    "Data engineering": {"data-engineering", "etl", "analytics", "big-data", "apache-spark", "data-pipeline"},
    "Mobile": {"android", "ios", "flutter", "react-native", "mobile"},
}
DESCRIPTION_RULES = {
    "AI": r"\b(machine learning|deep learning|large language model|artificial intelligence|generative ai)\b",
    "Cloud": r"\b(cloud native|cloud computing|kubernetes)\b",
    "Data engineering": r"\b(data engineering|data pipeline|apache spark|extract transform load)\b",
    "Mobile": r"\b(android|ios|mobile application|mobile app)\b",
}


def categories(topics, description=None):
    topics = {t.casefold() for t in topics}
    description = description.casefold() if isinstance(description, str) else ""
    matches = [label for label in TOPIC_RULES if topics & TOPIC_RULES[label] or re.search(DESCRIPTION_RULES[label], description)]
    return matches or ["Unclassified"]


METADATA_SCHEMA = StructType([
    StructField("repo_id", LongType(), False), StructField("archived_repo_name", StringType()),
    StructField("current_full_name", StringType()), StructField("enrichment_status", StringType(), False),
    StructField("primary_language", StringType(), False), StructField("topics", ArrayType(StringType()), False),
    StructField("technology_categories", ArrayType(StringType()), False),
    StructField("metadata_fetched_at_utc", StringType()), StructField("languages_fetched_at_utc", StringType()),
    StructField("repository_created_at", StringType()), StructField("api_stargazers_count", LongType()),
    StructField("api_forks_count", LongType()),
])


def metadata_frame(session, entries):
    rows = []
    for e in entries:
        good = e["status"] == "ok"
        rows.append((e["repo_id"], e["archived_repo_name"], e.get("current_full_name"), e["status"],
                     (e.get("primary_language") or "No primary language reported") if good else "Metadata unavailable",
                     e.get("topics", []), categories(e.get("topics", []), e.get("description")) if good else ["Metadata unavailable"],
                     e.get("metadata_fetched_at_utc"), e.get("languages_fetched_at_utc"), e.get("repository_created_at"),
                     e.get("api_stargazers_count"), e.get("api_forks_count")))
    return session.createDataFrame(rows, METADATA_SCHEMA)


def primary_language_daily(events, metadata):
    joined = events.join(F.broadcast(metadata.select("repo_id", "primary_language", "enrichment_status")), "repo_id", "left").fillna(
        {"primary_language": "Not enriched", "enrichment_status": "not_selected"})
    return joined.groupBy("event_date", "primary_language", "enrichment_status").agg(
        F.countDistinct("repo_id").alias("active_repositories"), *activity_aggregates())


def weighted_language_daily(repository_daily, entries):
    rows = []
    for entry in entries:
        values = entry.get("language_bytes", {}) if entry["status"] == "ok" else {}
        total = sum(values.values())
        if total:
            rows.extend((entry["repo_id"], language, value, value / total) for language, value in sorted(values.items()) if value)
        else:
            label = "No language reported" if entry["status"] == "ok" else "Metadata unavailable"
            rows.append((entry["repo_id"], label, 0, 1.0))
    schema = "repo_id long, language string, language_bytes long, byte_share double"
    shares = repository_daily.sparkSession.createDataFrame(rows, schema)
    joined = repository_daily.join(F.broadcast(shares), "repo_id", "left").fillna({"language": "Not enriched", "byte_share": 1.0, "language_bytes": 0})
    metrics = ["total_events", "star_events", "fork_events", "push_events", "pull_requests_opened", "issues_opened"]
    weighted = joined.groupBy("event_date", "language").agg(
        F.countDistinct("repo_id").alias("associated_repositories"),
        *[F.sum(F.col(name) * F.col("byte_share")).alias(f"attributed_{name}") for name in metrics])
    return weighted, shares


def technology_daily(repository_daily, metadata):
    joined = repository_daily.join(F.broadcast(metadata.select("repo_id", "technology_categories")), "repo_id", "left")
    joined = joined.withColumn("technology_category", F.explode(F.coalesce("technology_categories", F.array(F.lit("Not enriched")))))
    return joined.groupBy("event_date", "technology_category").agg(
        F.countDistinct("repo_id").alias("active_repositories"),
        *[F.sum(name).alias(name) for name in ("total_events", "star_events", "fork_events", "push_events")])


def primary_language_growth(repository_metrics, metadata):
    joined = repository_metrics.join(F.broadcast(metadata.select("repo_id", "primary_language", "enrichment_status")), "repo_id", "left").fillna(
        {"primary_language": "Not enriched", "enrichment_status": "not_selected"})
    grouped = joined.groupBy("primary_language", "enrichment_status").agg(
        *[F.sum(c).alias(c) for c in ("previous_attention", "current_attention", "previous_events", "current_events")])
    return (grouped.withColumn("attention_growth_ratio", (F.col("current_attention") + 1.0) / (F.col("previous_attention") + 1.0))
            .withColumn("attention_delta", F.col("current_attention") - F.col("previous_attention"))
            .withColumn("zero_attention_baseline", F.col("previous_attention") == 0))
