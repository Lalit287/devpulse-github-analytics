"""Exact distinct-ID metrics and explicit equal-window intraday comparisons."""
from datetime import datetime, timedelta, timezone
import math

from pyspark.sql import Window, functions as F

METRIC_VERSION = "devpulse-analytics-v1"
EVENT_COLUMNS = ["event_id", "event_type", "actor_id", "actor_login", "repo_id", "repo_name",
                 "event_time", "event_date", "event_hour", "payload_action", "raw_record_sha256"]


def window_spec(sources):
    starts = sorted(datetime.fromisoformat(f"{s['date']}T{s['hour']:02d}:00:00+00:00") for s in sources)
    if len(starts) < 2 or len(starts) % 2 or len(starts) != len(set(starts)):
        raise ValueError("Trend comparison requires an even number of at least two unique archive hours")
    if any(b - a != timedelta(hours=1) for a, b in zip(starts, starts[1:])):
        raise ValueError("Trend comparison requires contiguous archive coverage")
    end = starts[-1] + timedelta(hours=1)
    split = starts[0] + (end - starts[0]) / 2
    return {"start_utc": starts[0].isoformat(), "split_utc": split.isoformat(), "end_utc": end.isoformat(),
            "period_hours": len(starts) // 2, "archive_hours": len(starts), "timezone": "UTC"}


def sum_when(condition):
    return F.sum(F.when(condition, 1).otherwise(0)).cast("long")


def activity_aggregates():
    kind, action = F.col("event_type"), F.col("payload_action")
    code = (kind == "PushEvent") | ((kind == "PullRequestEvent") & (action == "opened")) | (kind == "PullRequestReviewEvent")
    return [F.count("*").alias("total_events"), F.countDistinct("actor_id").alias("active_accounts"),
            F.countDistinct(F.when(code, F.col("actor_id"))).alias("code_participating_accounts"),
            sum_when(kind == "PushEvent").alias("push_events"),
            sum_when((kind == "WatchEvent") & (action == "started")).alias("star_events"),
            sum_when(kind == "ForkEvent").alias("fork_events"),
            sum_when(kind == "PullRequestEvent").alias("pull_request_events"),
            sum_when((kind == "PullRequestEvent") & (action == "opened")).alias("pull_requests_opened"),
            sum_when((kind == "IssuesEvent") & (action == "opened")).alias("issues_opened"),
            sum_when(kind == "IssueCommentEvent").alias("issue_comment_events"),
            sum_when(kind.isin("PullRequestReviewEvent", "PullRequestReviewCommentEvent")).alias("review_events")]


def repositories(events, spec):
    frame = events.groupBy("repo_id").agg(
        F.max_by("repo_name", F.struct("event_time", "raw_record_sha256")).alias("repo_name"),
        F.min("event_time").alias("first_event_time"), F.max("event_time").alias("last_event_time"),
        *activity_aggregates())
    start, split, end = [F.to_timestamp(F.lit(spec[k])) for k in ("start_utc", "split_utc", "end_utc")]
    aggregates = []
    for label, within in (("previous", (F.col("event_time") >= start) & (F.col("event_time") < split)),
                          ("current", (F.col("event_time") >= split) & (F.col("event_time") < end))):
        aggregates.extend([
            sum_when(within).alias(f"{label}_events"),
            sum_when(within & (F.col("event_type") == "WatchEvent") & (F.col("payload_action") == "started")).alias(f"{label}_stars"),
            sum_when(within & (F.col("event_type") == "ForkEvent")).alias(f"{label}_forks"),
            sum_when(within & (F.col("event_type") == "PullRequestEvent") & (F.col("payload_action") == "opened")).alias(f"{label}_prs_opened"),
            F.countDistinct(F.when(within, F.col("actor_id"))).alias(f"{label}_active_accounts"),
        ])
    periods = events.groupBy("repo_id").agg(*aggregates)
    frame = frame.join(periods, "repo_id")
    for label in ("previous", "current"):
        frame = frame.withColumn(f"{label}_attention", F.col(f"{label}_stars") + F.col(f"{label}_forks"))
    frame = (frame.withColumn("attention_delta", F.col("current_attention") - F.col("previous_attention"))
             .withColumn("attention_growth_ratio", (F.col("current_attention") + 1.0) / (F.col("previous_attention") + 1.0))
             .withColumn("star_growth_ratio", (F.col("current_stars") + 1.0) / (F.col("previous_stars") + 1.0))
             .withColumn("attention_change_percent", F.when(F.col("previous_attention") > 0,
                 100.0 * F.col("attention_delta") / F.col("previous_attention")))
             .withColumn("zero_attention_baseline", F.col("previous_attention") == 0)
             .withColumn("growth_eligible", (F.col("current_attention") >= 5) & (F.col("current_active_accounts") >= 2))
             .withColumn("emerging_attention_candidate", (F.col("previous_attention") <= 2) &
                         (F.col("current_attention") >= 5) & (F.col("current_active_accounts") >= 2))
             .withColumn("participation_ratio", F.col("active_accounts") / F.col("total_events")))
    names = ["current_stars", "current_forks", "current_active_accounts", "current_prs_opened"]
    maxima = frame.agg(*[F.max(c).alias(c) for c in names]).first().asDict()
    normalized = []
    for name in names:
        maximum = maxima[name] or 0
        normalized.append(F.log1p(F.col(name)) / F.lit(math.log1p(maximum)) if maximum else F.lit(0.0))
    volume = sum(weight * value for weight, value in zip([0.4, 0.3, 0.2, 0.1], normalized))
    growth = F.log(F.greatest(F.lit(1.0), F.least(F.lit(10.0), F.col("attention_growth_ratio")))) / F.lit(math.log(10))
    return (frame.withColumn("activity_score", volume)
            .withColumn("trend_score", 0.7 * F.col("activity_score") + 0.3 * growth)), maxima


def repository_daily(events):
    return events.groupBy("event_date", "repo_id").agg(
        F.max_by("repo_name", F.struct("event_time", "raw_record_sha256")).alias("repo_name"), *activity_aggregates())


def repository_hourly(events):
    return events.groupBy("event_date", "event_hour", "repo_id").agg(*activity_aggregates())


def accounts(events, keys=("actor_id",)):
    kind, action = F.col("event_type"), F.col("payload_action")
    return events.groupBy(*keys).agg(
        F.max_by("actor_login", F.struct("event_time", "raw_record_sha256")).alias("actor_login"),
        F.count("*").alias("total_events"), F.countDistinct("repo_id").alias("active_repositories"),
        F.countDistinct("event_date").alias("active_days"),
        sum_when(kind == "PushEvent").alias("push_events"),
        sum_when((kind == "PullRequestEvent") & (action == "opened")).alias("pull_requests_opened"),
        sum_when((kind == "IssuesEvent") & (action == "opened")).alias("issues_opened"),
        sum_when(kind == "IssueCommentEvent").alias("issue_comment_events"),
        sum_when(kind.isin("PullRequestReviewEvent", "PullRequestReviewCommentEvent")).alias("review_events"),
        sum_when(F.dayofweek("event_date").isin(1, 7)).alias("weekend_events"))


def calendar(events):
    return (events.groupBy("event_date").agg(F.count("*").alias("total_events"),
                                           F.countDistinct("actor_id").alias("active_accounts"),
                                           F.countDistinct("repo_id").alias("active_repositories"))
            .withColumn("weekday_utc", F.date_format("event_date", "EEEE"))
            .withColumn("is_weekend_utc", F.dayofweek("event_date").isin(1, 7))
            .withColumn("week_start_utc", F.date_sub("event_date", F.pmod(F.dayofweek("event_date") + 5, F.lit(7)))))


def hourly_activity(events, spec):
    observed = events.groupBy("event_date", "event_hour").agg(
        F.count("*").alias("total_events"), F.countDistinct("actor_id").alias("active_accounts"),
        F.countDistinct("repo_id").alias("active_repositories"))
    epoch = int(datetime.fromisoformat(spec["start_utc"]).timestamp())
    grid = (events.sparkSession.range(spec["archive_hours"]).withColumn(
        "at", F.timestamp_seconds(F.lit(epoch) + F.col("id") * 3600))
        .select(F.to_date("at").alias("event_date"), F.hour("at").alias("event_hour")))
    return grid.join(observed, ["event_date", "event_hour"], "left").fillna(0)


def participation_sample(events, repository_metrics, repo_limit=20, account_limit=50):
    selected = repository_metrics.orderBy(F.col("star_events").desc(), F.col("repo_id").asc()).limit(repo_limit).select("repo_id")
    edges = events.join(F.broadcast(selected), "repo_id").groupBy("repo_id", "actor_id").agg(
        F.count("*").alias("participation_events"))
    ordering = Window.partitionBy("repo_id").orderBy(F.col("participation_events").desc(), "actor_id")
    return edges.withColumn("account_rank", F.row_number().over(ordering)).filter(F.col("account_rank") <= account_limit)


def enrichment_candidates(frame, limit=20):
    selected = {}
    rules = [("star_activity", frame.orderBy(F.col("star_events").desc(), "repo_id"), 10),
             ("intraday_trend", frame.filter("growth_eligible").orderBy(F.col("trend_score").desc(), "repo_id"), 6),
             ("overall_activity", frame.orderBy(F.col("total_events").desc(), "repo_id"), 4)]
    for reason, ranking, quota in rules:
        for row in ranking.select("repo_id", "repo_name", "total_events", "star_events", "fork_events", "trend_score").limit(quota).collect():
            entry = selected.setdefault(row.repo_id, dict(row.asDict(), selection_reasons=[]))
            entry["selection_reasons"].append(reason)
    if len(selected) < limit:
        for row in frame.orderBy(F.col("star_events").desc(), "repo_id").select(
                "repo_id", "repo_name", "total_events", "star_events", "fork_events", "trend_score").limit(limit).collect():
            if len(selected) >= limit:
                break
            selected.setdefault(row.repo_id, dict(row.asDict(), selection_reasons=["star_activity_fill"]))
    return list(selected.values())[:limit]
