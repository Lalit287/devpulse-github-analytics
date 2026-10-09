"""Read bounded rankings or compare repositories from the current Gold snapshot."""
import argparse
import json
from pathlib import Path

from pyspark.sql import functions as F
from config.settings import ROOT
from spark.snapshots import current_manifest
from analytics.session import create_analytics_spark
from analytics.artifacts import cleanup_session


def compare(frame, ids):
    if len(set(ids)) < 2:
        raise ValueError("Select at least two distinct repository IDs")
    rows = [row.asDict() for row in frame.filter(F.col("repo_id").isin(*ids)).select(
        "repo_id", "repo_name", "total_events", "star_events", "fork_events", "push_events", "pull_requests_opened",
        "issues_opened", "active_accounts", "code_participating_accounts", "previous_attention", "current_attention",
        "attention_growth_ratio", "attention_change_percent", "activity_score", "trend_score").orderBy("repo_id").collect()]
    if {row["repo_id"] for row in rows} != set(ids):
        raise ValueError("One or more repository IDs are absent from this dataset")
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gold-root", type=Path, default=ROOT / "data/gold")
    parser.add_argument("--repo-id", type=int, nargs="+", required=True)
    args = parser.parse_args()
    manifest = current_manifest(args.gold_root)
    if manifest is None:
        parser.error("No complete Gold analytics snapshot")
    session = create_analytics_spark(master="local[2]", driver_memory="1g", partitions=4)
    try:
        frame = session.read.parquet((args.gold_root.resolve() / "snapshots" / manifest["snapshot_id"] / "repository_metrics").as_uri())
        print(json.dumps(compare(frame, args.repo_id), indent=2))
    finally:
        cleanup_session(session)


if __name__ == "__main__":
    main()
