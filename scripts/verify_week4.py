"""Full-table parity, source preservation, API identity, and ranking sensitivity."""
import json
import math
import xml.etree.ElementTree as ET
from pathlib import Path

from pyspark.sql import functions as F

from config.settings import ROOT
from exploration.io import sha256_file, write_json
from ingestion.state import utc_now
from spark.snapshots import current_manifest
from analytics.artifacts import code_hashes, cleanup_session
from analytics.query import compare
from analytics.session import create_analytics_spark
from enrichment.github import checksum, load_cache


def main():
    checks = {}
    def check(name, value):
        checks[name] = bool(value)
        if not value:
            raise ValueError(f"Week 4 verification failed: {name}")

    gold_root = ROOT / "data/gold"
    manifest = current_manifest(gold_root)
    silver = current_manifest(ROOT / "data/silver")
    check("current_gold_from_current_silver", manifest["recipe"]["silver_snapshot_id"] == silver["snapshot_id"])
    check("current_analytics_code", manifest["recipe"]["code_hashes"] == code_hashes())
    target = gold_root / "snapshots" / manifest["snapshot_id"]
    check("all_pipeline_checks", all(v == "passed" for v in manifest["checks"].values()))
    entries = json.loads((target / "metadata_snapshot.json").read_text())
    check("pinned_metadata_checksum", checksum(entries) == manifest["recipe"]["metadata_snapshot_sha256"])
    for e in entries:
        cache = load_cache(ROOT / "data/metadata/github_repositories" / f"{e['repo_id']}.json", e["repo_id"])
        check(f"cache_matches_pinned_metadata:{e['repo_id']}", cache == e)
        if e["status"] == "ok":
            check(f"public_stable_identity:{e['repo_id']}", e["metadata"]["body"]["id"] == e["repo_id"] and
                  e["metadata"]["body"]["private"] is False and all(type(v) is int and v >= 0 for v in e["language_bytes"].values()))
    session = create_analytics_spark()
    sensitivity = {}
    try:
        def table(name):return session.read.parquet((target / name).as_uri())
        events = session.read.parquet((ROOT / "data/silver/snapshots" / silver["snapshot_id"] / "events").as_uri())
        repos, accounts = table("repository_metrics"), table("account_metrics")
        key_map = {"repository_metrics":["repo_id"],"repository_daily":["repo_id","event_date"],
                   "repository_hourly":["repo_id","event_date","event_hour"],"account_metrics":["actor_id"],
                   "account_daily":["actor_id","event_date"],"account_hourly":["actor_id","event_date","event_hour"],
                   "activity_calendar":["event_date"],"hourly_activity":["event_date","event_hour"],
                   "participation_edges_sample":["repo_id","actor_id"],"repository_metadata":["repo_id"],
                   "repository_language_shares":["repo_id","language"],
                   "language_primary_daily":["event_date","primary_language","enrichment_status"],
                   "language_weighted_daily":["event_date","language"],"technology_daily":["event_date","technology_category"],
                   "language_intraday_growth":["primary_language","enrichment_status"]}
        for name, measure in manifest["tables"].items():
            frame = table(name)
            check(f"saved_rows_and_keys:{name}", frame.count() == frame.select(*key_map[name]).distinct().count() == measure["rows"])
            if "events" in measure:
                check(f"saved_event_total:{name}", frame.agg(F.sum("total_events")).first()[0] == measure["events"])
        baseline = session.read.parquet((ROOT / "data/silver/snapshots" / silver["snapshot_id"] / "repository_daily_stats").as_uri())
        shared = ["event_date","repo_id","repo_name","total_events","active_accounts","push_events","star_events",
                  "fork_events","pull_request_events","pull_requests_opened","issues_opened"]
        actual, reference = table("repository_daily").select(*shared), baseline.select(*shared)
        check("full_repository_daily_week3_parity", actual.exceptAll(reference).limit(1).count()==0 and
              reference.exceptAll(actual).limit(1).count()==0)
        pairs=events.select("repo_id","actor_id").distinct().count()
        check("exact_cross_repository_account_participation", repos.agg(F.sum("active_accounts")).first()[0] ==
              accounts.agg(F.sum("active_repositories")).first()[0] == pairs)
        for name, predicate in (("push_events", "event_type = 'PushEvent'"),
                                ("pull_requests_opened", "event_type = 'PullRequestEvent' AND payload_action = 'opened'"),
                                ("issues_opened", "event_type = 'IssuesEvent' AND payload_action = 'opened'"),
                                ("issue_comment_events", "event_type = 'IssueCommentEvent'"),
                                ("review_events", "event_type IN ('PullRequestReviewEvent','PullRequestReviewCommentEvent')")):
            count = events.filter(predicate).count()
            check(f"account_and_repository_metric:{name}", accounts.agg(F.sum(name)).first()[0] == repos.agg(F.sum(name)).first()[0] == count)
        spec = manifest["recipe"]["window"]
        for period, left, right in (("previous",spec["start_utc"],spec["split_utc"]),("current",spec["split_utc"],spec["end_utc"])):
            covered=events.filter((F.col("event_time")>=F.to_timestamp(F.lit(left))) & (F.col("event_time")<F.to_timestamp(F.lit(right))))
            check(f"equal_window_events:{period}", repos.agg(F.sum(f"{period}_events")).first()[0] == covered.count())
            attention=covered.filter("(event_type='WatchEvent' AND payload_action='started') OR event_type='ForkEvent'").count()
            check(f"equal_window_attention:{period}", repos.agg(F.sum(f"{period}_attention")).first()[0] == attention)
        check("account_and_repo_ratios_valid", repos.filter("participation_ratio <= 0 OR participation_ratio > 1 OR code_participating_accounts > active_accounts").count()==0)
        primary=table("language_primary_daily")
        check("primary_language_exact_accounting", primary.agg(F.sum("total_events")).first()[0]==silver["clean_events"])
        weighted=table("language_weighted_daily")
        check("byte_weighted_activity_accounting", abs(weighted.agg(F.sum("attributed_total_events")).first()[0]-silver["clean_events"])<0.01)
        named = primary.filter("enrichment_status='ok' AND primary_language != 'No primary language reported'").agg(F.sum("total_events")).first()[0] or 0
        language_coverage = {"named_primary_language_events": named, "named_primary_language_event_fraction": named/silver["clean_events"],
                             "metadata_covered_events":manifest["coverage"]["covered_events"], "total_events":silver["clean_events"]}
        rankings=json.loads((target/"rankings.json").read_text())
        selected_ids=[r["repo_id"] for r in rankings["most_starred"][:2]]
        comparison=compare(repos,selected_ids)
        write_json(ROOT/"reports/week4/repository_comparison.json",comparison)
        check("real_repository_comparison",len(comparison)==2)
        core=json.loads((target/"core_manifest.json").read_text())
        names=["current_stars","current_forks","current_active_accounts","current_prs_opened"]
        normalized=[F.log1p(F.col(c))/F.lit(math.log1p(core["normalization_maxima"][c])) if core["normalization_maxima"][c] else F.lit(0.0) for c in names]
        growth=F.log(F.greatest(F.lit(1.0),F.least(F.lit(10.0),F.col("attention_growth_ratio"))))/F.lit(math.log(10))
        default_ids={r["repo_id"] for r in rankings["intraday_trending"][:20]}
        for label, weights in (("balanced",[.25,.25,.25,.25]),("star_emphasis",[.7,.1,.1,.1]),("participation_emphasis",[.2,.1,.6,.1])):
            scored=repos.filter("growth_eligible").withColumn("alternative_score",.7*sum(w*x for w,x in zip(weights,normalized))+.3*growth)
            rows=[r.asDict() for r in scored.orderBy(F.col("alternative_score").desc(),"repo_id").select("repo_id","repo_name","alternative_score").limit(20).collect()]
            ids={r["repo_id"] for r in rows}
            sensitivity[label]={"activity_weights":weights,"top20_overlap_with_default":len(ids & default_ids),
                                "top20_jaccard":len(ids & default_ids)/len(ids | default_ids),"top20":rows}
        write_json(ROOT/"reports/week4/score_sensitivity.json",sensitivity)
    finally:
        cleanup_session(session)
    initial=json.loads((ROOT/"reports/week4/initial_run.json").read_text())
    rerun=json.loads((ROOT/"reports/week4/idempotent_rerun.json").read_text())
    check("real_idempotent_rerun",initial["snapshot_id"]==rerun["snapshot_id"]==manifest["snapshot_id"] and
          rerun["status"]=="reused" and rerun["enrichment"]["http_requests"]==0 and rerun["enrichment"]["cache_hits"]==20)
    first_enrichment=json.loads((ROOT/"reports/week4/enrichment_run.json").read_text())
    check("real_bounded_metadata_requests",0 < first_enrichment["http_requests"] <= first_enrichment["request_budget"] and len(entries)==20)
    tests=0
    for name in ["analytics_tests.xml","previous_week_regression.xml"]:
        tree=ET.parse(ROOT/"reports/week4"/name).getroot()
        cases=tree.findall('.//testcase')
        check(f"tests:{name}",bool(cases) and not any(tree.findall(f'.//{tag}') for tag in ['failure','error','skipped']))
        tests+=len(cases)
    baseline=json.loads((ROOT/"reports/week4/previous_weeks_preservation.json").read_text())
    for relative,digest in baseline.items():check(f"preserved:{relative}",sha256_file(ROOT/relative)==digest)
    result={"status":"passed","snapshot_id":manifest["snapshot_id"],"verified_at_utc":utc_now(),"checks":checks,
            "tests_passed":tests,"previous_artifacts_preserved":len(baseline),"language_coverage":language_coverage,
            "score_sensitivity_overlap":{k:v["top20_overlap_with_default"] for k,v in sensitivity.items()}}
    write_json(ROOT/"reports/week4/integration_verification.json",result)
    print(json.dumps({k:v for k,v in result.items() if k!='checks'},indent=2))


if __name__ == '__main__':main()
