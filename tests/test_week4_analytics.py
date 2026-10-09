"""Small synthetic fixtures check exact analytics semantics, never production data."""
from datetime import datetime, timezone
import math
import pytest
from pyspark.sql import functions as F

from analytics.metrics import (accounts, calendar, enrichment_candidates, hourly_activity,
                               repositories, repository_daily, window_spec)
from analytics.languages import (categories, metadata_frame, primary_language_daily,
                                 technology_daily, weighted_language_daily)
from analytics.query import compare
from analytics.session import create_analytics_spark


@pytest.fixture(scope="module")
def session():
    spark = create_analytics_spark(master="local[2]", driver_memory="1g", partitions=4)
    yield spark
    spark.stop()


def sources(hours=24):
    return [{"date": "2025-01-01", "hour": h} for h in range(hours)]


def events(session):
    values = []
    def add(repo, actor, hour, kind, action=None, day=1, name=None):
        label = name or ("owner/renamed" if repo == 10 and hour >= 12 else f"owner/repo{repo}")
        values.append((str(len(values)+1), kind, actor, f"account{actor}", repo, label,
                       datetime(2025, 1, day, hour, tzinfo=timezone.utc), datetime(2025, 1, day).date(), hour,
                       action, f"{len(values):064x}"))
    add(10, 1, 0, "WatchEvent", "started")
    add(10, 1, 11, "PushEvent")
    for n in range(6): add(10, 1+n%2, 12+n, "WatchEvent", "started", name="owner/renamed")
    add(10, 2, 18, "ForkEvent")
    add(10, 2, 19, "PullRequestEvent", "closed")
    add(10, 2, 20, "PullRequestEvent", "opened")
    add(20, 1, 0, "WatchEvent", "started")
    add(20, 3, 12, "PushEvent")
    add(30, 4, 12, "WatchEvent", "other")
    add(30, 4, 23, "IssuesEvent", "opened")
    add(30, 4, 0, "PushEvent", day=2)
    schema="event_id string,event_type string,actor_id long,actor_login string,repo_id long,repo_name string,event_time timestamp,event_date date,event_hour int,payload_action string,raw_record_sha256 string"
    return session.createDataFrame(values, schema)


def test_exact_repository_growth_boundaries_renames_and_support(session):
    frame, maxima = repositories(events(session), window_spec(sources()))
    rows = {r.repo_id:r for r in frame.collect()}
    r=rows[10]
    assert r.repo_name == "owner/renamed" and r.total_events == 11
    assert r.active_accounts == 2 and r.code_participating_accounts == 2
    assert r.previous_events == 2 and r.current_events == 9
    assert r.previous_attention == 1 and r.current_attention == 7
    assert r.attention_growth_ratio == 4 and r.attention_change_percent == 600
    assert r.growth_eligible and r.emerging_attention_candidate
    assert r.pull_requests_opened == 1 and r.pull_request_events == 2
    assert not rows[30].growth_eligible and rows[30].attention_change_percent is None
    assert rows[30].total_events == 3 and rows[30].current_events == 2  # End-exclusive.
    assert all(0 <= r.activity_score <= 1 and 0 <= r.trend_score <= 1 for r in rows.values())
    assert maxima['current_stars']==6
    assert [r['repo_id'] for r in compare(frame,[10,20])]==[10,20]
    with pytest.raises(ValueError):compare(frame,[10,999])
    with pytest.raises(ValueError):compare(frame,[10,10])


def test_accounts_distinct_across_days_and_calendar(session):
    frame=events(session)
    summary={r.actor_id:r for r in accounts(frame).collect()}
    assert summary[1].active_repositories==2
    assert summary[4].total_events==3 and summary[4].active_days==2 and summary[4].active_repositories==1
    assert summary[4].weekend_events==0
    daily=accounts(frame,('actor_id','event_date'))
    assert daily.filter('actor_id=4').agg(F.sum('active_repositories')).first()[0]==2
    cal=calendar(frame).orderBy('event_date').collect()
    assert cal[0].weekday_utc=='Wednesday' and str(cal[0].week_start_utc)=='2024-12-30'
    assert not cal[0].is_weekend_utc
    hours=hourly_activity(frame,window_spec(sources()))
    assert hours.count()==24 and hours.filter('event_hour=1').first().total_events==0
    assert hours.agg(F.sum('total_events')).first()[0]==15


@pytest.mark.parametrize('selected', [sources(1),sources(3),sources()[1:3]+sources()[4:6],sources(2)*2])
def test_incomplete_or_unequal_periods_rejected(selected):
    with pytest.raises(ValueError):window_spec(selected)


def entries():
    return [{'repo_id':10,'archived_repo_name':'owner/repo10','status':'ok','primary_language':'Python',
             'language_bytes':{'Python':75,'Rust':25},'topics':['machine-learning'],'description':'example'},
            {'repo_id':20,'archived_repo_name':'owner/repo20','status':'ok','primary_language':None,
             'language_bytes':{},'topics':[]},
            {'repo_id':30,'archived_repo_name':'owner/repo30','status':'identity_mismatch'}]


def test_language_weighting_unknown_buckets_and_distinct_accounts(session):
    frame=events(session)
    metadata=metadata_frame(session,entries())
    primary=primary_language_daily(frame,metadata)
    assert primary.agg(F.sum('total_events')).first()[0]==16
    py=primary.filter("primary_language='Python'").first()
    assert py.total_events==11 and py.active_accounts==2
    assert primary.filter("primary_language='Metadata unavailable'").agg(F.sum('total_events')).first()[0]==3
    weighted, shares=weighted_language_daily(repository_daily(frame),entries())
    assert weighted.agg(F.sum('attributed_total_events')).first()[0]==pytest.approx(16)
    assert weighted.filter("language='Python'").first().attributed_total_events==pytest.approx(8.25)
    assert weighted.filter("language='Rust'").first().attributed_total_events==pytest.approx(2.75)
    assert shares.groupBy('repo_id').agg(F.sum('byte_share')).collect()[0][1]==pytest.approx(1)
    topics=technology_daily(repository_daily(frame),metadata)
    assert topics.filter("technology_category='AI'").first().total_events==11


def test_topic_rules_have_boundaries_and_allow_overlap():
    assert categories(['AI','cloud-native'])==['AI','Cloud']
    assert categories([], 'mail support and daily activity')==['Unclassified']
    assert categories([], 'Machine learning and data pipeline tools')==['AI','Data engineering']
