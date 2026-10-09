"""Synthetic edge fixtures verify temporal boundaries; training uses real archives."""
from datetime import datetime,timedelta,timezone
import json

import pytest
from pyspark.sql import functions as F
from modeling.corpus import project,SCHEMA
from modeling.features import weekly,examples,target_threshold,label_examples,audit_splits,FEATURES
from modeling.session import create_spark


@pytest.fixture(scope='module')
def spark():
    session=create_spark();yield session;session.stop()


def row(identifier,week,kind='PushEvent',repo=1):
    source=datetime(2015,1,1,tzinfo=timezone.utc)+timedelta(days=7*week)
    event={'id':str(identifier),'repo':{'id':repo,'name':f'fixture/repo{repo}'},'actor':{'id':identifier},
           'public':True,'type':kind,'created_at':source.isoformat(),'payload':{'action':'started'}}
    return project(event,json.dumps(event),source,identifier)


def frame(spark,rows):
    from pyspark.sql.types import StructType,StructField,StringType,LongType,TimestampType
    schema=StructType([StructField(f.name,TimestampType() if 'timestamp' in str(f.type) else LongType() if str(f.type)=='int64' else StringType()) for f in SCHEMA])
    return spark.createDataFrame(rows,schema)


def test_lossless_identity_and_invalid_inputs():
    r=row(1,0,repo=2**63-1);assert r['repo_id']==2**63-1
    event={'id':'1','repo':{'id':True,'name':'x/y'},'actor':{'id':1},'public':True,'type':'PushEvent','created_at':'2015-01-01T00:00:00Z','payload':{}}
    with pytest.raises(ValueError,match='invalid_identity'):project(event,'{}',datetime(2015,1,1,tzinfo=timezone.utc),1)
    event['repo']['id']=1;event['created_at']='2015-01-01'
    with pytest.raises(ValueError,match='invalid_timestamp'):project(event,'{}',datetime(2015,1,1,tzinfo=timezone.utc),1)


def test_future_and_late_archive_perturbations_cannot_change_past_features(spark):
    initial=[row(i,0) for i in range(1,7)]
    baseline=weekly(frame(spark,initial)).collect()[0].asDict()
    future=[row(i,4,'WatchEvent',repo=1) for i in range(20,30)]
    late=row(50,4,'WatchEvent');late['event_time']=initial[0]['event_time']
    observed=weekly(frame(spark,initial+future+[late])).filter('week=0').collect()[0].asDict()
    assert baseline==observed
    assert set(FEATURES).isdisjoint({'label','future_star_events_7d','star_delta_7d','repo_id','repo_name'})


def test_absent_future_activity_is_zero_only_with_known_outcome_period(spark):
    rows=[row(i,week,repo=week+1) for week in range(6) for i in range(week*10+1,week*10+7)]
    result=examples(weekly(frame(spark,rows)))
    known=result.filter("split != 'forecast'").collect()
    assert all(r.future_star_events_7d==0 for r in known)
    assert all(r.future_star_events_7d is None for r in result.filter("split='forecast'").collect())


def test_target_cutoff_uses_training_only(spark):
    data=spark.createDataFrame([('train',1),('train',5),('train',10),('validation',999),('test',99999)],['split','star_delta_7d'])
    assert target_threshold(data)==10
    assert target_threshold(data.withColumn('star_delta_7d',F.when(F.col('split')!='train',-99999).otherwise(F.col('star_delta_7d'))))==10


def test_equal_seven_day_temporal_boundaries_and_unknown_forecast(spark):
    rows=[row(week*100+i,week,'WatchEvent' if i<=week*2+1 else 'PushEvent',repo=repo)
          for week in range(6) for repo in [1,2] for i in range(1,10)]
    w=weekly(frame(spark,rows))
    assert w.filter("feature_event_time_max >= as_of_utc OR feature_archive_end_max > as_of_utc").count()==0
    labelled=label_examples(examples(w),2)
    assert labelled.filter("split='forecast' AND label IS NOT NULL").count()==0
    assert labelled.filter("split='test'").select(F.date_format('as_of_utc',"yyyy-MM-dd'T'HH:mm:ssXXX")).first()[0]=='2015-02-05T00:00:00Z'


def test_guard_rejects_missing_or_one_class_splits(spark):
    rows=[row(i,0) for i in range(1,7)]
    with pytest.raises(ValueError,match='Missing chronological split'):
        audit_splits(label_examples(examples(weekly(frame(spark,rows))),1))
