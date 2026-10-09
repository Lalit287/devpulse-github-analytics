"""Past-only weekly features and complete seven-day outcomes, keyed by stable IDs."""
from datetime import datetime,timedelta,timezone
import math

from pyspark import StorageLevel
from pyspark.sql import Window,functions as F

from modeling.corpus import START

COUNTS=['star_events_7d','fork_events_7d','push_events_7d','pull_requests_opened_7d',
        'issues_opened_7d','total_events_7d','active_accounts_7d','code_accounts_7d',
        'active_days_7d','stars_first4d','stars_last3d']
FEATURES=['log_'+name for name in COUNTS]+['star_growth_rate']
BASELINE_MIN_EVENTS=5


def weekly(events):
    origin=datetime.combine(START,datetime.min.time(),timezone.utc)
    source_week=F.floor((F.unix_seconds('source_archive_start')-int(origin.timestamp()))/(7*86400)).cast('int')
    frame=events.withColumn('week',source_week)
    frame=frame.withColumn('week_start',F.timestamp_seconds(F.lit(int(origin.timestamp()))+F.col('week')*7*86400))
    # A later archive cannot retroactively insert a late event into earlier features.
    frame=frame.filter((F.col('event_time')>=F.col('week_start')) &
                       (F.col('event_time')<F.col('week_start')+F.expr('INTERVAL 7 DAYS')))
    kind=F.col('event_type');star=(kind=='WatchEvent') & (F.col('payload_action')=='started')
    pr=(kind=='PullRequestEvent') & (F.col('payload_action')=='opened')
    def count(condition):return F.sum(F.when(condition,1).otherwise(0)).cast('long')
    day=F.floor((F.unix_seconds('source_archive_start')-F.unix_seconds('week_start'))/86400)
    result=frame.groupBy('week','repo_id').agg(
        F.max_by('repo_name',F.struct('event_time','source_archive_start','source_row')).alias('repo_name'),
        count(star).alias('star_events_7d'),count(kind=='ForkEvent').alias('fork_events_7d'),
        count(kind=='PushEvent').alias('push_events_7d'),count(pr).alias('pull_requests_opened_7d'),
        count((kind=='IssuesEvent') & (F.col('payload_action')=='opened')).alias('issues_opened_7d'),
        F.count('*').alias('total_events_7d'),F.countDistinct('actor_id').alias('active_accounts_7d'),
        F.countDistinct(F.when((kind=='PushEvent') | pr,F.col('actor_id'))).alias('code_accounts_7d'),
        F.countDistinct(F.to_date('event_time')).alias('active_days_7d'),
        count(star & (day<4)).alias('stars_first4d'),count(star & (day>=4)).alias('stars_last3d'),
        F.max('event_time').alias('feature_event_time_max'),
        F.max(F.col('source_archive_start')+F.expr('INTERVAL 1 HOUR')).alias('feature_archive_end_max'))
    result=(result.withColumn('as_of_utc',F.timestamp_seconds(F.lit(int(origin.timestamp()))+(F.col('week')+1)*7*86400))
            .withColumn('feature_start_utc',F.col('as_of_utc')-F.expr('INTERVAL 7 DAYS'))
            .withColumn('label_end_utc',F.col('as_of_utc')+F.expr('INTERVAL 7 DAYS'))
            .withColumn('star_growth_rate',(F.col('stars_last3d')/3+1)/(F.col('stars_first4d')/4+1)))
    for name in COUNTS:result=result.withColumn('log_'+name,F.log1p(name))
    return result


def examples(weeks):
    future=weeks.select((F.col('week')-1).alias('week'),'repo_id',F.col('star_events_7d').alias('future_star_events_7d'))
    result=(weeks.filter(F.col('total_events_7d')>=BASELINE_MIN_EVENTS).join(future,['week','repo_id'],'left')
            .withColumn('split',F.when(F.col('week')<=2,'train').when(F.col('week')==3,'validation').when(F.col('week')==4,'test').otherwise('forecast')))
    result=result.withColumn('future_star_events_7d',F.when(F.col('split')!='forecast',F.coalesce('future_star_events_7d',F.lit(0))).otherwise(F.lit(None).cast('long')))
    return result.withColumn('star_delta_7d',F.col('future_star_events_7d')-F.col('star_events_7d'))


def target_threshold(frame):
    training=frame.filter((F.col('split')=='train') & (F.col('star_delta_7d')>0))
    quantile=training.approxQuantile('star_delta_7d',[.90],0.0)
    if not quantile:raise ValueError('Training data has no positive star growth')
    return max(1,math.ceil(quantile[0]))


def label_examples(frame,threshold):
    return frame.withColumn('label',F.when(F.col('split')!='forecast',
        ((F.col('future_star_events_7d')>=5) & (F.col('star_delta_7d')>=threshold)).cast('double')))


def audit_splits(frame):
    bounds={}
    summary=frame.groupBy('split').agg(F.count('*').alias('rows'),F.sum('label').alias('positives'),
                F.min('as_of_utc').alias('first_as_of'),F.max('as_of_utc').alias('last_as_of'),
                F.max('label_end_utc').alias('last_label_end'))
    for name in ['first_as_of','last_as_of','last_label_end']:
        summary=summary.withColumn(name,F.date_format(name,"yyyy-MM-dd'T'HH:mm:ssXXX"))
    for row in summary.collect():
        bounds[row['split']]={k:(v.isoformat() if hasattr(v,'isoformat') else v) for k,v in row.asDict().items() if k!='split'}
    if set(bounds)!={'train','validation','test','forecast'}:raise ValueError('Missing chronological split')
    for name in ['train','validation','test']:
        b=bounds[name]
        if not 0<b['positives']<b['rows']:raise ValueError(f'{name} requires both target classes')
    if not (bounds['train']['last_label_end']<=bounds['validation']['first_as_of'] and
            bounds['validation']['last_label_end']<=bounds['test']['first_as_of'] and
            bounds['test']['last_label_end']<=bounds['forecast']['first_as_of']):
        raise ValueError('Future-label periods cross split boundaries')
    violations=frame.filter((F.col('feature_event_time_max')>=F.col('as_of_utc')) |
        (F.col('feature_archive_end_max')>F.col('as_of_utc')) |
        (F.col('feature_event_time_max')<F.col('feature_start_utc'))).limit(1).count()
    if violations:raise ValueError('Future information in feature windows')
    if frame.filter("split='forecast' AND (label IS NOT NULL OR future_star_events_7d IS NOT NULL)").count():
        raise ValueError('Unknown forecast outcomes were manufactured')
    return bounds
