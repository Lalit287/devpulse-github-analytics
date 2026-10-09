"""Bounded, parameterized queries through the read-only PostgreSQL account."""
import re
from datetime import date

import pandas as pd
from psycopg import sql
from psycopg.rows import dict_row

from database.cluster import connect

SORTS={'Activity':'total_events','Star actions':'star_events','Intraday score':'trend_score','Attention growth':'attention_growth_ratio','Participation':'active_accounts'}


def current_dataset():
    with connect('reader',autocommit=True) as connection:
        connection.row_factory=dict_row
        row=connection.execute('SELECT v.* FROM devpulse_control.current_dataset c JOIN devpulse_control.dataset_versions v USING(load_id) WHERE c.singleton').fetchone()
        if row is None:raise RuntimeError('No complete PostgreSQL analytics load is available')
        validate_schema(row['schema_name'])
        return row


def validate_schema(name):
    if not re.fullmatch(r'dp_[a-f0-9]{24}',name):raise ValueError('Unrecognized analytics snapshot schema')
    return name


def table(snapshot,name):
    from database.contracts import KEYS
    if name not in KEYS:raise ValueError('Unknown analytics table')
    return sql.SQL('{}.{}').format(sql.Identifier(validate_schema(snapshot['schema_name'])),sql.Identifier(name))


def frame(query,params=()):
    with connect('reader',autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query,params)
            columns=[c.name for c in cursor.description]
            return pd.DataFrame(cursor.fetchall(),columns=columns)


def like(value):
    if len(value)>150:raise ValueError('Search is limited to 150 characters')
    return '%'+value.replace('\\','\\\\').replace('%','\\%').replace('_','\\_')+'%'


def limit_rows(limit):
    if type(limit) is not int or not 1<=limit<=200:raise ValueError('Row limit must be 1–200')
    return limit


def date_bounds(snapshot):
    result=frame(sql.SQL('SELECT min(event_date) AS first,max(event_date) AS last FROM {}').format(table(snapshot,'activity_calendar')))
    return result.iloc[0]['first'],result.iloc[0]['last']


def validate_dates(start,end):
    if not isinstance(start,date) or not isinstance(end,date) or start>end:raise ValueError('Select an ordered date range')


def calendar(snapshot):
    return frame(sql.SQL('SELECT * FROM {} ORDER BY event_date').format(table(snapshot,'activity_calendar')))


def hourly(snapshot):
    return frame(sql.SQL('SELECT * FROM {} ORDER BY event_date,event_hour').format(table(snapshot,'hourly_activity')))


def repository_rankings(snapshot,*,sort='Activity',search='',language='All',minimum=0,limit=20,start=None,end=None):
    if sort not in SORTS or type(minimum) is not int or minimum<0:raise ValueError('Invalid ranking parameters')
    limit_rows(limit)
    name_clause=sql.SQL('r.repo_name ILIKE %s')
    where=[name_clause,sql.SQL('r.total_events >= %s')]
    params=[like(search),minimum]
    if language!='All':
        where.append(sql.SQL("coalesce(m.primary_language,'Not enriched')=%s"));params.append(language)
    if sort in {'Intraday score','Attention growth'}:where.append(sql.SQL('r.growth_eligible'))
    if sort=='Attention growth':where.append(sql.SQL('r.attention_delta>0'))
    if start is not None:
        validate_dates(start,end)
        # Date filtering selects repositories observed in the selected days. Scores
        # and exact whole-window participation retain their explicitly fixed scope.
        where.append(sql.SQL('EXISTS (SELECT 1 FROM {} d WHERE d.repo_id=r.repo_id AND d.event_date BETWEEN %s AND %s)').format(table(snapshot,'repository_daily')))
        params.extend([start,end])
    params.append(limit)
    return frame(sql.SQL("SELECT r.repo_id,r.repo_name,r.total_events,r.star_events,r.fork_events,r.push_events,r.pull_requests_opened,r.issues_opened,r.active_accounts,r.code_participating_accounts,r.previous_attention,r.current_attention,r.attention_growth_ratio,r.attention_change_percent,r.zero_attention_baseline,r.activity_score,r.trend_score,coalesce(m.primary_language,'Not enriched') AS primary_language,coalesce(m.enrichment_status,'not_selected') AS enrichment_status FROM {} r LEFT JOIN {} m USING(repo_id) WHERE {} ORDER BY r.{} DESC,r.repo_id LIMIT %s").format(
        table(snapshot,'repository_metrics'),table(snapshot,'repository_metadata'),sql.SQL(' AND ').join(where),sql.Identifier(SORTS[sort])),params)


def comparison(snapshot,ids):
    if not 2<=len(set(ids))<=4 or any(type(i) is not int or i<=0 for i in ids):raise ValueError('Choose two to four distinct repository IDs')
    result=frame(sql.SQL('SELECT repo_id,repo_name,total_events,star_events,fork_events,push_events,pull_requests_opened,issues_opened,active_accounts,code_participating_accounts,previous_attention,current_attention,attention_growth_ratio,trend_score FROM {} WHERE repo_id=ANY(%s) ORDER BY repo_id').format(table(snapshot,'repository_metrics')),(list(set(ids)),))
    if set(result['repo_id'])!=set(ids):raise ValueError('Repository ID absent from this snapshot')
    return result


def repository_history(snapshot,repo_id):
    return frame(sql.SQL('SELECT event_date,event_hour,total_events,star_events,fork_events,push_events FROM {} WHERE repo_id=%s ORDER BY event_date,event_hour').format(table(snapshot,'repository_hourly')),(repo_id,))


def language_options(snapshot):
    rows=frame(sql.SQL('SELECT DISTINCT primary_language FROM {} ORDER BY primary_language').format(table(snapshot,'repository_metadata')))
    return ['All']+rows['primary_language'].tolist()+['Not enriched']


def languages(snapshot,start,end,weighted=False):
    validate_dates(start,end)
    name='language_weighted_daily' if weighted else 'language_primary_daily'
    return frame(sql.SQL('SELECT * FROM {} WHERE event_date BETWEEN %s AND %s ORDER BY event_date').format(table(snapshot,name)),(start,end))


def technologies(snapshot,start,end):
    validate_dates(start,end)
    return frame(sql.SQL('SELECT * FROM {} WHERE event_date BETWEEN %s AND %s').format(table(snapshot,'technology_daily')),(start,end))


def language_growth(snapshot):
    return frame(sql.SQL('SELECT * FROM {} ORDER BY current_attention DESC').format(table(snapshot,'language_intraday_growth')))


def accounts(snapshot,search='',limit=20):
    limit_rows(limit)
    return frame(sql.SQL('SELECT * FROM {} WHERE actor_login ILIKE %s ORDER BY total_events DESC,actor_id LIMIT %s').format(table(snapshot,'account_metrics')),(like(search),limit))


def account_history(snapshot,actor_id):
    return frame(sql.SQL('SELECT event_date,event_hour,total_events,active_repositories,push_events,pull_requests_opened FROM {} WHERE actor_id=%s ORDER BY event_date,event_hour').format(table(snapshot,'account_hourly')),(actor_id,))


def edges(snapshot,limit=200):
    limit_rows(limit)
    return frame(sql.SQL('SELECT e.*,r.repo_name,a.actor_login FROM {} e JOIN {} r USING(repo_id) JOIN {} a USING(actor_id) ORDER BY participation_events DESC,e.repo_id,e.actor_id LIMIT %s').format(table(snapshot,'participation_edges_sample'),table(snapshot,'repository_metrics'),table(snapshot,'account_metrics')),(limit,))


def monitoring():
    runs=frame('SELECT run_id,gold_snapshot_id,status,started_at,finished_at,copied_rows,duration_seconds,verification_run,error_class FROM devpulse_control.pipeline_runs ORDER BY started_at DESC LIMIT 25')
    state=frame('SELECT pg_database_size(current_database()) AS database_bytes,current_user AS role,now() AS checked_at')
    return runs,state


def predictions():
    return frame('SELECT model_id,repo_id,as_of_utc,horizon_days,predicted_probability FROM devpulse_control.repository_predictions ORDER BY as_of_utc DESC,repo_id LIMIT 100')


def model_experiment():
    with connect('reader',autocommit=True) as connection:
        if connection.execute("SELECT to_regclass('devpulse_ml.current_model')").fetchone()[0] is None:
            return None
        connection.row_factory=dict_row
        return connection.execute('SELECT m.* FROM devpulse_ml.current_model c JOIN devpulse_ml.models m USING(model_id) WHERE c.singleton').fetchone()


def model_forecasts(model_id,search='',limit=20):
    limit_rows(limit)
    return frame('SELECT c.repo_id,c.repo_name,p.as_of_utc,p.horizon_days,p.predicted_probability,c.recent_stars,c.recent_forks,c.recent_pushes,c.recent_events,c.active_accounts FROM devpulse_control.repository_predictions p JOIN devpulse_ml.forecast_context c USING(model_id,repo_id,as_of_utc) WHERE p.model_id=%s AND c.repo_name ILIKE %s ORDER BY p.predicted_probability DESC,c.repo_id LIMIT %s',(model_id,like(search),limit))


def heldout_predictions(model_id,limit=100):
    limit_rows(limit)
    return frame('SELECT repo_id,repo_name,as_of_utc,past_stars,future_stars,actual_label,predicted_probability FROM devpulse_ml.heldout_predictions WHERE model_id=%s ORDER BY predicted_probability DESC,repo_id LIMIT %s',(model_id,limit))
