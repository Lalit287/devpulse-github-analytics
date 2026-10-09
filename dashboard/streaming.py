"""Read-only streaming and Airflow queries for the pipeline monitor."""
import pandas as pd
from dashboard.service import frame
from database.cluster import connect
MONITOR_VERSION=2

def snapshot():
    exists=frame("SELECT to_regclass('devpulse_stream.runs') AS name").iloc[0]['name']
    if exists is None:return None
    runs=frame("SELECT * FROM devpulse_stream.runs WHERE stream_id LIKE %s OR stream_id LIKE %s ORDER BY CASE WHEN stream_id='historical-20250101-12' THEN 0 ELSE 1 END,created_at DESC LIMIT 1",('historical-%','recovery-%'))
    if runs.empty:return None
    r=runs.iloc[0].to_dict();sid=r['stream_id']
    minute=frame('SELECT * FROM devpulse_stream.minute_activity WHERE stream_id=%s ORDER BY minute_utc',(sid,))
    repositories=frame("""WITH windows AS (
      SELECT repo_id,count(*) FILTER(WHERE event_time > %s::timestamptz-interval '10 minutes') AS recent_events,
      count(*) FILTER(WHERE event_time > %s::timestamptz-interval '10 minutes' AND event_type='WatchEvent' AND payload_action='started') AS recent_stars,
      count(*) FILTER(WHERE event_time > %s::timestamptz-interval '10 minutes' AND event_type='ForkEvent') AS recent_forks,
      count(*) FILTER(WHERE event_time <= %s::timestamptz-interval '10 minutes') AS previous_events,
      (array_agg(repo_name ORDER BY event_time DESC,event_id DESC))[1] AS repo_name
      FROM devpulse_stream.events WHERE stream_id=%s AND event_time > %s::timestamptz-interval '20 minutes' GROUP BY repo_id)
      SELECT repo_name,recent_events,recent_stars,recent_forks,previous_events,round((recent_events+1)::numeric/(previous_events+1),2) AS activity_ratio,
      recent_events>=10 AND (recent_events+1)::numeric/(previous_events+1)>=2 AS activity_spike,repo_id
      FROM windows ORDER BY recent_events DESC,repo_id LIMIT 20""",(r['max_event_time'],r['max_event_time'],r['max_event_time'],r['max_event_time'],sid,r['max_event_time']))
    accounts=frame("SELECT actor_id,(array_agg(actor_login ORDER BY event_time DESC,event_id DESC))[1] AS actor_login,count(*) AS recent_events,max(event_time) AS last_event_utc FROM devpulse_stream.events WHERE stream_id=%s AND event_time>%s::timestamptz-interval '10 minutes' GROUP BY actor_id ORDER BY recent_events DESC,actor_id LIMIT 20",(sid,r['max_event_time']))
    batches=frame('SELECT spark_batch_id,committed_at,input_rows,new_messages,new_events,duplicates,invalid,conflicts,late_events,offset_ranges FROM devpulse_stream.batches WHERE stream_id=%s ORDER BY committed_at DESC LIMIT 10',(sid,))
    return {'run':r,'minute':minute,'repositories':repositories,'accounts':accounts,'batches':batches}

def airflow_runs():
    try:
        with connect('reader',dbname='devpulse_airflow') as c:
            rows=c.execute("SELECT dag_id,run_id,state,run_type,start_date,end_date FROM dag_run WHERE dag_id IN ('devpulse_daily_batch','devpulse_historical_replay') ORDER BY run_after DESC LIMIT 10").fetchall()
            return pd.DataFrame(rows,columns=['Workflow','Run','State','Type','Started UTC','Finished UTC'])
    except Exception:return pd.DataFrame()
