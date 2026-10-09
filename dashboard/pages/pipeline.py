import streamlit as st
from dashboard import ui,service

s=ui.snapshot();f=s['facts'];runs,state=service.monitoring()
ui.heading('Pipeline monitor','Inspect dataset provenance, measured processing, and database publication.')
ui.cards([('Database snapshot','Ready','Only a fully validated transaction can become current.'),
          ('Database size',f"{int(state.iloc[0]['database_bytes'])/1024**3:.2f} GiB",'PostgreSQL logical database size, including indexes.'),
          ('Loaded Gold tables',str(len(s['row_counts'])),'Aggregates, not raw JSON events.'),
          ('Loaded rows',f"{sum(s['row_counts'].values()):,}",'Total rows across the 15 analytics tables.')])
st.subheader('Measured stages')
ui.cards([('Spark ETL',f"{f['spark_etl_seconds']:.1f} s",'Week 3 transformation and saved-data checks.'),
          ('Core analytics',f"{f['core_analytics_seconds']:.1f} s",'Week 4 aggregation and readback checks.'),
          ('ETL throughput',f"{f['input_events']/f['spark_etl_seconds']:,.0f} events/s",'This measured local run, not a distributed benchmark.'),
          ('Read-only role',str(state.iloc[0]['role']),'The dashboard cannot change analytics tables.')])
st.write({'Gold dataset':f['gold_snapshot_id'],'Silver dataset':f['silver_snapshot_id'],
          'Latest validated ETL':f['silver_completed_at_utc'],'Latest analytics completion':f['gold_completed_at_utc'],
          'Database load time':s['loaded_at'].isoformat(),'Archive hours':f['source_archive_count'],
          'Raw compressed GiB':round(f['raw_compressed_bytes']/1024**3,3),'Silver GiB':round(f['silver_output_bytes']/1024**3,3),
          'Gold MiB':round(f['gold_output_bytes']/1024**2,3)})
st.subheader('Recent database load runs')
if not runs.empty:
    runs=runs.copy();runs['run_id']=runs['run_id'].astype(str)
    st.dataframe(runs,hide_index=True,width='stretch')
    st.caption('Runs marked verification are controlled recovery checks. Failed loads preserve the previous current dataset.')
st.subheader('Table inventory')
st.dataframe([{'Table':n,'Rows':count,'Validation':'Passed'} for n,count in s['row_counts'].items()],hide_index=True,width='stretch')

# This section reads live SQL on each fragment refresh; historical event time stays explicit.
from dashboard import streaming as stream_service
import importlib
if getattr(stream_service,'MONITOR_VERSION',None)!=2:stream_service=importlib.reload(stream_service)
@st.fragment(run_every='5s')
def stream_monitor():
    st.divider()
    st.subheader('Historical Kafka replay')
    data=stream_service.snapshot()
    if data is None:
        st.info('Start a registered Kafka replay to see minute activity and checkpoint progress.')
    else:
        r=data['run']
        st.caption(f"Real GH Archive events · {r['start_utc']:%Y-%m-%d %H:%M}–{r['end_utc']:%H:%M} UTC · refreshes every 5 seconds")
        ui.cards([('Unique events',f"{r['unique_events']:,}",f"{r['expected_events']:,} expected from the verified source hour."),
                  ('Replay state',r['status'].replace('_',' ').title(),f"Last database update: {r['updated_at']:%H:%M:%S} UTC"),
                  ('Duplicates excluded',f"{r['duplicates']:,}",'Repeated event IDs do not increase activity counts.'),
                  ('Late corrections',f"{r['late_events']:,}",'Unique delayed events revise their original UTC minute.')])
        st.write({'Event-time watermark UTC':str(r['watermark']),'Latest event UTC':str(r['max_event_time']),
                  'Out-of-order events':int(r['out_of_order_events']),'Invalid messages':int(r['invalid']),
                  'Conflicting IDs':int(r['conflicts']),'Kafka topic':r['topic']})
        st.caption('Watermark = maximum committed event time minus 10 minutes. Older unique events are retained and correct prior windows. This is a historical replay, not current GitHub activity.')
        chart=data['minute'][['total_events','star_events','fork_events']].copy()
        # Explicit strings keep browser timezone conversion from changing the UTC axis.
        chart.index=data['minute']['minute_utc'].dt.tz_convert('UTC').dt.strftime('%H:%M UTC')
        chart.index.name='Minute (UTC)'
        chart=chart.rename(columns={'total_events':'All events','star_events':'Star actions','fork_events':'Fork actions'})
        st.line_chart(chart,height=260)
        st.subheader('Active repositories in the latest 10 event-time minutes')
        st.dataframe(data['repositories'],hide_index=True,width='stretch')
        st.caption('Activity ratio compares the latest 10 minutes with the preceding 10 minutes, adding 1 to both counts. A spike needs at least 10 recent events and a ratio of 2 or more.')
        with st.expander('Recent active accounts and committed microbatches'):
            st.dataframe(data['accounts'],hide_index=True,width='stretch')
            st.dataframe(data['batches'],hide_index=True,width='stretch')
    st.subheader('Airflow workflow runs')
    runs=stream_service.airflow_runs()
    if runs.empty:st.info('Airflow run history is not available yet.')
    else:st.dataframe(runs,hide_index=True,width='stretch')
    st.caption('Daily batch schedule: 08:00 UTC. The local demonstration uses January 1, 2025 and is paused after verification. Historical replay runs on demand.')
stream_monitor()

from dashboard import evaluation
with st.expander('Performance benchmarks and final project status'):
    evaluation.render()
