import pandas as pd
import streamlit as st
from dashboard import ui

s=ui.snapshot();facts=s['facts']
ui.heading('GitHub activity overview',ui.scope(s))
st.markdown('Explore the complete observed day, from repository attention to public account participation.')
types=facts['event_type_counts']
ui.cards([('Clean events',f"{facts['input_events']:,}",'Unique event IDs after validation and deduplication.'),
          ('Repositories',f"{facts['repositories']:,}",'Distinct stable repository IDs.'),
          ('Public accounts',f"{facts['accounts']:,}",'Includes bots and automation.'),
          ('Push events',f"{types.get('PushEvent',0):,}",'Push records, not individual commits.')])
ui.cards([('Star actions',f"{sum(r['metric_totals'].get('star_events',0) for name,r in s['audit'].items() if name=='repository_metrics'):,}",'Observed WatchEvent actions, not current star inventory.'),
          ('Fork events',f"{types.get('ForkEvent',0):,}",'Observed public fork records.'),
          ('Archive coverage',f"{facts['source_archive_count']} hours",'Contiguous source collection.'),
          ('Metadata coverage',f"{facts['coverage']['covered_event_fraction']*100:.2f}%",'Events associated with selected current metadata.')])
st.subheader('Activity through the day')
hourly=ui.hour_labels(ui.data(s,'hourly'))
ui.bar(hourly,'hour_utc','total_events','Clean events by UTC hour')
left,right=st.columns(2)
with left:
    distribution=pd.DataFrame([{'Event type':k,'Events':v} for k,v in types.items()]).sort_values('Events',ascending=False)
    ui.bar(distribution,'Events','Event type','Event distribution',horizontal=True)
with right:
    st.subheader('Observed calendar')
    st.dataframe(ui.data(s,'calendar'),hide_index=True,width='stretch')
    st.info('This dataset covers one Wednesday. Weekend and weekly trends need more observed days.')
    st.markdown('**Interpretation**\n\nPublic activity includes automation. Event counts measure observations, not total developer work or productivity.')
