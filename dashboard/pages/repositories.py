import streamlit as st
from dashboard import ui,service

s=ui.snapshot();w=s['facts']['window']
ui.heading('Repository explorer','Rank activity, inspect attention, and compare stable repository IDs.')
with st.expander('Filters',expanded=True):
    a,b,c=st.columns(3)
    with a:ranking=st.selectbox('Rank by',list(service.SORTS),key='repo_sort')
    with b:language=st.selectbox('Current primary language',ui.data(s,'language_options'),key='repo_language')
    with c:minimum=st.number_input('Minimum whole-window events',min_value=0,value=0,step=10,key='repo_minimum')
    a,b=st.columns([2,1])
    with a:search=st.text_input('Repository name contains',max_chars=150,key='repo_search')
    with b:limit=st.selectbox('Rows to show',[10,20,50,100],index=1,key='repo_limit')
    start,end=ui.date_filter(s,'repo_dates')
st.caption('Date filters select repositories observed on those days. Counts and scores describe the complete loaded window.')
st.caption(f"Scores compare equal {w['period_hours']}-hour windows split at {w['split_utc']}. Weights are exploratory; a score is not a prediction probability.")
rows=ui.data(s,'repository_rankings',sort=ranking,search=search,language=language,minimum=int(minimum),limit=limit,start=start,end=end)
ui.show_table(rows,'repo_csv','devpulse_repositories.csv')
if not rows.empty:
    metric=service.SORTS[ranking]
    ui.bar(rows.head(12),metric,'repo_name',f'{ranking} ranking',horizontal=True)
    st.subheader('Compare repositories')
    options={int(r.repo_id):r.repo_name for r in rows.itertuples()}
    ids=st.multiselect('Choose two to four repositories',list(options),default=list(options)[:2],format_func=lambda i:f'{options[i]} · {i}',max_selections=4,key='repo_compare')
    if len(ids)>=2:
        compared=ui.data(s,'comparison',ids=ids)
        st.dataframe(compared,hide_index=True,width='stretch')
        ui.bar(compared,'repo_name','star_events','Observed star actions — whole window')
    else:st.info('Select at least two repositories to compare.')
    chosen=st.selectbox('Repository hourly history',list(options),format_func=lambda i:options[i],key='repo_history')
    history=ui.hour_labels(ui.data(s,'repository_history',repo_id=chosen))
    ui.bar(history,'hour_utc','total_events','Observed hourly events')
    st.caption('Repository history is sparse: only hours with observed events are stored. A zero attention baseline has no percentage change.')
