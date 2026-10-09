import plotly.graph_objects as go
import streamlit as st
from dashboard import ui

s=ui.snapshot()
ui.heading('Public account participation','Observe activity and repository participation, including automation.')
st.caption('Activity is not human productivity or total developer work. Distinct counts are calculated across the complete loaded window.')
search=st.text_input('Account login contains',max_chars=150,key='account_search')
limit=st.selectbox('Accounts to show',[10,20,50,100],index=1,key='account_limit')
rows=ui.data(s,'accounts',search=search,limit=limit)
ui.show_table(rows,'accounts_csv','devpulse_public_accounts.csv')
if not rows.empty:
    ui.bar(rows.head(12),'total_events','actor_login','Observed account activity',horizontal=True)
    options={int(r.actor_id):r.actor_login for r in rows.itertuples()}
    chosen=st.selectbox('Account hourly history',list(options),format_func=lambda i:options[i],key='account_history')
    history=ui.hour_labels(ui.data(s,'account_history',actor_id=chosen))
    ui.bar(history,'hour_utc','total_events','Public events by UTC hour')
st.subheader('Sampled participation network')
edges=ui.data(s,'edges',limit=100)
if not edges.empty:
    repos=edges[['repo_id','repo_name']].drop_duplicates('repo_id')
    actors=edges[['actor_id','actor_login']].drop_duplicates('actor_id')
    labels=repos['repo_name'].tolist()+actors['actor_login'].tolist()
    repo_positions={int(value):i for i,value in enumerate(repos['repo_id'])}
    actor_positions={int(value):len(repos)+i for i,value in enumerate(actors['actor_id'])}
    sources=[repo_positions[int(value)] for value in edges['repo_id']]
    targets=[actor_positions[int(value)] for value in edges['actor_id']]
    fig=go.Figure(go.Sankey(node=dict(label=labels,pad=10,thickness=12),link=dict(source=sources,target=targets,value=edges['participation_events'])))
    fig.update_layout(height=550,title='100 strongest edges from the bounded participation sample',font_size=10)
    st.plotly_chart(fig,width='stretch')
    with st.expander('Participation edge data'):st.dataframe(edges,hide_index=True,width='stretch')
st.caption('The full sample is 20 star-active repositories with up to 50 accounts each. Edges count public events; this is not a complete social network or proof of direct collaboration.')
