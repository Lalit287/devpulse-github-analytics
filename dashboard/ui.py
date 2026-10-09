"""Shared visual presentation and snapshot-keyed query caches."""
import pandas as pd
import plotly.express as px
import streamlit as st

from dashboard import service


@st.cache_data(ttl=15,max_entries=4,show_spinner=False)
def snapshot():return service.current_dataset()


@st.cache_data(ttl=60,max_entries=128,show_spinner=False)
def data(snapshot,name,**kwargs):
    return getattr(service,name)(snapshot,**kwargs)


def heading(title,description):
    st.title(title)
    st.caption(description)


def cards(items):
    for column,(label,value,help_text) in zip(st.columns(len(items)),items):
        column.metric(label,value,help=help_text)


def scope(snapshot):
    w=snapshot['facts']['window']
    return f"{w['start_utc'][:10]} · {w['archive_hours']} complete archive hours · UTC"


def date_filter(snapshot,key):
    first,last=data(snapshot,'date_bounds')
    value=st.date_input('Observed dates',(first,last),min_value=first,max_value=last,key=key)
    if len(value)!=2:
        st.info('Select both the start and end date.');st.stop()
    return value


def show_table(frame,key,filename):
    if frame.empty:
        st.info('No matching activity in this selection.');return
    st.dataframe(frame,hide_index=True,width='stretch')
    st.download_button('Download these rows',frame.to_csv(index=False).encode('utf-8'),file_name=filename,mime='text/csv',key=key)


def bar(frame,x,y,title,horizontal=False,color=None):
    if frame.empty:
        st.info('No matching chart data.');return
    chart=px.bar(frame,x=x,y=y,color=color,orientation='h' if horizontal else 'v',title=title,
                 template='plotly_white',color_discrete_sequence=['#00b894','#5665d6','#faaf47','#c06bda'])
    chart.update_layout(margin=dict(l=10,r=10,t=45,b=10),font=dict(family='Arial'),legend_title_text='')
    if horizontal:chart.update_yaxes(autorange='reversed')
    st.plotly_chart(chart,width='stretch')


def hour_labels(frame):
    copy=frame.copy()
    copy['hour_utc']=pd.to_datetime(copy['event_date'])+pd.to_timedelta(copy['event_hour'],unit='h')
    return copy
