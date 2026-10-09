"""DevPulse interactive dashboard backed by the read-only PostgreSQL role."""
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))

import psycopg
import streamlit as st
from dashboard import ui

st.set_page_config(page_title='DevPulse',page_icon='◈',layout='wide',initial_sidebar_state='expanded')
st.markdown('''<style>
.stApp {background:#f8fafc;color:#142132;}
[data-testid="stSidebar"] {background:#edf2f7;}
[data-testid="stMetric"] {background:white;border:1px solid #e0e7ef;border-radius:12px;padding:15px;}
[data-testid="stMetricValue"] {font-size:1.7rem;}
.block-container {padding-top:2rem;max-width:1450px;}
h1,h2,h3 {letter-spacing:-.03em;}
</style>''',unsafe_allow_html=True)
st.sidebar.markdown('## ◈ DevPulse')
st.sidebar.caption('Public GitHub activity, made measurable.')
page=st.navigation([
    st.Page('pages/overview.py',title='Activity overview',icon=':material/monitoring:',default=True),
    st.Page('pages/repositories.py',title='Repositories',icon=':material/folder_open:',url_path='repositories'),
    st.Page('pages/languages.py',title='Languages & technology',icon=':material/code:',url_path='languages'),
    st.Page('pages/accounts.py',title='Public accounts',icon=':material/groups:',url_path='accounts'),
    st.Page('pages/predictions.py',title='Popularity prediction',icon=':material/insights:',url_path='predictions'),
    st.Page('pages/pipeline.py',title='Pipeline monitor',icon=':material/settings_input_component:',url_path='pipeline'),
])
try:
    snapshot=ui.snapshot()
except (psycopg.Error,RuntimeError,FileNotFoundError):
    st.title('DevPulse')
    st.info('The PostgreSQL analytics database is not ready yet.')
    st.code('cd ~/Desktop/DevPulse\n.venv/bin/python -m scripts.complete_week5')
    st.caption('Run this in your Mac Terminal to load and verify the real dataset, then refresh this page.')
    st.stop()
st.sidebar.caption('Activity snapshot: '+ui.scope(snapshot))
if st.sidebar.button('Refresh dataset',key='refresh_dataset'):
    st.cache_data.clear();st.rerun()
page.run()
