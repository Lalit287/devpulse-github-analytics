"""Actual read-only dashboard queries and native monitor rendering."""
import streamlit as st
from streamlit.testing.v1 import AppTest
from dashboard import streaming
from config.settings import ROOT

def test_monitor_queries_return_complete_real_hour_and_bounded_rankings():
    d=streaming.snapshot();assert d['run']['stream_id']=='historical-20250101-12'
    assert d['run']['unique_events']==223540 and len(d['minute'])==60
    assert d['minute'].total_events.sum()==223540
    assert len(d['repositories'])<=20 and len(d['accounts'])<=20 and len(d['batches'])<=10
    assert not streaming.airflow_runs().empty

def test_live_monitor_renders_without_exceptions():
    st.cache_data.clear()
    app=AppTest.from_file(str(ROOT/'dashboard/app.py'),default_timeout=40).run()
    app.switch_page('pages/pipeline.py').run();assert not app.exception
    assert any('Historical Kafka replay'==x.value for x in app.subheader)
    assert any('Airflow workflow runs'==x.value for x in app.subheader)
    st.cache_data.clear()
