"""UI logic with explicitly synthetic service responses; no PostgreSQL success claim."""
from datetime import date,datetime,timezone
import pandas as pd
import pytest
import streamlit as st
from streamlit.testing.v1 import AppTest
from config.settings import ROOT
from dashboard import service,streaming


@pytest.fixture
def app(monkeypatch):
    st.cache_data.clear()
    monkeypatch.setattr(streaming,'snapshot',lambda:None)
    monkeypatch.setattr(streaming,'airflow_runs',lambda:pd.DataFrame())
    when=date(2025,1,1)
    snapshot={'load_id':'synthetic-fixture','schema_name':'dp_'+'a'*24,'loaded_at':datetime(2025,1,1,tzinfo=timezone.utc),
      'row_counts':{'repository_metrics':2},'audit':{'repository_metrics':{'metric_totals':{'star_events':4}}},
      'facts':{'input_events':10,'repositories':2,'accounts':2,'event_type_counts':{'PushEvent':6,'WatchEvent':4},
      'window':{'start_utc':'2025-01-01T00:00:00+00:00','split_utc':'2025-01-01T12:00:00+00:00','period_hours':12,'archive_hours':24},
      'coverage':{'selected_repositories':2,'successful_repositories':2,'covered_event_fraction':1},
      'source_archive_count':24,'spark_etl_seconds':1,'core_analytics_seconds':1,'gold_snapshot_id':'synthetic',
      'silver_snapshot_id':'synthetic','silver_completed_at_utc':'synthetic','gold_completed_at_utc':'synthetic',
      'raw_compressed_bytes':100,'silver_output_bytes':100,'gold_output_bytes':100}}
    repos=pd.DataFrame([{'repo_id':i,'repo_name':f'fixture/repo{i}','total_events':5,'star_events':2,'fork_events':0,
       'push_events':3,'pull_requests_opened':0,'issues_opened':0,'active_accounts':1,'code_participating_accounts':1,
       'previous_attention':1,'current_attention':1,'attention_growth_ratio':1.,'attention_change_percent':0.,
       'zero_attention_baseline':False,'activity_score':.5,'trend_score':.35,'primary_language':'Rust','enrichment_status':'ok'} for i in [1,2]])
    hourly=pd.DataFrame([{'event_date':when,'event_hour':0,'total_events':10,'star_events':4,'fork_events':0,'push_events':6,'active_repositories':2}])
    monkeypatch.setattr(service,'current_dataset',lambda:snapshot)
    monkeypatch.setattr(service,'date_bounds',lambda s:(when,when))
    monkeypatch.setattr(service,'hourly',lambda s:hourly)
    monkeypatch.setattr(service,'calendar',lambda s:pd.DataFrame([{'event_date':when,'total_events':10}]))
    monkeypatch.setattr(service,'language_options',lambda s:['All','Rust'])
    monkeypatch.setattr(service,'repository_rankings',lambda s,**k:repos.iloc[:0] if k.get('search') else repos)
    monkeypatch.setattr(service,'comparison',lambda s,ids:repos[repos.repo_id.isin(ids)])
    monkeypatch.setattr(service,'repository_history',lambda s,repo_id:hourly)
    monkeypatch.setattr(service,'accounts',lambda s,search='',limit=20:pd.DataFrame([{'actor_id':1,'actor_login':'fixture','total_events':10}]) if not search else pd.DataFrame(columns=['actor_id','actor_login','total_events']))
    monkeypatch.setattr(service,'account_history',lambda s,actor_id:hourly)
    monkeypatch.setattr(service,'edges',lambda s,limit=100:pd.DataFrame([{'repo_id':1,'repo_name':'fixture/repo1','actor_id':1,'actor_login':'fixture','participation_events':5}]))
    monkeypatch.setattr(service,'languages',lambda s,start,end,weighted=False:pd.DataFrame([
       {'event_date':when,('language' if weighted else 'primary_language'):'Rust',('attributed_total_events' if weighted else 'total_events'):10,'enrichment_status':'ok'}]))
    monkeypatch.setattr(service,'technologies',lambda s,start,end:pd.DataFrame([{'technology_category':'Unclassified','total_events':10}]))
    monkeypatch.setattr(service,'language_growth',lambda s:pd.DataFrame([{'primary_language':'Rust','current_attention':2}]))
    monkeypatch.setattr(service,'predictions',lambda:pd.DataFrame())
    monkeypatch.setattr(service,'model_experiment',lambda:None)
    monkeypatch.setattr(service,'monitoring',lambda:(pd.DataFrame(),pd.DataFrame([{'database_bytes':100,'role':'synthetic_reader'}])))
    yield AppTest.from_file(str(ROOT/'dashboard/app.py'),default_timeout=20).run()
    st.cache_data.clear()


def test_six_routes_render_against_synthetic_service(app):
    assert not app.exception
    for path in ['repositories','languages','accounts','predictions','pipeline']:
        app.switch_page(f'pages/{path}.py').run()
        assert not app.exception,path


def test_repository_empty_selection_and_comparison_guards(app):
    app.switch_page('pages/repositories.py').run()
    app.multiselect(key='repo_compare').set_value([]).run()
    assert not app.exception and any('at least two' in i.value for i in app.info)
    app.text_input(key='repo_search').set_value('synthetic-missing').run()
    assert not app.exception and any('No matching' in i.value for i in app.info)


def test_language_toggle_and_empty_account_search(app):
    app.switch_page('pages/languages.py').run()
    app.radio(key='language_mode').set_value('Current code-byte allocation').run()
    assert not app.exception
    app.switch_page('pages/accounts.py').run()
    app.text_input(key='account_search').set_value('synthetic-missing').run()
    assert not app.exception and any('No matching' in i.value for i in app.info)
