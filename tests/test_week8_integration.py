"""Actual final-stage service, dataset and benchmark assertions."""
import json
from pathlib import Path
from config.settings import ROOT
from deployment.runtime import status
from scripts.week8_evidence import validate_compute,validate_storage,validate_rates
from benchmarks.common import digest,OUTPUT
from database.contracts import dataset
from spark.snapshots import current_manifest
from streamlit.testing.v1 import AppTest


def test_all_native_deployment_services_are_healthy():
    services=status();assert all(x['status']=='healthy' for x in services.values())
    assert len(services['spark']['workers'])==2
    assert services['database']['clean_events']==3909986

def test_airflow_migration_preserves_existing_run_history():
    from database.cluster import connect
    from orchestration.runtime import initialize
    def history():
        with connect('reader',dbname='devpulse_airflow') as connection:
            return connection.execute('SELECT dag_id,run_id,state FROM dag_run ORDER BY dag_id,run_id').fetchall()
    before=history()
    assert before, 'The verified Week 7 Airflow runs must remain available'
    initialize()
    assert history()==before


def test_compute_counts_match_the_original_full_gold_warehouse():
    r=json.loads((ROOT/'reports/week8/compute_benchmark.json').read_text());assert validate_compute(r)
    m=current_manifest(ROOT/'data/gold');p=ROOT/'data/gold/snapshots'/m['snapshot_id']/'repository_metrics'
    table=dataset(p).to_table(columns=OUTPUT).sort_by([('repo_id','ascending')])
    assert digest(zip(*(table.column(k).to_pylist() for k in OUTPUT)))==r['exact_result']

def test_storage_and_concurrent_streaming_rate_parity():
    assert validate_storage(json.loads((ROOT/'reports/week8/storage_benchmark.json').read_text()))
    rates=json.loads((ROOT/'reports/week8/streaming_benchmark.json').read_text())
    assert validate_rates(rates)
    from streaming.sink import metadata
    for trial in rates['results']:
        state=metadata(trial['stream_id'])
        assert state['status']=='complete'
        assert state['messages']==state['unique_events']==state['expected_events']==trial['events']
        assert state['source_snapshot']==rates['source_snapshot']

def test_both_workers_executed_full_raw_etl_and_core_analytics():
    r=json.loads((ROOT/'reports/week8/distributed_pipeline.json').read_text())
    assert r['status']=='passed' and r['clean_events']==3909986 and r['duplicates']==7 and r['quarantine']==0
    assert len(r['executor_ids_with_tasks'])==2 and all(v=='passed' for v in r['core_checks'].values())
    assert r['core_tables']['repository_metrics']['rows']==683676 and r['core_tables']['account_metrics']['rows']==419709

def test_final_evaluation_ui_renders_actual_measured_tables():
    app=AppTest.from_file(str(ROOT/'dashboard/app.py'),default_timeout=40).run();app.switch_page('pages/pipeline.py').run()
    assert not app.exception and any(x.value=='Project evaluation' for x in app.subheader)
