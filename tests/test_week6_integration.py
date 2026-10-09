"""Real evaluated artifacts, PostgreSQL serving, idempotency and dashboard behavior."""
import json
from datetime import datetime,timezone
from pathlib import Path
import pytest
import pyarrow.compute as pc
from psycopg import errors
from streamlit.testing.v1 import AppTest

from config.settings import ROOT
from database.cluster import connect
from database.contracts import dataset
from dashboard import service
from modeling.pipeline import OUTPUT,code_hashes
from modeling.publish import publish
from spark.snapshots import current_manifest


@pytest.fixture(scope='module')
def manifest():
    m=current_manifest(OUTPUT)
    assert m and m['recipe']['code_hashes']==code_hashes()
    return m


def test_real_split_recipe_and_artifact_accounting(manifest):
    assert manifest['corpus']['hours']==1008 and manifest['corpus']['days']==42
    assert manifest['clean_events']>1_000_000
    assert manifest['corpus']['projected_rows']==manifest['clean_events']+manifest['duplicate_rows']
    assert manifest['clean_events']==manifest['window_accounted_events']+manifest['outside_source_week_events']
    assert set(manifest['models'])=={'logistic_regression','random_forest','gradient_boosted_trees'}
    assert manifest['splits']['train']['last_label_end']=='2015-01-29T00:00:00Z'
    assert manifest['splits']['test']['first_as_of']=='2015-02-05T00:00:00Z'
    chosen=max(manifest['models'],key=lambda n:(manifest['models'][n]['validation']['pr_auc'],manifest['models'][n]['validation']['precision_at_10'],n))
    assert manifest['selected_model']==chosen
    target=OUTPUT/'snapshots'/manifest['snapshot_id']
    forecasts=dataset(target/'forecast_predictions').to_table()
    assert len(forecasts)==manifest['forecast_rows']
    assert forecasts['label'].null_count==len(forecasts)
    assert forecasts['future_star_events_7d'].null_count==len(forecasts)
    heldout=dataset(target/'heldout_predictions').to_table()
    assert len(heldout)==manifest['heldout_rows']
    assert pc.sum(heldout['label']).as_py()==manifest['models'][chosen]['test']['positives']
    positive=pc.equal(heldout['label'],1.)
    predicted=pc.greater_equal(heldout['score'],manifest['decision_threshold'])
    expected=manifest['models'][chosen]['test']
    assert pc.sum(pc.cast(pc.and_(positive,predicted),'int64')).as_py()==expected['tp']
    assert pc.sum(pc.cast(pc.and_(pc.invert(positive),predicted),'int64')).as_py()==expected['fp']
    top=heldout.sort_by([('score','descending'),('repo_id','ascending')]).slice(0,10)
    assert pc.mean(top['label']).as_py()==pytest.approx(expected['precision_at_10'])


def test_postgresql_full_prediction_parity_and_utc(manifest):
    model=manifest['snapshot_id']
    with connect('reader') as c:
        assert c.execute('SELECT model_id FROM devpulse_ml.current_model').fetchone()[0]==model
        assert c.execute('SELECT count(*) FROM devpulse_control.repository_predictions WHERE model_id=%s',(model,)).fetchone()[0]==manifest['forecast_rows']
        assert c.execute('SELECT count(*) FROM devpulse_ml.heldout_predictions WHERE model_id=%s',(model,)).fetchone()[0]==manifest['heldout_rows']
        assert c.execute('SELECT min(as_of_utc),max(as_of_utc) FROM devpulse_control.repository_predictions WHERE model_id=%s',(model,)).fetchone()==(datetime(2015,2,12,tzinfo=timezone.utc),)*2
        sums=c.execute('SELECT sum(predicted_probability),min(predicted_probability),max(predicted_probability) FROM devpulse_control.repository_predictions WHERE model_id=%s',(model,)).fetchone()
        scores=dataset(OUTPUT/'snapshots'/model/'forecast_predictions').to_table(columns=['score'])['score']
        assert float(sums[0])==pytest.approx(pc.sum(scores).as_py(),abs=1e-6)
        assert sums[1]>=0 and sums[2]<=1
    with connect('reader') as c:
        c.execute('SET TRANSACTION READ WRITE')
        with pytest.raises(errors.InsufficientPrivilege):c.execute('UPDATE devpulse_ml.current_model SET model_id=model_id WHERE false')


def test_publication_reuse_and_rollback_preserve_pointer(manifest):
    before=service.model_experiment()['model_id']
    assert publish()['status']=='reused'
    with pytest.raises(RuntimeError,match='rollback verification'):publish(fail_before_activation=True)
    assert service.model_experiment()['model_id']==before==manifest['snapshot_id']


def test_literal_search_and_bounded_forecasts(manifest):
    rows=service.model_forecasts(manifest['snapshot_id'],limit=10)
    assert len(rows)==10 and rows['predicted_probability'].is_monotonic_decreasing
    assert service.model_forecasts(manifest['snapshot_id'],search="' OR 1=1 --").empty
    with pytest.raises(ValueError):service.model_forecasts(manifest['snapshot_id'],limit=10000)


def test_real_prediction_page_controls_and_evaluation(manifest):
    app=AppTest.from_file(str(ROOT/'dashboard/app.py'),default_timeout=30).run()
    app.switch_page('pages/predictions.py').run()
    assert not app.exception
    assert any('Historical experiment' in w.value for w in app.warning)
    assert len(app.dataframe)>=3
    assert len(app.dataframe[0].value)==20
    app.selectbox(key='prediction_limit').set_value(10).run()
    assert not app.exception and len(app.dataframe[0].value)==10
    app.text_input(key='prediction_search').set_value('missing-repo__verification__').run()
    assert not app.exception and any('No matching' in i.value for i in app.info)
