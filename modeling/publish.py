"""Atomic, idempotent publication of verified historical forecasts and held-out results."""
import json
from datetime import datetime,timezone
from pathlib import Path
from psycopg.types.json import Jsonb
from config.settings import ROOT
from database.cluster import connect
from database.contracts import dataset
from exploration.io import sha256_file,write_json
from spark.snapshots import current_manifest

LOCK=19282933872


def utc_timestamp(value):
    # Arrow's TIMESTAMP_MICROS naive value represents stored UTC, not host time.
    return value.replace(tzinfo=timezone.utc) if isinstance(value,datetime) and value.tzinfo is None else value


def publish(*,fail_before_activation=False):
    manifest=current_manifest(ROOT/'data/models')
    if not manifest or not all(c=='passed' for c in manifest['checks'].values()):
        raise ValueError('A verified completed model snapshot is required')
    from modeling.pipeline import code_hashes
    if manifest['recipe']['code_hashes']!=code_hashes():raise ValueError('Model implementation changed after evaluation')
    identifier=manifest['snapshot_id'];source=ROOT/'data/models/snapshots'/identifier
    digest=sha256_file(source/'manifest.json')
    with connect('writer') as connection:
        if not connection.execute('SELECT pg_try_advisory_xact_lock(%s)',(LOCK,)).fetchone()[0]:raise RuntimeError('Another model publication is active')
        connection.execute(Path(__file__).with_name('schema.sql').read_text())
        existing=connection.execute('SELECT snapshot_sha256 FROM devpulse_ml.models WHERE model_id=%s',(identifier,)).fetchone()
        if existing:
            if existing[0]!=digest:raise ValueError('Published model identity has conflicting evidence')
            forecast_count=connection.execute('SELECT count(*) FROM devpulse_control.repository_predictions WHERE model_id=%s',(identifier,)).fetchone()[0]
            context_count=connection.execute('SELECT count(*) FROM devpulse_ml.forecast_context WHERE model_id=%s',(identifier,)).fetchone()[0]
            heldout_count=connection.execute('SELECT count(*) FROM devpulse_ml.heldout_predictions WHERE model_id=%s',(identifier,)).fetchone()[0]
            if forecast_count!=context_count or forecast_count!=manifest['forecast_rows'] or heldout_count!=manifest['heldout_rows']:
                raise ValueError('Published prediction count mismatch')
            outcome='reused'
        else:
            # A registry row is visible only with the rest of this committed transaction.
            evaluation={k:v for k,v in manifest.items() if k not in {'files','recipe'}}
            evaluation['feature_contract']=manifest['recipe']['features'];evaluation['recipe']=manifest['recipe']
            connection.execute('INSERT INTO devpulse_ml.models(model_id,snapshot_sha256,selected_algorithm,historical_only,evaluation) VALUES(%s,%s,%s,true,%s)',
                               (identifier,digest,manifest['selected_model'],Jsonb(evaluation)))
            forecast=dataset(source/'forecast_predictions')
            with connection.cursor().copy('COPY devpulse_control.repository_predictions(model_id,repo_id,as_of_utc,horizon_days,predicted_probability,model_metrics) FROM STDIN (FORMAT BINARY)') as copy:
                copy.set_types(['text','int8','timestamptz','int4','float8','jsonb'])
                for batch in forecast.scanner(batch_size=4096,use_threads=False).to_batches():
                    for row in batch.to_pylist():
                        copy.write_row((identifier,row['repo_id'],utc_timestamp(row['as_of_utc']),7,row['score'],Jsonb({'experiment_id':identifier,'historical_only':True,'score_calibrated':False})))
            with connection.cursor().copy('COPY devpulse_ml.forecast_context(model_id,repo_id,as_of_utc,repo_name,recent_stars,recent_forks,recent_pushes,recent_events,active_accounts) FROM STDIN (FORMAT BINARY)') as copy:
                copy.set_types(['text','int8','timestamptz','text','int8','int8','int8','int8','int8'])
                for batch in forecast.scanner(batch_size=4096,use_threads=False).to_batches():
                    for r in batch.to_pylist():copy.write_row((identifier,r['repo_id'],utc_timestamp(r['as_of_utc']),r['repo_name'],r['star_events_7d'],r['fork_events_7d'],r['push_events_7d'],r['total_events_7d'],r['active_accounts_7d']))
            heldout=dataset(source/'heldout_predictions')
            with connection.cursor().copy('COPY devpulse_ml.heldout_predictions(model_id,repo_id,as_of_utc,repo_name,past_stars,future_stars,actual_label,predicted_probability) FROM STDIN (FORMAT BINARY)') as copy:
                copy.set_types(['text','int8','timestamptz','text','int8','int8','bool','float8'])
                for batch in heldout.scanner(batch_size=4096,use_threads=False).to_batches():
                    for r in batch.to_pylist():copy.write_row((identifier,r['repo_id'],utc_timestamp(r['as_of_utc']),r['repo_name'],r['star_events_7d'],r['future_star_events_7d'],bool(r['label']),r['score']))
            for table,expected in [('devpulse_control.repository_predictions',manifest['forecast_rows']),('devpulse_ml.forecast_context',manifest['forecast_rows']),('devpulse_ml.heldout_predictions',manifest['heldout_rows'])]:
                from psycopg import sql
                schema,name=table.split('.')
                if connection.execute(sql.SQL('SELECT count(*) FROM {}.{} WHERE model_id=%s').format(sql.Identifier(schema),sql.Identifier(name)),(identifier,)).fetchone()[0]!=expected:
                    raise ValueError('Copied prediction row count mismatch')
            outcome='complete'
        if fail_before_activation:raise RuntimeError('Controlled model publication rollback verification')
        connection.execute('GRANT USAGE ON SCHEMA devpulse_ml TO devpulse_reader')
        connection.execute('GRANT SELECT ON ALL TABLES IN SCHEMA devpulse_ml TO devpulse_reader')
        connection.execute('INSERT INTO devpulse_ml.current_model(singleton,model_id) VALUES(true,%s) ON CONFLICT(singleton) DO UPDATE SET model_id=excluded.model_id',(identifier,))
    result={'status':outcome,'model_id':identifier,'forecast_rows':manifest['forecast_rows'],'heldout_rows':manifest['heldout_rows']}
    write_json(ROOT/'reports/week6/latest_publication.json',result)
    print(json.dumps(result,indent=2));return result


if __name__=='__main__':publish()
