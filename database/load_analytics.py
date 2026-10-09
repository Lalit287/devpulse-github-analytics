"""Bounded binary COPY into immutable PostgreSQL schemas with atomic activation."""
import argparse
import hashlib
import json
import shutil
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.compute as pc
from psycopg import sql
from psycopg.types.json import Jsonb

from config.settings import ROOT
from exploration.io import sha256_file, write_json
from spark.snapshots import current_manifest
from database.cluster import ROLES, connect
from database.contracts import KEYS, dataset, pg_type

VERSION = 1
LOCK_ID = 19282933871


class InjectedLoadFailure(RuntimeError):
    """Controlled rollback drill, never used by the normal CLI."""


def ensure_control():
    path = Path(__file__).with_name("schema.sql")
    digest = sha256_file(path)
    with connect("writer") as connection:
        connection.execute(path.read_text())
        row = connection.execute("SELECT source_sha256 FROM devpulse_control.schema_migrations WHERE version=%s", (VERSION,)).fetchone()
        if row and row[0] != digest:
            raise ValueError("Database control schema changed; an explicit migration is required")
        if not row:
            connection.execute("INSERT INTO devpulse_control.schema_migrations(version,source_sha256) VALUES (%s,%s)", (VERSION,digest))
        connection.execute(sql.SQL("GRANT USAGE ON SCHEMA devpulse_control,devpulse TO {}").format(sql.Identifier(ROLES["reader"])))
        connection.execute(sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA devpulse_control TO {}").format(sql.Identifier(ROLES["reader"])))


def create_table(connection, schema, name, arrow_schema):
    if name not in KEYS:
        raise ValueError("Table is not in the Gold contract")
    pieces=[]
    for field in arrow_schema:
        required=field.name in KEYS[name]
        pieces.append(sql.SQL("{} {} {}").format(sql.Identifier(field.name),sql.SQL(pg_type(field.type)),sql.SQL("NOT NULL" if required else "")))
        if field.name in {"repo_id","actor_id"}:
            pieces.append(sql.SQL("CHECK ({} > 0)").format(sql.Identifier(field.name)))
        if field.name.endswith("_events") or field.name in {"active_accounts","active_repositories","active_days","code_participating_accounts","language_bytes"}:
            pieces.append(sql.SQL("CHECK ({} >= 0)").format(sql.Identifier(field.name)))
        if field.name in {"activity_score","trend_score","byte_share","participation_ratio"}:
            pieces.append(sql.SQL("CHECK ({} BETWEEN 0 AND 1)").format(sql.Identifier(field.name)))
    pieces.append(sql.SQL("PRIMARY KEY ({})").format(sql.SQL(',').join(map(sql.Identifier,KEYS[name]))))
    connection.execute(sql.SQL("CREATE TABLE {}.{} ({})").format(sql.Identifier(schema),sql.Identifier(name),sql.SQL(',').join(pieces)))


def copy_table(connection,schema,name,source,batch_size=4096):
    columns=source.schema.names
    statement=sql.SQL("COPY {}.{} ({}) FROM STDIN (FORMAT BINARY)").format(sql.Identifier(schema),sql.Identifier(name),sql.SQL(',').join(map(sql.Identifier,columns)))
    types=[pg_type(field.type) for field in source.schema]
    rows=0
    metric_names=[field.name for field in source.schema if (field.name.endswith("_events") or field.name.startswith("attributed_")) and pg_type(field.type) in {'int8','float8'}]
    totals={name:0 for name in metric_names}
    samples=[]
    with connection.cursor().copy(statement) as copy:
        copy.set_types(types)
        for batch in source.scanner(batch_size=batch_size,use_threads=False).to_batches():
            values=batch.to_pydict()
            for metric in metric_names:
                result=pc.sum(batch.column(batch.schema.get_field_index(metric))).as_py()
                if result is not None:totals[metric]+=result
            for row in zip(*(values[column] for column in columns)):
                row=tuple(value.replace(tzinfo=timezone.utc) if isinstance(value,datetime) and value.tzinfo is None else value for value in row)
                copy.write_row(row)
                rows+=1
                if len(samples)<5:samples.append(dict(zip(columns,row)))
    return rows,totals,samples


def index_table(connection,schema,name,columns):
    indexes={
        'repository_metrics':[("total_events DESC,repo_id",None),("star_events DESC,repo_id",None),("trend_score DESC,repo_id","growth_eligible"),("attention_growth_ratio DESC,repo_id","growth_eligible AND attention_delta > 0"),("lower(repo_name)",None)],
        'account_metrics':[("total_events DESC,actor_id",None),("lower(actor_login)",None)],
        'repository_daily':[("repo_id,event_date",None)],'repository_hourly':[("repo_id,event_date,event_hour",None)],
        'account_daily':[("actor_id,event_date",None)],'account_hourly':[("actor_id,event_date,event_hour",None)],
    }
    for n,(expression,predicate) in enumerate(indexes.get(name,[])):
        query=sql.SQL("CREATE INDEX {} ON {}.{} ({}) {}").format(sql.Identifier(f'{name}_query_{n}'),sql.Identifier(schema),sql.Identifier(name),sql.SQL(expression),sql.SQL('WHERE '+predicate if predicate else ''))
        connection.execute(query)
    connection.execute(sql.SQL("ANALYZE {}.{}").format(sql.Identifier(schema),sql.Identifier(name)))


def verify_table(connection,schema,name,expected,totals=None,samples=None):
    target=sql.SQL("{}.{}").format(sql.Identifier(schema),sql.Identifier(name))
    count=connection.execute(sql.SQL("SELECT count(*) FROM {}").format(target)).fetchone()[0]
    if count!=expected:raise ValueError(f'PostgreSQL row count mismatch: {name}')
    if totals:
        result=connection.execute(sql.SQL("SELECT {} FROM {}").format(sql.SQL(',').join(sql.SQL('sum({})').format(sql.Identifier(c)) for c in totals),target)).fetchone()
        for (column,expected_total),value in zip(totals.items(),result):
            if value is None or abs(value-expected_total)>max(.01,abs(expected_total)*1e-9):
                raise ValueError(f'PostgreSQL aggregate mismatch: {name}.{column}')
    for sample in samples or []:
        keys=KEYS[name]
        where=sql.SQL(' AND ').join(sql.SQL('{}=%s').format(sql.Identifier(k)) for k in keys)
        actual=connection.execute(sql.SQL('SELECT {} FROM {} WHERE {}').format(sql.SQL(',').join(map(sql.Identifier,sample)),target,where),[sample[k] for k in keys]).fetchone()
        if actual is None:raise ValueError(f'Missing copied sample key: {name}')
        for wanted,got in zip(sample.values(),actual):
            if isinstance(wanted,float):
                if got is None or abs(wanted-got)>max(1e-9,abs(wanted)*1e-12):raise ValueError(f'Copied numeric sample mismatch: {name}')
            elif wanted!=got:raise ValueError(f'Copied field sample mismatch: {name}')
    return {'rows':count,'metric_totals':totals or {},'field_sample_checks':len(samples or [])}


def activate(connection,schema,load_id):
    for name in KEYS:
        connection.execute(sql.SQL('CREATE OR REPLACE VIEW devpulse.{} AS SELECT * FROM {}.{}').format(sql.Identifier(name),sql.Identifier(schema),sql.Identifier(name)))
    for alias,name in {'repository_daily_stats':'repository_daily','developer_daily_stats':'account_daily','language_daily_stats':'language_primary_daily'}.items():
        connection.execute(sql.SQL('CREATE OR REPLACE VIEW devpulse.{} AS SELECT * FROM devpulse.{}').format(sql.Identifier(alias),sql.Identifier(name)))
    connection.execute('CREATE OR REPLACE VIEW devpulse.repositories AS SELECT r.*,m.primary_language,m.enrichment_status,m.current_full_name,m.metadata_fetched_at_utc FROM devpulse.repository_metrics r LEFT JOIN devpulse.repository_metadata m USING(repo_id)')
    connection.execute('CREATE OR REPLACE VIEW devpulse.pipeline_runs AS SELECT * FROM devpulse_control.pipeline_runs')
    connection.execute('CREATE OR REPLACE VIEW devpulse.repository_predictions AS SELECT * FROM devpulse_control.repository_predictions')
    connection.execute(sql.SQL('GRANT USAGE ON SCHEMA {} TO {}').format(sql.Identifier(schema),sql.Identifier(ROLES['reader'])))
    connection.execute(sql.SQL('GRANT SELECT ON ALL TABLES IN SCHEMA {},devpulse TO {}').format(sql.Identifier(schema),sql.Identifier(ROLES['reader'])))
    connection.execute('INSERT INTO devpulse_control.current_dataset(singleton,load_id) VALUES(true,%s) ON CONFLICT(singleton) DO UPDATE SET load_id=excluded.load_id',(load_id,))


def dataset_facts(gold,source,silver_root):
    silver_path=Path(silver_root)/'snapshots'/gold['recipe']['silver_snapshot_id']/'manifest.json'
    silver=json.loads(silver_path.read_text())
    if silver['status']!='complete':raise ValueError('Source Silver evidence incomplete')
    return {'gold_snapshot_id':gold['snapshot_id'],'silver_snapshot_id':silver['snapshot_id'],
            'input_events':gold['input_events'],'repositories':gold['tables']['repository_metrics']['rows'],
            'accounts':gold['tables']['account_metrics']['rows'],'window':gold['recipe']['window'],
            'event_type_counts':silver['event_type_counts'],'coverage':gold['coverage'],
            'source_archive_count':len(silver['recipe']['sources']),
            'raw_compressed_bytes':sum(s['compressed_bytes'] for s in silver['recipe']['sources']),
            'silver_output_bytes':silver['output_bytes'],'gold_output_bytes':gold['output_bytes'],
            'silver_completed_at_utc':silver['completed_at_utc'],'gold_completed_at_utc':gold['completed_at_utc'],
            'spark_etl_seconds':silver['duration_seconds'],'core_analytics_seconds':gold['core_duration_seconds']}


def load_analytics(gold_root=ROOT/'data/gold',silver_root=ROOT/'data/silver',*,verification_nonce=None,fail_after=None):
    if fail_after and (verification_nonce is None or fail_after not in KEYS):
        raise ValueError('Failure injection requires a verification namespace and a supported table')
    started=time.monotonic()
    gold=current_manifest(gold_root)
    if not gold or set(gold['tables'])!=set(KEYS):raise ValueError('A complete supported Gold snapshot is required')
    source=Path(gold_root).resolve()/'snapshots'/gold['snapshot_id']
    recipe={'gold_manifest_sha256':sha256_file(source/'manifest.json'),'version':VERSION,
            'code_hashes':{n:sha256_file(Path(__file__).with_name(n)) for n in ['schema.sql','contracts.py','load_analytics.py']},
            'verification_nonce':verification_nonce}
    load_id=hashlib.sha256(json.dumps(recipe,sort_keys=True).encode()).hexdigest()[:24]
    schema='dp_'+load_id
    ensure_control()
    run_id=uuid.uuid4()
    with connect('writer',autocommit=True) as ledger:
        if not ledger.execute('SELECT pg_try_advisory_lock(%s)',(LOCK_ID,)).fetchone()[0]:
            raise RuntimeError('Another DevPulse loader is active')
        ledger.execute('INSERT INTO devpulse_control.pipeline_runs(run_id,gold_snapshot_id,load_id,status,verification_run) VALUES(%s,%s,%s,%s,%s)',(run_id,gold['snapshot_id'],load_id,'running',verification_nonce is not None))
        copied_rows=0
        try:
            with connect('writer') as connection:
                existing=connection.execute('SELECT row_counts,audit FROM devpulse_control.dataset_versions WHERE load_id=%s',(load_id,)).fetchone()
                if existing:
                    audits=existing[1]
                    for name,measure in gold['tables'].items():
                        verify_table(connection,schema,name,measure['rows'],audits[name]['metric_totals'])
                    activate(connection,schema,load_id)
                    outcome='reused'
                else:
                    if shutil.disk_usage(ROOT).free<2*1024**3:raise ValueError('Need at least 2 GiB free for PostgreSQL data and WAL')
                    connection.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
                    audits={}
                    for name in sorted(KEYS,key=lambda n:(gold['tables'][n]['rows'],n)):
                        arrow=dataset(source/name)
                        create_table(connection,schema,name,arrow.schema)
                        rows,totals,samples=copy_table(connection,schema,name,arrow)
                        if rows!=gold['tables'][name]['rows']:raise ValueError(f'Source COPY row mismatch: {name}')
                        index_table(connection,schema,name,arrow.schema.names)
                        audits[name]=verify_table(connection,schema,name,rows,totals,samples)
                        copied_rows+=rows
                        print(f'Validated {name}: {rows:,} rows',flush=True)
                        if fail_after==name:raise InjectedLoadFailure('Controlled transaction rollback verification')
                    facts=dataset_facts(gold,source,silver_root)
                    connection.execute('INSERT INTO devpulse_control.dataset_versions(load_id,gold_snapshot_id,source_sha256,schema_name,row_counts,facts,audit) VALUES(%s,%s,%s,%s,%s,%s,%s)',
                        (load_id,gold['snapshot_id'],recipe['gold_manifest_sha256'],schema,Jsonb({n:v['rows'] for n,v in gold['tables'].items()}),Jsonb(facts),Jsonb(audits)))
                    activate(connection,schema,load_id)
                    outcome='complete'
            duration=round(time.monotonic()-started,3)
            ledger.execute('UPDATE devpulse_control.pipeline_runs SET status=%s,finished_at=now(),copied_rows=%s,duration_seconds=%s WHERE run_id=%s',(outcome,copied_rows,duration,run_id))
            return {'status':outcome,'run_id':str(run_id),'load_id':load_id,'schema':schema,'gold_snapshot_id':gold['snapshot_id'],
                    'duration_seconds':duration,'copied_rows':copied_rows,'tables':len(KEYS),'audit':audits}
        except BaseException as exc:
            ledger.execute('UPDATE devpulse_control.pipeline_runs SET status=%s,finished_at=now(),duration_seconds=%s,error_class=%s WHERE run_id=%s',('failed',round(time.monotonic()-started,3),type(exc).__name__,run_id))
            raise
        finally:
            ledger.execute('SELECT pg_advisory_unlock(%s)',(LOCK_ID,))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--gold-root',type=Path,default=ROOT/'data/gold')
    parser.add_argument('--silver-root',type=Path,default=ROOT/'data/silver')
    parser.add_argument('--run-report',type=Path,default=ROOT/'reports/week5/latest_load.json')
    args=parser.parse_args()
    result=load_analytics(args.gold_root,args.silver_root)
    write_json(args.run_report,result)
    print(json.dumps({k:v for k,v in result.items() if k!='audit'},indent=2))


if __name__=='__main__':main()
