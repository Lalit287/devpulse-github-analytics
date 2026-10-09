"""Daily bounded archive pipeline and controlled historical Kafka replay."""
from datetime import timedelta
import os,shlex
from pathlib import Path
import pendulum
from airflow.sdk import DAG,Param
from airflow.providers.standard.operators.bash import BashOperator
ROOT=Path(os.environ['DEVPULSE_ROOT'])
PYTHON=shlex.quote(str(ROOT/'.venv/bin/python'))
DEFAULT={'owner':'devpulse','retries':2,'retry_delay':timedelta(seconds=10),'execution_timeout':timedelta(minutes=30),'pool':'devpulse_heavy'}
with DAG('devpulse_daily_batch',schedule='0 8 * * *',start_date=pendulum.datetime(2026,10,7,tz='UTC'),catchup=False,
         max_active_runs=1,max_active_tasks=1,default_args=DEFAULT,tags=['devpulse','week7','batch'],
         params={'source_date':Param('2025-01-01',type=['string','null'],pattern=r'^\d{4}-\d{2}-\d{2}$',description='Historical demo default. Null selects the previous completed UTC date.'),
                 'metadata_requests':Param(0,type='integer',minimum=0,maximum=50),
                 'verify_retry':Param(False,type='boolean')},
         description='Collect 24 verified hours, Spark Silver/Gold, atomically publish PostgreSQL and check parity.') as daily:
    tasks=[]
    for stage in ['collect','etl','gold','publish','check']:
        tasks.append(BashOperator(task_id=stage,bash_command=f'{PYTHON} -m orchestration.jobs {stage}',cwd=str(ROOT),append_env=True,
            env={'DEVPULSE_SOURCE_DATE':'{{ params.source_date }}','DEVPULSE_INTERVAL_END':'{{ data_interval_end.isoformat() if data_interval_end is defined and data_interval_end else "" }}',
                 'DEVPULSE_METADATA_REQUESTS':'{{ params.metadata_requests }}','DEVPULSE_FAIL_ONCE':'{{ "true" if params.verify_retry else "false" }}'}))
    for first,second in zip(tasks,tasks[1:]):first>>second
with DAG('devpulse_historical_replay',schedule=None,start_date=pendulum.datetime(2026,10,7,tz='UTC'),catchup=False,
         max_active_runs=1,max_active_tasks=1,default_args=DEFAULT,tags=['devpulse','week7','stream'],
         description='Replay one real Silver hour at 10,000 events/s, then drain Kafka using a persistent Spark checkpoint.') as replay:
    produce=BashOperator(task_id='produce',bash_command=f'{PYTHON} -m streaming.producer',cwd=str(ROOT),append_env=True)
    consume=BashOperator(task_id='consume',bash_command=f'{PYTHON} -m streaming.consumer',cwd=str(ROOT),append_env=True)
    verify=BashOperator(task_id='verify',bash_command=f'{PYTHON} -m scripts.verify_week7 --stream-only',cwd=str(ROOT),append_env=True)
    produce>>consume>>verify
