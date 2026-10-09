"""Isolated Airflow 3 environment and project-owned localhost services."""
import argparse,json,os,secrets,signal,socket,subprocess,time
from pathlib import Path
from urllib.parse import quote
from psycopg import sql
from config.settings import ROOT
from database.cluster import connect
from exploration.io import write_json
HOME=ROOT/'.runtime/airflow'
EXE=ROOT/'.venv-airflow/bin/airflow'
PORT=18080

def environment():
    HOME.mkdir(parents=True,exist_ok=True);HOME.chmod(0o700)
    path=HOME/'secrets.json'
    if not path.exists():
        import base64
        write_json(path,{'db_password':secrets.token_urlsafe(32),'jwt':secrets.token_urlsafe(48),'fernet':base64.urlsafe_b64encode(secrets.token_bytes(32)).decode(),'ui_password':secrets.token_urlsafe(20)})
        path.chmod(0o600)
    s=json.loads(path.read_text())
    auth=HOME/'passwords.json'
    if not auth.exists():write_json(auth,{'devpulse':s['ui_password']});auth.chmod(0o600)
    return {**os.environ,'AIRFLOW_HOME':str(HOME),'AIRFLOW__CORE__DAGS_FOLDER':str(ROOT/'orchestration/dags'),
        'AIRFLOW__CORE__EXECUTOR':'LocalExecutor','AIRFLOW__CORE__PARALLELISM':'2','AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG':'1',
        'AIRFLOW__CORE__LOAD_EXAMPLES':'False','AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION':'True',
        'AIRFLOW__CORE__FERNET_KEY':s['fernet'],'AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS':'devpulse:admin',
        'AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE':str(auth),
        'AIRFLOW__DATABASE__SQL_ALCHEMY_CONN':f"postgresql+psycopg2://devpulse_airflow:{quote(s['db_password'])}@127.0.0.1:15432/devpulse_airflow",
        'AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE':'1','AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_OVERFLOW':'0',
        'AIRFLOW__API__BASE_URL':f'http://127.0.0.1:{PORT}','AIRFLOW__API__HOST':'127.0.0.1','AIRFLOW__API__PORT':str(PORT),
        'AIRFLOW__API__WORKERS':'1','AIRFLOW__CORE__EXECUTION_API_SERVER_URL':f'http://127.0.0.1:{PORT}/execution/',
        'AIRFLOW__API_AUTH__JWT_SECRET':s['jwt'],'AIRFLOW__LOGGING__COLORED_CONSOLE_LOG':'False',
        'AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL':'10','AIRFLOW__DAG_PROCESSOR__MIN_FILE_PROCESS_INTERVAL':'10',
        'AIRFLOW__SCHEDULER__MIN_SERIALIZED_DAG_UPDATE_INTERVAL':'5',
        'PYTHONPATH':str(ROOT),'DEVPULSE_ROOT':str(ROOT)}

def cli(*args,timeout=120):
    env=environment()
    if args[:2]==('db','migrate'):
        # Airflow 3.3 uses a second connection for the PostgreSQL migration lock.
        # Leave the long-running services at their bounded one-connection pool.
        env['AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE']='2'
    r=subprocess.run([str(EXE),*args],env=env,cwd=ROOT,capture_output=True,text=True,timeout=timeout)
    if r.returncode:
        # Keep private runtime output out of exceptions (URLs / credentials may appear).
        (HOME/'last_cli_error.log').write_text(r.stdout+r.stderr);(HOME/'last_cli_error.log').chmod(0o600)
        raise RuntimeError(f'Airflow {args[0]} command failed; inspect .runtime/airflow/last_cli_error.log')
    return r.stdout

def initialize():
    env=environment();s=json.loads((HOME/'secrets.json').read_text())
    with connect('admin',dbname='postgres',autocommit=True) as c:
        if not c.execute("SELECT 1 FROM pg_roles WHERE rolname='devpulse_airflow'").fetchone():
            c.execute(sql.SQL('CREATE ROLE devpulse_airflow LOGIN PASSWORD {}').format(sql.Literal(s['db_password'])))
        if not c.execute("SELECT 1 FROM pg_database WHERE datname='devpulse_airflow'").fetchone():c.execute('CREATE DATABASE devpulse_airflow OWNER devpulse_airflow')
        c.execute('REVOKE ALL ON DATABASE devpulse_airflow FROM PUBLIC')
    cli('db','migrate')
    with connect('admin',dbname='postgres',autocommit=True) as c:c.execute('GRANT CONNECT ON DATABASE devpulse_airflow TO devpulse_reader')
    with connect('admin',dbname='devpulse_airflow') as c:
        c.execute('GRANT USAGE ON SCHEMA public TO devpulse_reader');c.execute('GRANT SELECT ON dag_run,task_instance,job,dag TO devpulse_reader')
    cli('pools','set','devpulse_heavy','1','One bounded Spark or ingestion stage at a time')

def api(path,method='GET',payload=None):
    import requests
    s=json.loads((HOME/'secrets.json').read_text())
    base=f'http://127.0.0.1:{PORT}'
    r=requests.post(base+'/auth/token',json={'username':'devpulse','password':s['ui_password']},timeout=10);r.raise_for_status()
    token=r.json()['access_token']
    r=requests.request(method,base+'/api/v2/'+path,headers={'Authorization':'Bearer '+token},json=payload,timeout=15);r.raise_for_status();return r.json()

def status():
    try:
        import requests
        r=requests.get(f'http://127.0.0.1:{PORT}/api/v2/monitor/health',timeout=3);r.raise_for_status();return r.json()
    except Exception as e:return {'status':'unavailable','error_class':type(e).__name__}

def start():
    environment()
    if status().get('metadatabase',{}).get('status')=='healthy':return status()
    with socket.socket() as s:
        if s.connect_ex(('127.0.0.1',PORT))==0:raise RuntimeError('Airflow port is occupied by another service')
    initialize();processes={}
    for name,args in [('api-server',['api-server','--host','127.0.0.1','--port',str(PORT),'--workers','1']),('scheduler',['scheduler']),('dag-processor',['dag-processor'])]:
        with (HOME/f'{name}.log').open('ab') as output:
            p=subprocess.Popen([str(EXE),*args],env=environment(),cwd=ROOT,stdout=output,stderr=subprocess.STDOUT,start_new_session=True)
        processes[name]=p.pid
    write_json(HOME/'processes.json',processes)
    for _ in range(80):
        state=status()
        if state.get('scheduler',{}).get('status')=='healthy' and state.get('dag_processor',{}).get('status')=='healthy':return state
        time.sleep(.5)
    raise RuntimeError('Airflow services not healthy; inspect private runtime logs')

def stop():
    path=HOME/'processes.json'
    if not path.exists():return
    for name,pid in json.loads(path.read_text()).items():
        r=subprocess.run(['ps','-p',str(pid),'-o','command='],capture_output=True,text=True)
        if str(EXE) in r.stdout and name in r.stdout:os.kill(pid,signal.SIGTERM)

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('action',choices=['start','stop','status','initialize']);a=p.parse_args();print(json.dumps(globals()[a.action](),indent=2))
