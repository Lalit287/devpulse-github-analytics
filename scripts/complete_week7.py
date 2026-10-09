"""Run native Week 7 workflows, fault drills, tests and evidence verification."""
import argparse,json,subprocess,sys,time,uuid,xml.etree.ElementTree as ET
from datetime import datetime,timezone
from config.settings import ROOT
from exploration.io import sha256_file,write_json
from orchestration.runtime import api,start as start_airflow
from streaming.broker import start as start_kafka
from scripts.verify_week7 import parity,preservation
EVIDENCE=ROOT/'reports/week7'
GROUPS={'unit_tests':['tests/test_week7_contracts.py'],
        'integration_tests':['tests/test_week7_database.py','tests/test_week7_dashboard.py'],
        'regression_tests':['tests','--ignore=tests/test_week7_contracts.py','--ignore=tests/test_week7_database.py','--ignore=tests/test_week7_dashboard.py']}

def hashes():
    paths=[]
    for folder in ['streaming','orchestration','dashboard','tests']:
        paths+=list((ROOT/folder).rglob('*.py'));paths+=list((ROOT/folder).glob('*.sql'))
    paths+=list((ROOT/'scripts').glob('*week7*.py'))+[ROOT/'analytics/pipeline.py',ROOT/'enrichment/github.py',ROOT/'pyproject.toml']
    return {str(p.relative_to(ROOT)):sha256_file(p) for p in sorted(paths)}

def capture_workflows():
    result=[]
    for dag in ['devpulse_daily_batch','devpulse_historical_replay']:
        detail=api('dags/'+dag)
        for r in api('dags/'+dag+'/dagRuns?limit=100')['dag_runs']:
            tasks=api('dags/'+dag+'/dagRuns/'+r['dag_run_id']+'/taskInstances?limit=100')['task_instances']
            result.append({'dag_id':dag,'dag_run_id':r['dag_run_id'],'state':r['state'],'run_type':r['run_type'],
                'start_date':r['start_date'],'end_date':r['end_date'],'is_paused':detail['is_paused'],
                'schedule':detail['timetable_summary'],'tasks':[{k:t[k] for k in ['task_id','state','try_number','duration','pool']} for t in tasks]})
    write_json(EVIDENCE/'airflow_verification.json',{'status':'passed' if workflow_checks(result) else 'incomplete','runs':result})
    return result

def workflow_checks(runs):
    scheduled=any(r['dag_id']=='devpulse_daily_batch' and r['run_type']=='scheduled' and r['state']=='success' and all(t['state']=='success' for t in r['tasks']) for r in runs)
    retried=any(r['dag_id']=='devpulse_daily_batch' and r['state']=='success' and any(t['task_id']=='collect' and t['try_number']>=2 for t in r['tasks']) for r in runs)
    stream=any(r['dag_id']=='devpulse_historical_replay' and r['state']=='success' and all(t['state']=='success' for t in r['tasks']) for r in runs)
    return scheduled and retried and stream

def trigger(dag,conf=None):
    run_id='week7-'+uuid.uuid4().hex[:12]
    api('dags/'+dag+'/dagRuns','POST',{'dag_run_id':run_id,'logical_date':None,'conf':conf or {}})
    api('dags/'+dag,'PATCH',{'is_paused':False})
    until=time.monotonic()+2400;last=None
    try:
        while time.monotonic()<until:
            r=api('dags/'+dag+'/dagRuns/'+run_id)
            if r['state']!=last:print(f'{dag}: {r["state"]}',flush=True);last=r['state']
            if r['state']=='success':return
            if r['state']=='failed':raise RuntimeError(f'Airflow workflow failed: {dag}; inspect its task logs')
            time.sleep(5)
        raise RuntimeError('Airflow workflow timeout')
    finally:api('dags/'+dag,'PATCH',{'is_paused':True})

def tests():
    before=hashes();result={}
    for name,args in GROUPS.items():
        print('Running '+name,flush=True)
        with (EVIDENCE/f'{name}.log').open('w') as log:
            subprocess.run([sys.executable,'-m','pytest',*args,'-q',f'--junitxml={EVIDENCE / (name+".xml")}'],cwd=ROOT,stdout=log,stderr=subprocess.STDOUT,check=True)
        print((EVIDENCE/f'{name}.log').read_text().strip(),flush=True)
        suites=ET.parse(EVIDENCE/f'{name}.xml').getroot().findall('testsuite')
        counts={k:sum(int(s.attrib.get(k,0)) for s in suites) for k in ['tests','failures','errors','skipped']}
        if counts['failures'] or counts['errors'] or counts['skipped']:raise ValueError('Every required test must pass without skips')
        result[name]=counts
    if before!=hashes():raise ValueError('Code changed while tests ran')
    write_json(EVIDENCE/'test_receipt.json',{'code_hashes':before,'test_results':result,'xml_hashes':{n:sha256_file(EVIDENCE/f'{n}.xml') for n in GROUPS}})
    return result

def finish():
    from scripts.generate_week7_report import generate
    receipt=json.loads((EVIDENCE/'test_receipt.json').read_text())
    if receipt['code_hashes']!=hashes():raise ValueError('Test evidence is stale')
    for name,h in receipt['xml_hashes'].items():
        if sha256_file(EVIDENCE/f'{name}.xml')!=h:raise ValueError('Test XML changed')
    recovery=json.loads((EVIDENCE/'recovery_verification.json').read_text())
    if recovery['status']!='passed':raise ValueError('Native recovery drills incomplete')
    runs=capture_workflows()
    if not workflow_checks(runs):raise ValueError('Scheduled/retried/replay Airflow successes missing')
    errors=api('importErrors?limit=100')
    if errors['total_entries']:raise ValueError('Airflow DAG import errors exist')
    result={'status':'complete','verified_at_utc':datetime.now(timezone.utc).isoformat(),'parity':parity(),
            'recovery':recovery,'airflow_runs':runs,'tests':receipt['test_results'],'preservation':preservation(),'code_hashes':hashes()}
    from dashboard.service import current_dataset
    from spark.snapshots import current_manifest
    if current_dataset()['gold_snapshot_id']!=current_manifest(ROOT/'data/gold')['snapshot_id']:raise ValueError('Original active dashboard dataset was not restored')
    write_json(EVIDENCE/'verification.json',result);generate(result);return result

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--verify-existing',action='store_true',help='Verify already executed native runs and fault drills, then run all tests');a=p.parse_args()
    from scripts.generate_week7_report import generate
    EVIDENCE.mkdir(parents=True,exist_ok=True);write_json(EVIDENCE/'verification.json',{'status':'running'});generate({'status':'running'})
    try:
        start_kafka();start_airflow()
        if not a.verify_existing:
            trigger('devpulse_daily_batch',{'source_date':'2025-01-01','metadata_requests':0,'verify_retry':True})
            trigger('devpulse_historical_replay')
            subprocess.run([sys.executable,'-m','scripts.week7_recovery'],cwd=ROOT,check=True)
        from database.load_analytics import load_analytics
        write_json(EVIDENCE/'original_dataset_restored.json',load_analytics())
        tests();finish()
    except BaseException as e:
        failed={'status':'failed','error_class':type(e).__name__}
        write_json(EVIDENCE/'verification.json',failed);generate(failed);raise
    print('Week 7 complete. Dashboard: http://127.0.0.1:18501/pipeline',flush=True)
    print('Evidence: reports/week7_report.md',flush=True)

if __name__=='__main__':main()
