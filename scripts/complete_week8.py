"""Complete the bounded final evaluation, native deployment and source handover."""
import argparse,json,subprocess,sys,time,xml.etree.ElementTree as ET
from datetime import datetime,timezone
from config.settings import ROOT
from exploration.io import sha256_file,write_json
from scripts.week8_evidence import preservation,code_hashes,validate_compute,validate_storage,validate_rates
from scripts.generate_week8_report import generate
EVIDENCE=ROOT/'reports/week8'
GROUPS={'unit_tests':['tests/test_week8_contracts.py'],'integration_tests':['tests/test_week8_integration.py'],
        'regression_tests':['tests','--ignore=tests/test_week8_contracts.py','--ignore=tests/test_week8_integration.py']}


def command(module,label):
    with (EVIDENCE/f'{label}.log').open('w') as f:subprocess.run([sys.executable,'-m',module],cwd=ROOT,stdout=f,stderr=subprocess.STDOUT,check=True)

def tests():
    before=code_hashes();results={}
    for name,args in GROUPS.items():
        print('Running '+name,flush=True)
        with (EVIDENCE/f'{name}.log').open('w') as f:subprocess.run([sys.executable,'-m','pytest',*args,'-q',f'--junitxml={EVIDENCE/(name+".xml")}'],cwd=ROOT,stdout=f,stderr=subprocess.STDOUT,check=True)
        print((EVIDENCE/f'{name}.log').read_text().strip(),flush=True)
        suites=ET.parse(EVIDENCE/f'{name}.xml').getroot().findall('testsuite');r={k:sum(int(x.attrib.get(k,0)) for x in suites) for k in ['tests','failures','errors','skipped']}
        if not r['tests'] or any(r[k] for k in ['failures','errors','skipped']):raise ValueError('Every final test must pass without skips')
        results[name]=r
    if code_hashes()!=before:raise ValueError('Implementation changed during final tests')
    write_json(EVIDENCE/'test_receipt.json',{'code_hashes':before,'tests':results,'xml_hashes':{n:sha256_file(EVIDENCE/f'{n}.xml') for n in GROUPS}});return results

def verify():
    from deployment.runtime import status
    from dashboard.service import current_dataset
    from spark.snapshots import current_manifest
    from benchmarks.common import OUTPUT,digest
    from database.contracts import dataset
    from modeling.pipeline import verify_corpus
    preserved=preservation();compute=json.loads((EVIDENCE/'compute_benchmark.json').read_text());storage=json.loads((EVIDENCE/'storage_benchmark.json').read_text());rates=json.loads((EVIDENCE/'streaming_benchmark.json').read_text())
    validate_compute(compute);validate_storage(storage);validate_rates(rates)
    dp=json.loads((EVIDENCE/'distributed_pipeline.json').read_text())
    if dp['status']!='passed' or dp['clean_events']!=3909986 or len(dp['executor_ids_with_tasks'])!=2:raise ValueError('Full distributed pipeline verification is missing')
    if not all(v=='passed' for v in dp['core_checks'].values()):raise ValueError('Distributed core table checks failed')
    services=status()
    if not all(v['status']=='healthy' for v in services.values()):raise ValueError('All final deployment services must be healthy')
    gold=current_manifest(ROOT/'data/gold');model=current_manifest(ROOT/'data/models');served=current_dataset()
    if served['gold_snapshot_id']!=gold['snapshot_id']:raise ValueError('Serving dataset differs from preserved current Gold')
    arrow=dataset(ROOT/'data/gold/snapshots'/gold['snapshot_id']/'repository_metrics').to_table(columns=OUTPUT).sort_by([('repo_id','ascending')])
    if digest(zip(*(arrow.column(k).to_pylist() for k in OUTPUT)))!=compute['exact_result']:raise ValueError('Benchmark differs from actual Gold repository metrics')
    corpus=verify_corpus()
    receipt=json.loads((EVIDENCE/'test_receipt.json').read_text())
    if receipt['code_hashes']!=code_hashes():raise ValueError('Final test code evidence is stale')
    for n,h in receipt['xml_hashes'].items():
        if sha256_file(EVIDENCE/f'{n}.xml')!=h:raise ValueError('Final test receipt XML changed')
    result={'status':'complete','verified_at_utc':datetime.now(timezone.utc).isoformat(),'tests':receipt['tests'],
        'preservation':preserved,'services':services,'gold_snapshot_id':gold['snapshot_id'],'model_id':model['snapshot_id'],
        'model_corpus_hours':corpus['hours'],'benchmark_result':compute['exact_result'],'code_hashes':code_hashes(),
        'evidence_hashes':{n:sha256_file(EVIDENCE/n) for n in ['compute_benchmark.json','storage_benchmark.json','streaming_benchmark.json','distributed_pipeline.json']}}
    write_json(EVIDENCE/'deployment_status.json',{'status':'passed','services':services});write_json(EVIDENCE/'verification.json',result);generate(result)
    return result

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--reuse-benchmarks',action='store_true',help='Reuse completed measured compute/storage/distributed proof; run absent rate trials and all final tests');a=p.parse_args()
    EVIDENCE.mkdir(parents=True,exist_ok=True);write_json(EVIDENCE/'verification.json',{'status':'running'});generate({'status':'running'})
    try:
        from deployment.runtime import start
        print('Starting verified native local deployment...',flush=True);start()
        if not a.reuse_benchmarks:
            command('benchmarks.run_compute','compute_run');command('benchmarks.storage','storage_run');command('deployment.distributed_batch','distributed_pipeline_run')
        elif not all((EVIDENCE/n).exists() for n in ['compute_benchmark.json','storage_benchmark.json','distributed_pipeline.json']):raise ValueError('Required benchmark proof is absent; rerun without --reuse-benchmarks')
        if not a.reuse_benchmarks or not (EVIDENCE/'streaming_benchmark.json').exists():command('benchmarks.streaming','streaming_run')
        from benchmarks.streaming import complete_stream
        rates=json.loads((EVIDENCE/'streaming_benchmark.json').read_text());validate_rates(rates)
        for trial in rates['results']:complete_stream(trial['stream_id'],trial['events'])
        command('benchmarks.figures','figures')
        from database.load_analytics import load_analytics
        from modeling.publish import publish
        write_json(EVIDENCE/'warehouse_reuse.json',load_analytics());write_json(EVIDENCE/'model_publication_reuse.json',publish())
        tests();verify()
        from deployment.package import create
        create()
    except BaseException as e:
        failed={'status':'failed','error_class':type(e).__name__};write_json(EVIDENCE/'verification.json',failed);generate(failed);raise
    print('Week 8 and the local DevPulse project are complete.',flush=True)
    print('Project report: reports/project_report.md\nDashboard: http://127.0.0.1:18501',flush=True)

if __name__=='__main__':main()
