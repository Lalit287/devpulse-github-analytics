"""Collect, fit, publish, test and verify the real historical Week 6 experiment."""
import argparse
import json
from pathlib import Path
import subprocess
import sys

from config.settings import ROOT
from database.cluster import connect
from exploration.io import sha256_file,write_json

EVIDENCE=ROOT/'reports/week6'
TESTS={'unit_tests':['tests/test_week6_features.py','tests/test_week6_evaluation.py'],
       'integration_tests':['tests/test_week6_integration.py'],
       'regression_tests':['tests','--ignore=tests/test_week6_features.py','--ignore=tests/test_week6_evaluation.py','--ignore=tests/test_week6_integration.py']}


def implementation_hashes():
    paths=[]
    for folder in ['modeling','dashboard','tests']:
        paths+=list((ROOT/folder).rglob('*.py'))
    paths+=list((ROOT/'modeling').glob('*.sql'))
    paths+=list((ROOT/'scripts').glob('*week6*.py'))
    return {str(p.relative_to(ROOT)):sha256_file(p) for p in sorted(paths)}


def publication_state():
    with connect('writer',autocommit=True) as c:
        count=c.execute('SELECT count(*) FROM devpulse_control.repository_predictions').fetchone()[0]
        exists=c.execute("SELECT to_regclass('devpulse_ml.models')").fetchone()[0] is not None
        models=c.execute('SELECT count(*) FROM devpulse_ml.models').fetchone()[0] if exists else 0
        return {'prediction_rows':count,'model_registry_exists':exists,'models':models}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--skip-collection',action='store_true',help='Use the already complete corpus, checking every source/output hash')
    args=parser.parse_args()
    from modeling.corpus import collect
    from modeling.pipeline import run
    from modeling.publish import publish
    from dashboard.run import start
    from scripts.verify_week6 import verify
    from scripts.generate_week6_report import generate
    EVIDENCE.mkdir(parents=True,exist_ok=True)
    write_json(EVIDENCE/'verification.json',{'status':'running'})
    try:
        if not args.skip_collection:collect()
        initial=run()
        repeated=run()
        if initial['snapshot_id']!=repeated['snapshot_id'] or initial['completed_at_utc']!=repeated['completed_at_utc']:
            raise ValueError('Unchanged model rerun did not reuse the completed artifact')
        write_json(EVIDENCE/'model_reuse.json',{'status':'passed','model_id':initial['snapshot_id'],
                                              'completed_at_utc':initial['completed_at_utc']})
        before=publication_state()
        try:publish(fail_before_activation=True)
        except RuntimeError as exc:
            if 'rollback verification' not in str(exc):raise
        else:raise ValueError('Controlled publication failure did not fail')
        after=publication_state()
        if before!=after:raise ValueError('Failed publication changed database state')
        write_json(EVIDENCE/'publication_rollback.json',{'status':'passed','before':before,'after':after})
        write_json(EVIDENCE/'initial_publication.json',publish())
        if publish()['status']!='reused':raise ValueError('Model publication rerun did not reuse data')
        print(start(),flush=True)
        receipt_path=EVIDENCE/'test_receipt.json';receipt_path.unlink(missing_ok=True)
        tested=implementation_hashes()
        for name,arguments in TESTS.items():
            print(f'Running {name}...',flush=True)
            with (EVIDENCE/f'{name}.log').open('w') as output:
                subprocess.run([sys.executable,'-m','pytest',*arguments,'-q',f'--junitxml={EVIDENCE / (name+".xml")}'],
                               cwd=ROOT,stdout=output,stderr=subprocess.STDOUT,check=True)
            print((EVIDENCE/f'{name}.log').read_text().strip(),flush=True)
        if tested!=implementation_hashes():raise ValueError('Implementation changed during verification tests')
        write_json(receipt_path,{'code_hashes':tested,'test_xml_hashes':{name:sha256_file(EVIDENCE/f'{name}.xml') for name in TESTS}})
        verify()
        if not generate():raise ValueError('Week 6 completion report rejected stale evidence')
    except BaseException as exc:
        write_json(EVIDENCE/'verification.json',{'status':'failed','error_class':type(exc).__name__})
        generate();raise
    print('Week 6 complete: evaluated historical experiment and dashboard published.',flush=True)


if __name__=='__main__':main()
