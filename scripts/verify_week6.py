"""Verify source preservation, evaluated artifacts, native serving and current tests."""
import json
from pathlib import Path
import xml.etree.ElementTree as ET
from datetime import datetime,timezone

from config.settings import ROOT
from exploration.io import sha256_file,write_json
from modeling.pipeline import OUTPUT,code_hashes,verify_corpus
from spark.snapshots import current_manifest
from dashboard import service
from dashboard.run import healthy

EVIDENCE=ROOT/'reports/week6'


def test_summary(path):
    suites=list(ET.parse(path).getroot().iter('testsuite'))
    return {k:sum(int(s.get(k,0)) for s in suites) for k in ['tests','failures','errors','skipped']}


def protected_sources():
    saved=json.loads((EVIDENCE/'previous_weeks_preservation.json').read_text())
    changed=[name for name,digest in saved.items() if not (ROOT/name).exists() or sha256_file(ROOT/name)!=digest]
    if changed:raise ValueError('Earlier sources or reports changed: '+', '.join(changed))
    return len(saved)


def verify():
    write_json(EVIDENCE/'verification.json',{'status':'running'})
    corpus=verify_corpus();manifest=current_manifest(OUTPUT)
    if not manifest or manifest['recipe']['code_hashes']!=code_hashes():raise ValueError('Missing/current model evidence mismatch')
    if manifest['recipe']['corpus_id']!=corpus['corpus_id']:raise ValueError('Historical corpus changed after fitting')
    tests={name:test_summary(EVIDENCE/f'{name}.xml') for name in ['unit_tests','integration_tests','regression_tests']}
    from scripts.complete_week6 import implementation_hashes
    receipt=json.loads((EVIDENCE/'test_receipt.json').read_text())
    if receipt['code_hashes']!=implementation_hashes() or receipt['test_xml_hashes']!={n:sha256_file(EVIDENCE/f'{n}.xml') for n in tests}:
        raise ValueError('Week 6 tests are stale relative to implementation')
    for name,value in tests.items():
        if value['tests']<{'unit_tests':8,'integration_tests':5,'regression_tests':155}[name] or any(value[k] for k in ['failures','errors','skipped']):
            raise ValueError(f'Week 6 tests incomplete: {name}')
    if not healthy():raise ValueError('Dashboard HTTP health check failed')
    rollback=json.loads((EVIDENCE/'publication_rollback.json').read_text())
    if rollback['status']!='passed' or rollback['before']!=rollback['after']:
        raise ValueError('Initial publication rollback evidence failed')
    reuse=json.loads((EVIDENCE/'model_reuse.json').read_text())
    if reuse['status']!='passed' or reuse['model_id']!=manifest['snapshot_id']:
        raise ValueError('Immutable model reuse evidence missing')
    experiment=service.model_experiment()
    if not experiment or experiment['model_id']!=manifest['snapshot_id']:raise ValueError('Published model differs from verified artifact')
    if service.current_dataset()['gold_snapshot_id']!='6e03839285499224b0403539':raise ValueError('Original 2025 activity dataset changed')
    with __import__('database.cluster',fromlist=['connect']).connect('reader') as c:
        row=c.execute('SELECT snapshot_sha256 FROM devpulse_ml.models WHERE model_id=%s',(manifest['snapshot_id'],)).fetchone()
        if row[0]!=sha256_file(OUTPUT/'snapshots'/manifest['snapshot_id']/'manifest.json'):raise ValueError('SQL model checksum differs')
        role=c.execute('SELECT current_user').fetchone()[0]
    checks={'status':'complete','verified_at_utc':datetime.now(timezone.utc).isoformat(),'model_id':manifest['snapshot_id'],
            'corpus_id':corpus['corpus_id'],'protected_files':protected_sources(),'tests':tests,'reader_role':role,
            'model_code_hashes':code_hashes(),'implementation_hashes':implementation_hashes(),
            'test_xml_hashes':{n:sha256_file(EVIDENCE/f'{n}.xml') for n in tests}}
    write_json(EVIDENCE/'verification.json',checks)
    print(json.dumps({k:v for k,v in checks.items() if k!='model_code_hashes'},indent=2));return checks


if __name__=='__main__':verify()
