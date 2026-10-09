"""Strict final-stage gates; incomplete native execution never becomes completion."""
import json,math
from pathlib import Path
from config.settings import ROOT
from exploration.io import sha256_file


def preservation():
    expected=json.loads((ROOT/'reports/week8/preservation_before.json').read_text());changed=[]
    for name,h in expected.items():
        p=ROOT/name
        if not p.is_file() or sha256_file(p)!=h:changed.append(name)
    if changed:raise ValueError('Previous data/evidence changed: '+', '.join(changed))
    return {'status':'passed','files':len(expected),'changed_files':[]}

def valid_seconds(value):
    if not isinstance(value,(float,int)) or isinstance(value,bool) or not math.isfinite(value) or value<=0:raise ValueError('Measured elapsed time must be positive and finite')
    return value

def validate_compute(r,expected_events=3909986):
    if r.get('status')!='passed' or r['exact_result']['input_events']!=expected_events:raise ValueError('Full real-day compute proof missing')
    required=['pandas','local4','1-workers','2-workers','local4-shuffle200']
    for label in required:
        rows=[x for x in r['records'] if x['label']==label]
        if len(rows)!=3:raise ValueError('Each compute configuration requires three trials')
        for row in rows:
            valid_seconds(row['query_seconds'])
            if row['result']!=r['exact_result']:raise ValueError('Compute result hashes/counts differ')
            if label.endswith('-workers') and len(row['details']['executor_ids_with_tasks'])!=int(label[0]):raise ValueError('Executor task proof missing')
    return True

def validate_storage(r):
    if r.get('status')!='passed' or r['input_events']!=3909986:raise ValueError('Storage source scope differs')
    formats=r['formats']
    if set(formats)!={'json','gzip_json','flat','partitioned'}:raise ValueError('Storage experiment incomplete')
    expected=formats['json']['counts']
    for f in formats.values():
        if f['rows']!=223540 or f['counts']!=expected or len(f['trials'])!=3:raise ValueError('Storage query parity/trials failed')
        valid_seconds(f['median_seconds'])
    if formats['partitioned']['candidate_fragments']>=formats['flat']['candidate_fragments']:raise ValueError('Partition pruning proof absent')
    return True

def validate_rates(r):
    if r.get('status')!='passed' or r['events_per_trial']!=20000:raise ValueError('Streaming rate proof missing')
    for rate in [1000,5000,10000]:
        trials=[x for x in r['results'] if x['rate_requested']==rate]
        if len(trials)!=2:raise ValueError('Two trials are required at each replay rate')
        for x in trials:
            if x['events']!=20000 or x['publication']['acknowledged']!=20000 or x['failed_batches']!=0:raise ValueError('Rate trial delivery/accounting failed')
            valid_seconds(x['elapsed_seconds']);valid_seconds(x['p95_commit_delay_seconds'])
    return True

def code_hashes():
    paths=[]
    for folder in ['deployment','benchmarks','dashboard','tests','storage','database','spark','analytics','modeling','streaming','orchestration','enrichment','ingestion','exploration','config']:
        paths+=list((ROOT/folder).rglob('*.py'));paths+=list((ROOT/folder).rglob('*.sql'))
    paths+=list((ROOT/'scripts').glob('*.py'))+[ROOT/'pyproject.toml',ROOT/'.github/workflows/offline-contracts.yml']
    return {str(p.relative_to(ROOT)):sha256_file(p) for p in sorted(paths)}
