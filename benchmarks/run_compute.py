"""Three real-day trials per engine/worker count; exact result and executor proof."""
import json,subprocess,sys,time,statistics
from config.settings import ROOT
from deployment.spark_cluster import Cluster
from exploration.io import write_json
EVIDENCE=ROOT/'reports/week8/compute'

def application(label,trial,engine,master='local[4]',cores=4,partitions=32):
    path=EVIDENCE/f'{label}-{trial}.json';log=EVIDENCE/f'{label}-{trial}.log'
    cmd=[sys.executable,'-m','benchmarks.workload','--engine',engine,'--master',master,'--cores',str(cores),'--partitions',str(partitions),'--output',str(path)]
    with log.open('w') as f:subprocess.run(cmd,cwd=ROOT,stdout=f,stderr=subprocess.STDOUT,check=True)
    r=json.loads(path.read_text());r.update(label=label,trial=trial)
    print(label,trial,round(r['query_seconds'],3),'seconds',flush=True);return r

def run():
    EVIDENCE.mkdir(parents=True,exist_ok=True);records=[]
    # Source checksum verification warms the filesystem consistently; do not claim cold-cache results.
    for trial in range(1,4):
        order=['pandas','local4'] if trial%2 else ['local4','pandas']
        for label in order:records.append(application(label,trial,'pandas' if label=='pandas' else 'spark'))
    for trial in range(1,4):
        for workers in ([1,2] if trial%2 else [2,1]):
            c=Cluster('benchmark')
            try:
                state=c.start(workers);r=application(f'{workers}-workers',trial,'spark',c.url,workers*2)
                if len(r['details']['executor_ids_with_tasks'])!=workers:raise ValueError('Expected worker executors did not process tasks')
                r['cluster']=state;records.append(r)
            finally:c.stop_children()
    # Compare a conventional high shuffle count with the bounded query profile.
    for trial in range(1,4):records.append(application('local4-shuffle200',trial,'spark',partitions=200))
    results={json.dumps(r['result'],sort_keys=True) for r in records}
    if len(results)!=1:raise ValueError('Pandas / local Spark / worker Spark / tuned results differ')
    summaries={label:{'trials':3,'median_seconds':statistics.median(r['query_seconds'] for r in records if r['label']==label),
        'min_seconds':min(r['query_seconds'] for r in records if r['label']==label),'max_seconds':max(r['query_seconds'] for r in records if r['label']==label)} for label in sorted({r['label'] for r in records})}
    out={'status':'passed','scope':'one Mac, separate standalone executor JVMs; filesystem cache warmed by checksum verification',
         'records':records,'summaries':summaries,'exact_result':records[0]['result']}
    write_json(ROOT/'reports/week8/compute_benchmark.json',out);return out

if __name__=='__main__':run()
