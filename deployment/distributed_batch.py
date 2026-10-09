"""Execute full-day ETL and core analytics on two actual standalone workers."""
import json,time
from pathlib import Path
import requests
from config.settings import ROOT
from exploration.io import write_json
from deployment.spark_cluster import Cluster
from benchmarks.workload import spark_session
from benchmarks.common import source
from spark.sources import plan_sources
from spark.clean_events import run_etl
from spark.snapshots import current_manifest,verify_snapshot
from analytics.core import build_core

WORK=ROOT/'work/week8/distributed'

def run():
    c=Cluster('benchmark',worker_memory='4g');spark=None;started=time.perf_counter();original,_=source()
    try:
        cluster=c.start(2);(ROOT/'.runtime/benchmark-events').mkdir(parents=True,exist_ok=True)
        spark=spark_session(c.url,2,32,executor_memory='3g',executor_cores=1)
        spark.conf.set('spark.sql.inMemoryColumnarStorage.batchSize','1000')
        spark.sparkContext.setLogLevel('ERROR')
        inputs=plan_sources(ROOT/'data/metadata/latest_collection.json',backend='local')
        etl=run_etl(inputs,output_root=WORK/'silver',session=spark,shuffle_partitions=32)
        silver=current_manifest(WORK/'silver')
        if (silver['clean_events'],silver['duplicate_rows'],silver['quarantined_rows'])!=(original['clean_events'],original['duplicate_rows'],original['quarantined_rows']):raise ValueError('Distributed ETL differs from original validated accounting')
        (WORK/'core/snapshots').mkdir(parents=True,exist_ok=True)
        path=build_core(WORK/'silver',WORK/'core',session=spark);core=verify_snapshot(path)
        app=spark.sparkContext.applicationId;url=spark.sparkContext.uiWebUrl
        executors=requests.get(f'{url}/api/v1/applications/{app}/executors',timeout=10).json()
        active=[e['id'] for e in executors if e['id']!='driver' and e.get('completedTasks',0)>0]
        if len(active)!=2:raise ValueError('Both executor JVMs must process real ETL tasks')
        result={'status':'passed','elapsed_seconds':time.perf_counter()-started,'cluster':cluster,'application_id':app,
            'executors':executors,'executor_ids_with_tasks':active,'etl':etl,'silver_snapshot_id':silver['snapshot_id'],
            'clean_events':silver['clean_events'],'duplicates':silver['duplicate_rows'],'quarantine':silver['quarantined_rows'],
            'etl_profile':{'executor_memory':'3g','executor_cores':1,'total_cores':2,'cache_batch_size':1000},'core_snapshot_id':core['snapshot_id'],'core_tables':core['tables'],'core_checks':core['checks']}
        write_json(ROOT/'reports/week8/distributed_pipeline.json',result);return result
    except BaseException as exc:
        write_json(ROOT/'reports/week8/distributed_pipeline_failure.json',{'status':'failed','error_class':type(exc).__name__,'error':str(exc)[:3000]})
        raise
    finally:
        if spark is not None:
            gateway=spark.sparkContext._gateway;spark.stop();gateway.shutdown()
            if gateway.proc is not None:gateway.proc.terminate();gateway.proc.wait(timeout=20)
        c.stop_children()

if __name__=='__main__':print(json.dumps(run(),indent=2))
