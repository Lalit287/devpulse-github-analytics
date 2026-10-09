"""One independently measured Pandas or Spark application on the full real day."""
import argparse,json,os,sys,time
from pathlib import Path
from config.settings import ROOT
from exploration.io import write_json
from exploration.spark_demo import configure_java
from benchmarks.common import FIELDS,OUTPUT,dataset,source,digest,rss

def pandas_run(path):
    t=time.perf_counter();df=dataset(path).to_table(columns=FIELDS).to_pandas()
    df['star']=(df.event_type.eq('WatchEvent')&df.payload_action.eq('started')).astype('int64')
    df['fork']=df.event_type.eq('ForkEvent').astype('int64')
    result=df.groupby('repo_id',sort=True).agg(total_events=('actor_id','size'),star_events=('star','sum'),fork_events=('fork','sum'),active_accounts=('actor_id','nunique')).reset_index()
    seconds=time.perf_counter()-t
    d=digest(result[OUTPUT].itertuples(index=False,name=None));return seconds,d,{'pandas_frame_bytes':int(df.memory_usage(deep=True).sum()),'input_rows':len(df)}

def spark_session(master,cores=4,partitions=32,*,executor_memory="1536m",executor_cores=2):
    configure_java();os.environ['SPARK_LOCAL_IP']='127.0.0.1';os.environ['PYSPARK_PYTHON']=sys.executable
    from pyspark.sql import SparkSession
    return (SparkSession.builder.master(master).appName('DevPulse-Week8-Fixed-Workload')
        .config('spark.driver.memory','3g').config('spark.executor.memory',executor_memory).config('spark.executor.cores',str(executor_cores))
        .config('spark.cores.max',str(cores)).config('spark.driver.host','127.0.0.1').config('spark.driver.bindAddress','127.0.0.1')
        .config('spark.sql.session.timeZone','UTC').config('spark.sql.shuffle.partitions',str(partitions))
        .config('spark.sql.adaptive.enabled','true').config('spark.sql.parquet.filterPushdown','true')
        .config('spark.executor.metrics.pollingInterval','1000').config('spark.eventLog.enabled','true')
        .config('spark.eventLog.dir',(ROOT/'.runtime/benchmark-events').as_uri()).config('spark.ui.enabled','true')
        .config('spark.ui.port','18404').config('spark.hadoop.fs.defaultFS','file:///').getOrCreate())

def spark_run(path,master,cores,partitions):
    from pyspark.sql import functions as F
    import requests
    (ROOT/'.runtime/benchmark-events').mkdir(parents=True,exist_ok=True)
    startup=time.perf_counter();spark=spark_session(master,cores,partitions);spark.sparkContext.setLogLevel('ERROR');startup=time.perf_counter()-startup
    try:
        t=time.perf_counter();events=spark.read.parquet(path.as_uri()).select(*FIELDS)
        frame=events.groupBy('repo_id').agg(F.count('*').alias('total_events'),
            F.sum(F.when((F.col('event_type')=='WatchEvent')&(F.col('payload_action')=='started'),1).otherwise(0)).alias('star_events'),
            F.sum(F.when(F.col('event_type')=='ForkEvent',1).otherwise(0)).alias('fork_events'),F.countDistinct('actor_id').alias('active_accounts'))
        # Materialize all output identically into a narrow, sorted driver table.
        rows=frame.select(*OUTPUT).orderBy('repo_id').collect();seconds=time.perf_counter()-t
        d=digest(tuple(r[k] for k in OUTPUT) for r in rows)
        app=spark.sparkContext.applicationId;url=spark.sparkContext.uiWebUrl
        executors=requests.get(f'{url}/api/v1/applications/{app}/executors',timeout=10).json()
        stages=requests.get(f'{url}/api/v1/applications/{app}/stages',timeout=10).json()
        metadata={'application_id':app,'master':master,'startup_seconds':startup,'executors':executors,
            'completed_stages':len([s for s in stages if s['status']=='COMPLETE']),
            'executor_ids_with_tasks':[e['id'] for e in executors if e['id']!='driver' and e.get('completedTasks',0)>0],
            'shuffle_partitions':partitions,'cores_max':cores,'event_log_directory':str(ROOT/'.runtime/benchmark-events')}
        return seconds,d,metadata
    finally:
        gateway=spark.sparkContext._gateway
        spark.stop()
        gateway.shutdown()
        if gateway.proc is not None:
            gateway.proc.terminate()
            gateway.proc.wait(timeout=20)

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--engine',choices=['pandas','spark'],required=True);p.add_argument('--master',default='local[4]');p.add_argument('--cores',type=int,default=4);p.add_argument('--partitions',type=int,default=32);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    m,path=source();start=time.perf_counter()
    seconds,result,extra=pandas_run(path) if a.engine=='pandas' else spark_run(path,a.master,a.cores,a.partitions)
    write_json(a.output,{'status':'passed','engine':a.engine,'source_snapshot':m['snapshot_id'],'source_events':m['clean_events'],
        'query_seconds':seconds,'overall_seconds':time.perf_counter()-start,'result':result,'details':extra,'memory':rss()})
    print(a.engine,round(seconds,3),'seconds;',result,flush=True)
