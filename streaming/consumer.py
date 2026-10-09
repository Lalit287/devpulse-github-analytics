"""Real Spark Kafka microbatches, durable SQL sink and checkpointed offsets."""
import argparse,json,os,sys,time
from pathlib import Path
from pyspark.sql import SparkSession,functions as F,types as T
from config.settings import ROOT
from exploration.spark_demo import configure_java
from exploration.io import write_json
from streaming.broker import ADDRESS
from streaming.dependencies import jars
from streaming.sink import metadata,commit_batch

class InjectedCommitFailure(RuntimeError):pass

def session():
    configure_java();os.environ['SPARK_LOCAL_IP']='127.0.0.1';os.environ['PYSPARK_PYTHON']=sys.executable
    s=(SparkSession.builder.master('local[2]').appName('DevPulse-Kafka-Streaming')
       .config('spark.driver.memory','2g').config('spark.jars',jars())
       .config('spark.driver.host','127.0.0.1').config('spark.driver.bindAddress','127.0.0.1')
       .config('spark.sql.session.timeZone','UTC').config('spark.sql.shuffle.partitions','3')
       .config('spark.ui.enabled','false').config('spark.hadoop.fs.defaultFS','file:///').getOrCreate())
    s.sparkContext.setLogLevel('ERROR');return s

def consume(stream_id,*,checkpoint_name='main',continuous=False,duration=None,fault_after_commit=False):
    import re
    if not re.fullmatch('[a-z0-9.-]{1,80}',checkpoint_name):raise ValueError('Invalid checkpoint name')
    meta=metadata(stream_id)
    if meta['bootstrap_servers']!=ADDRESS:raise ValueError('Stream belongs to another Kafka instance')
    checkpoint=ROOT/'.runtime/streaming'/stream_id/checkpoint_name
    checkpoint.mkdir(parents=True,exist_ok=True)
    contract=checkpoint/'devpulse_contract.json'
    expected={'stream_id':stream_id,'topic':meta['topic'],'source_snapshot':meta['source_snapshot'],'sink_version':'v1','bootstrap_servers':ADDRESS}
    if contract.exists() and json.loads(contract.read_text())!=expected:raise ValueError('Checkpoint belongs to another source or sink contract')
    if not contract.exists():write_json(contract,expected)
    spark=session();receipts=[];fault=checkpoint/'verification_fault_consumed.json'
    def batch(frame,batch_id):
        receipt=commit_batch(stream_id,batch_id,frame.toLocalIterator())
        receipt=json.loads(json.dumps(receipt,default=str))
        receipts.append(receipt)
        write_json(ROOT/'reports/week7/consumer_latest.json',{'stream_id':stream_id,'checkpoint':str(checkpoint),'receipt':receipt})
        if fault_after_commit and not fault.exists() and receipt.get('new_events',0)>0:
            write_json(fault,{'expected_failure':'after_database_commit_before_spark_checkpoint','batch_id':batch_id,'digest':receipt['digest']})
            raise InjectedCommitFailure('Verification: committed SQL, interrupted Spark before checkpoint commit')
    try:
        raw=(spark.readStream.format('kafka').option('kafka.bootstrap.servers',ADDRESS).option('subscribe',meta['topic'])
             .option('startingOffsets','earliest').option('failOnDataLoss','true').option('maxOffsetsPerTrigger',10000).load())
        schema=T.StructType([T.StructField('event_time',T.TimestampType())])
        projected=(raw.withColumn('event',F.from_json(F.col('value').cast('string'),schema))
                   .select('value','topic','partition','offset',F.unix_micros(F.date_trunc('minute',F.col('event.event_time'))).alias('minute_us')))
        writer=projected.writeStream.foreachBatch(batch).option('checkpointLocation',str(checkpoint)).queryName('devpulse_'+stream_id.replace('-','_'))
        q=(writer.trigger(processingTime='5 seconds') if continuous else writer.trigger(availableNow=True)).start()
        if continuous and duration is not None:
            until=time.monotonic()+duration
            while q.isActive and time.monotonic()<until:time.sleep(.5)
            q.stop()
            if q.exception():raise q.exception()
        else:q.awaitTermination()
        progress=[json.loads(p.json) for p in q.recentProgress]
        result={'status':'complete','stream_id':stream_id,'continuous':continuous,'checkpoint':str(checkpoint),'progress':progress,'receipts':receipts}
        write_json(ROOT/'reports/week7/consumers'/f'{stream_id}-{checkpoint_name}-{time.time_ns()}.json',result)
        from database.cluster import connect
        with connect('writer') as c:c.execute("UPDATE devpulse_stream.runs SET status=CASE WHEN unique_events=expected_events THEN 'complete' ELSE 'awaiting_replay' END WHERE stream_id=%s",(stream_id,))
        return result
    finally:spark.stop()

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--stream-id',default='historical-20250101-12');p.add_argument('--checkpoint',default='main');p.add_argument('--continuous',action='store_true');p.add_argument('--duration',type=float);p.add_argument('--fault-after-commit',action='store_true');a=p.parse_args();r=consume(a.stream_id,checkpoint_name=a.checkpoint,continuous=a.continuous,duration=a.duration,fault_after_commit=a.fault_after_commit);print(json.dumps({k:v for k,v in r.items() if k not in ['progress','receipts']},indent=2))
