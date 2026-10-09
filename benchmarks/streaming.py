"""Concurrent real Kafka/Spark/SQL rate trials with wall-clock commit latency."""
import json,statistics,threading,time,uuid
from pathlib import Path
from pyspark.sql import functions as F,types as T
from config.settings import ROOT
from exploration.io import write_json
from streaming.source import source
from streaming.producer import publish
from streaming.broker import ADDRESS,topic,start
from streaming.consumer import session
from streaming.sink import register,metadata,commit_batch
from database.cluster import connect


def complete_stream(stream_id,events):
    # Only fully accounted, conflict-free experiments can leave the running state.
    with connect('writer') as connection:
        row=connection.execute("UPDATE devpulse_stream.runs SET status='complete' WHERE stream_id=%s AND expected_events=%s AND unique_events=%s AND messages=%s AND duplicates=0 AND invalid=0 AND conflicts=0 RETURNING stream_id",(stream_id,events,events,events)).fetchone()
        if not row:raise ValueError('Streaming rate trial completion accounting failed')


def trial(rate,number,rows,meta):
    sid=f'rate-{rate}-{number}-'+uuid.uuid4().hex[:8];name='devpulse.'+sid
    scope=dict(meta,expected_events=len(rows));register(sid,name,scope);topic(name)
    spark=session();delays=[];receipts=[];failures=[];publication={}
    try:
        spark.range(1).count() # JVM warm-up is excluded consistently for every rate.
        raw=(spark.readStream.format('kafka').option('kafka.bootstrap.servers',ADDRESS).option('subscribe',name)
            .option('startingOffsets','earliest').option('failOnDataLoss','true').option('maxOffsetsPerTrigger',10000).load())
        schema=T.StructType([T.StructField('event_time',T.TimestampType())])
        frame=(raw.withColumn('event',F.from_json(F.col('value').cast('string'),schema))
            .select('value','topic','partition','offset',F.unix_micros(F.date_trunc('minute',F.col('event.event_time'))).alias('minute_us'),F.unix_micros('timestamp').alias('published_us')))
        def batch(df,bid):
            values=list(df.toLocalIterator());receipt=commit_batch(sid,bid,values);committed=time.time()
            delays.extend(max(0,committed-r['published_us']/1e6) for r in values)
            receipts.append(receipt)
        checkpoint=ROOT/'.runtime/streaming-week8'/sid
        q=frame.writeStream.foreachBatch(batch).option('checkpointLocation',str(checkpoint)).trigger(processingTime='2 seconds').start()
        begin=time.perf_counter()
        def produce():
            try:publication.update(publish(name,rows,rate))
            except BaseException as e:failures.append(type(e).__name__)
        producer=threading.Thread(target=produce);producer.start();until=time.monotonic()+180
        while time.monotonic()<until:
            state=metadata(sid)
            if state['unique_events']==len(rows) and not producer.is_alive():break
            if failures:raise RuntimeError('Rate trial producer failed')
            if not q.isActive:raise RuntimeError('Rate trial consumer terminated unexpectedly')
            time.sleep(.2)
        else:raise RuntimeError('Streaming rate trial deadline exceeded')
        producer.join(timeout=10);elapsed=time.perf_counter()-begin;q.stop()
        if q.exception():raise q.exception()
        with connect('reader') as c:
            ids={r[0] for r in c.execute('SELECT event_id FROM devpulse_stream.events WHERE stream_id=%s',(sid,))}
        if ids!={r['event_id'] for r in rows} or len(delays)!=len(rows):raise ValueError('Streaming rate trial event/latency accounting failed')
        sorted_delays=sorted(delays);state=metadata(sid)
        if state['duplicates'] or state['invalid'] or state['conflicts']:raise ValueError('Unexpected rejected rate-trial records')
        result={'stream_id':sid,'rate_requested':rate,'trial':number,'events':len(rows),'publication':publication,
            'elapsed_seconds':elapsed,'effective_end_to_end_events_per_second':len(rows)/elapsed,
            'mean_commit_delay_seconds':statistics.mean(delays),'p95_commit_delay_seconds':sorted_delays[int(.95*(len(delays)-1))],
            'max_commit_delay_seconds':max(delays),'failed_batches':0,'committed_batches':len(receipts),
            'progress':[json.loads(p.json) for p in q.recentProgress]}
        complete_stream(sid,len(rows))
        write_json(ROOT/'reports/week8/rates'/f'{sid}.json',result);print(rate,number,round(elapsed,3),'seconds',flush=True);return result
    finally:spark.stop()

def run():
    start();meta,all_rows=source();rows=all_rows[:20000];results=[]
    # Alternate order to reduce a fixed warm-up bias; source IDs/times stay historical.
    for number in [1,2]:
        for rate in ([1000,5000,10000] if number==1 else [10000,5000,1000]):results.append(trial(rate,number,rows,meta))
    out={'status':'passed','source_snapshot':meta['source_snapshot'],'events_per_trial':len(rows),
        'scope':'first 20,000 time-sorted real events of 2025-01-01 UTC hour 12; warm Spark startup excluded; 2-second microbatches; latency from Kafka CreateTime to SQL commit',
        'results':results}
    write_json(ROOT/'reports/week8/streaming_benchmark.json',out);return out

if __name__=='__main__':run()
