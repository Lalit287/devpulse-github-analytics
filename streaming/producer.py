"""Acknowledged Kafka replay with bounded rate and repository-key ordering."""
import argparse,json,math,time,uuid
from confluent_kafka import Producer
from config.settings import ROOT
from exploration.io import write_json
from streaming.broker import ADDRESS,topic
from streaming.source import source,encode

def publish(name,rows,rate=10000):
    if not math.isfinite(rate) or not 1<=rate<=50000:raise ValueError('Replay rate must be 1–50,000 events/s')
    topic(name);failures=[];acknowledged=0
    p=Producer({'bootstrap.servers':ADDRESS,'enable.idempotence':True,'acks':'all','compression.type':'lz4',
                'linger.ms':5,'delivery.timeout.ms':60000,'queue.buffering.max.messages':20000,'log_level':0})
    def delivered(error,msg):
        nonlocal acknowledged
        if error:failures.append(str(error))
        else:acknowledged+=1
    started=time.monotonic()
    for index,row in enumerate(rows):
        data=encode(row) if isinstance(row,dict) else row
        key=str(row.get('repo_id','invalid')).encode() if isinstance(row,dict) else b'invalid'
        while True:
            try:p.produce(name,value=data,key=key,on_delivery=delivered);break
            except BufferError:p.poll(.05)
        p.poll(0)
        # Fixed-rate pacing prevents unbounded bursts without altering historical timestamps.
        delay=(index+1)/rate-(time.monotonic()-started)
        if delay>0:time.sleep(delay)
    pending=p.flush(60)
    if failures or pending or acknowledged!=len(rows):raise RuntimeError('Not all Kafka records were acknowledged')
    return {'topic':name,'acknowledged':acknowledged,'configured_events_per_second':rate,'elapsed_seconds':round(time.monotonic()-started,3)}

def replay(stream_id='historical-20250101-12',date='2025-01-01',hour=12,rate=10000):
    from streaming.sink import register
    meta,rows=source(date,hour);name='devpulse.'+stream_id
    register(stream_id,name,meta)
    receipt=publish(name,rows,rate);receipt.update(meta,stream_id=stream_id)
    write_json(ROOT/'reports/week7/replays'/f'{uuid.uuid4().hex}.json',receipt)
    return receipt

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--stream-id',default='historical-20250101-12');p.add_argument('--date',default='2025-01-01');p.add_argument('--hour',type=int,default=12);p.add_argument('--rate',type=float,default=10000);a=p.parse_args();print(json.dumps(replay(a.stream_id,a.date,a.hour,a.rate),indent=2))
