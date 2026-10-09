"""Real Kafka / Spark recovery drills, with deliberately isolated test records."""
import json,subprocess,sys,time,uuid,os,socket
# Own a child broker for restart drills; never signal an unrelated long-running service.
for _port in range(19192,19993,100):
    with socket.socket() as _probe:
        if _probe.connect_ex(("127.0.0.1",_port))!=0:
            os.environ["DEVPULSE_KAFKA_INSTANCE"]=f"recovery-{_port}"
            break
else:raise RuntimeError("No free isolated recovery broker port")
from datetime import datetime,timezone
from confluent_kafka import Consumer,TopicPartition
from config.settings import ROOT
from database.cluster import connect
from exploration.io import write_json
from streaming.broker import start,stop,ADDRESS
from streaming.source import source
from streaming.sink import register,metadata
from streaming.producer import publish
from scripts.verify_week7 import parity
EVIDENCE=ROOT/'reports/week7'

def offsets(topic):
    c=Consumer({'bootstrap.servers':ADDRESS,'group.id':'devpulse-offset-audit','enable.auto.commit':False})
    try:
        partitions=c.list_topics(topic,timeout=10).topics[topic].partitions
        until=time.monotonic()+40
        while True:
            try:return {str(p):list(c.get_watermark_offsets(TopicPartition(topic,p),timeout=5,cached=False)) for p in partitions}
            except Exception:
                if time.monotonic()>=until:raise
                time.sleep(1)
    finally:c.close()

def consume(stream_id,label,*args,expected_failure=False):
    with (EVIDENCE/f'{label}.log').open('w') as log:
        r=subprocess.run([sys.executable,'-m','streaming.consumer','--stream-id',stream_id,*args],cwd=ROOT,stdout=log,stderr=subprocess.STDOUT)
    if expected_failure:
        if r.returncode==0 or 'InjectedCommitFailure' not in (EVIDENCE/f'{label}.log').read_text():raise ValueError('Expected post-commit fault was not observed')
    elif r.returncode:raise RuntimeError(f'Spark consumer failed; inspect reports/week7/{label}.log')

def exercise():
    EVIDENCE.mkdir(parents=True,exist_ok=True)
    stream='recovery-'+uuid.uuid4().hex[:12];topic='devpulse.'+stream
    m,rows=source();register(stream,topic,m)
    near=next(i for i,r in enumerate(rows) if datetime.fromisoformat(r['event_time']).minute==58)
    a=rows[1:20000];b=[r for i,r in enumerate(rows) if i>=20000 and i!=near]
    publish(topic,a);consume(stream,'recovery_phase_a')
    phase_a=metadata(stream)
    if phase_a['unique_events']!=19999:raise ValueError('Initial replay counts differ')
    publish(topic,b+rows[1:1001]);before=offsets(topic);stop();health=start();after=offsets(topic)
    if before!=after:raise ValueError('Kafka restart changed durable offsets')
    write_json(EVIDENCE/'broker_recovery.json',{'status':'passed','before':before,'after':after,'broker':health})
    consume(stream,'recovery_injected_failure','--fault-after-commit',expected_failure=True)
    failed_state=metadata(stream)
    if failed_state['unique_events']<=19999:raise ValueError('Injected crash did not occur after a committed SQL batch')
    consume(stream,'recovery_same_checkpoint')
    recovered=metadata(stream)
    if recovered['unique_events']!=len(rows)-2 or recovered['duplicates']!=1000:raise ValueError('Checkpoint restart lost/double-counted events')
    # Deliver both a genuinely late event and a recent out-of-order event AFTER the high watermark.
    publish(topic,[rows[0],rows[near]])
    consume(stream,'recovery_late_arrivals','--continuous','--duration','20')
    final=metadata(stream)
    if final['unique_events']!=len(rows) or final['late_events']<1 or final['out_of_order_events']<2:raise ValueError('Late/out-of-order correction policy was not exercised')
    checks=parity(stream)
    # A replacement checkpoint re-reads all retained offsets. Both ledgers remain unchanged.
    stable={k:final[k] for k in ['messages','unique_events','duplicates','invalid','conflicts','late_events','out_of_order_events']}
    consume(stream,'recovery_replacement_checkpoint','--checkpoint','replacement')
    repeated=metadata(stream)
    if stable!={k:repeated[k] for k in stable}:raise ValueError('Replacement checkpoint changed counted messages/events')
    # Invalid JSON and conflicting IDs use a separate stream; real evidence remains clean.
    test='contract-'+uuid.uuid4().hex[:12];test_topic='devpulse.'+test;register(test,test_topic,m)
    conflict=dict(rows[0],repo_name='verification/conflicting-name')
    future=dict(rows[2],event_time='2025-01-02T00:00:00+00:00')
    publish(test_topic,[rows[0],rows[0],conflict,b'{not-json',future])
    consume(test,'recovery_invalid_contract')
    invalid=metadata(test)
    if (invalid['messages'],invalid['unique_events'],invalid['duplicates'],invalid['invalid'],invalid['conflicts'])!=(5,1,1,2,1):raise ValueError('Invalid/conflicting-event quarantine counts differ')
    result={'status':'passed','stream_id':stream,'source_events':len(rows),'duplicate_records':1000,'messages':final['messages'],
        'late_events':final['late_events'],'out_of_order_events':final['out_of_order_events'],
        'broker_restart':'passed','post_commit_crash':'passed','same_checkpoint_restart':'passed','replacement_checkpoint':'passed',
        'continuous_microbatch_mode':'passed','invalid_and_conflicting_contracts':'passed','parity':checks,
        'completed_at_utc':datetime.now(timezone.utc).isoformat()}
    write_json(EVIDENCE/'recovery_verification.json',result);return result

def run():
    start()
    try:return exercise()
    finally:stop()

if __name__=='__main__':print(json.dumps(run(),indent=2))
