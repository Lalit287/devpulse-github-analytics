"""Loopback-only, project-owned Kafka KRaft broker (Java 17)."""
import argparse,json,os,signal,socket,subprocess,time,re
from pathlib import Path
from confluent_kafka.admin import AdminClient,NewTopic
from config.settings import ROOT
from exploration.io import write_json
from exploration.spark_demo import configure_java
HOME=ROOT/'.tools/kafka_2.13-4.2.2'
INSTANCE=os.environ.get('DEVPULSE_KAFKA_INSTANCE','main')
if INSTANCE!='main' and not re.fullmatch(r'recovery-19[1-9]92',INSTANCE):raise ValueError('Unknown project Kafka instance')
BROKER_PORT=19092 if INSTANCE=='main' else int(INSTANCE.split('-')[1])
CONTROLLER_PORT=BROKER_PORT+1
RUNTIME=ROOT/('.runtime/kafka' if INSTANCE=='main' else f'.runtime/kafka-{INSTANCE}')
ADDRESS=f'127.0.0.1:{BROKER_PORT}'

def admin():return AdminClient({'bootstrap.servers':ADDRESS,'socket.timeout.ms':3000,'log_level':0})

def status():
    try:
        m=admin().list_topics(timeout=3)
        marker=json.loads((RUNTIME/'identity.json').read_text())
        if m.cluster_id!=marker['cluster_id']:raise RuntimeError('Foreign Kafka cluster occupies project port')
        return {'status':'healthy','cluster_id':m.cluster_id,'bootstrap_servers':ADDRESS,'brokers':len(m.brokers)}
    except Exception as e:
        return {'status':'unavailable','error_class':type(e).__name__,'bootstrap_servers':ADDRESS}

def initialize():
    if not (HOME/'bin/kafka-server-start.sh').exists():raise RuntimeError('Install the verified Kafka distribution first')
    RUNTIME.mkdir(parents=True,exist_ok=True);RUNTIME.chmod(0o700)
    configure_java()
    marker=RUNTIME/'identity.json';config=RUNTIME/'server.properties'
    if marker.exists():
        value=json.loads(marker.read_text())
        if value['data_directory']!=str(RUNTIME/'data'):raise RuntimeError('Kafka ownership mismatch')
        return
    if (RUNTIME/'data').exists():raise RuntimeError('Unowned Kafka data directory; refusing to format')
    config.write_text('\n'.join(['process.roles=broker,controller','node.id=1',
        f'listeners=PLAINTEXT://127.0.0.1:{BROKER_PORT},CONTROLLER://127.0.0.1:{CONTROLLER_PORT}',
        f'advertised.listeners=PLAINTEXT://127.0.0.1:{BROKER_PORT}','controller.listener.names=CONTROLLER',
        'listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT',
        'inter.broker.listener.name=PLAINTEXT',f'controller.quorum.bootstrap.servers=127.0.0.1:{CONTROLLER_PORT}',
        f'log.dirs={RUNTIME / "data"}','num.partitions=3','offsets.topic.replication.factor=1',
        'transaction.state.log.replication.factor=1','transaction.state.log.min.isr=1',
        'num.network.threads=2','num.io.threads=4','log.retention.hours=24',
        'log.retention.bytes=536870912','log.segment.bytes=67108864','group.initial.rebalance.delay.ms=0'])+'\n')
    env={**os.environ,'KAFKA_HEAP_OPTS':'-Xms128m -Xmx512m'}
    cluster=subprocess.check_output([str(HOME/'bin/kafka-storage.sh'),'random-uuid'],env=env,text=True).strip()
    subprocess.run([str(HOME/'bin/kafka-storage.sh'),'format','--standalone','--cluster-id',cluster,'--config',str(config)],env=env,check=True)
    write_json(marker,{'cluster_id':cluster,'data_directory':str(RUNTIME/'data')})

CHILD=None

def start():
    global CHILD
    if status()['status']=='healthy':return status()
    for port in [BROKER_PORT,CONTROLLER_PORT]:
        with socket.socket() as s:
            if s.connect_ex(('127.0.0.1',port))==0:raise RuntimeError(f'Port {port} occupied by an unverified service')
    initialize();configure_java()
    with (RUNTIME/'broker.log').open('ab') as output:
        p=subprocess.Popen([str(HOME/'bin/kafka-server-start.sh'),str(RUNTIME/'server.properties')],
            cwd=ROOT,env={**os.environ,'KAFKA_HEAP_OPTS':'-Xms128m -Xmx512m'},stdout=output,stderr=subprocess.STDOUT,start_new_session=True)
    CHILD=p
    write_json(RUNTIME/'process.json',{'pid':p.pid,'command':str(HOME),'cluster_id':json.loads((RUNTIME/'identity.json').read_text())['cluster_id']})
    for _ in range(60):
        if status()['status']=='healthy':return status()
        if p.poll() is not None:raise RuntimeError('Kafka exited; inspect .runtime/kafka/broker.log')
        time.sleep(.5)
    raise RuntimeError('Kafka startup timeout')

def stop():
    if status()['status']!='healthy':return {'status':'stopped'}
    value=json.loads((RUNTIME/'process.json').read_text());pid=value['pid']
    identity=json.loads((RUNTIME/'identity.json').read_text())
    if value.get('cluster_id')!=identity['cluster_id'] or value.get('command')!=str(HOME):
        raise RuntimeError('Kafka process ownership cannot be verified')
    os.kill(pid,signal.SIGTERM)
    if CHILD is not None and CHILD.pid==pid:
        CHILD.wait(timeout=30)
        return {'status':'stopped'}
    for _ in range(40):
        with socket.socket() as s:
            if s.connect_ex(('127.0.0.1',BROKER_PORT))!=0:return {'status':'stopped'}
        time.sleep(.5)
    raise RuntimeError('Kafka graceful stop timeout')

def topic(name):
    import re
    if not re.fullmatch(r'devpulse\.[a-z0-9.-]{1,100}',name):raise ValueError('Topic must belong to devpulse namespace')
    a=admin();m=a.list_topics(timeout=5)
    if name not in m.topics:
        for f in a.create_topics([NewTopic(name,num_partitions=3,replication_factor=1,config={'retention.ms':'86400000','retention.bytes':'536870912'})]).values():f.result(10)
    return name

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('action',choices=['start','stop','status']);a=p.parse_args();print(json.dumps(globals()[a.action](),indent=2))
