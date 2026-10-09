"""Install pinned, isolated Week 7 runtimes from official sources."""
import hashlib,json,subprocess,sys,tarfile
from pathlib import Path
import requests
from config.settings import ROOT
from exploration.io import write_json
from streaming.dependencies import install

def setup():
    tools=ROOT/'.tools';tools.mkdir(exist_ok=True)
    name='kafka_2.13-4.2.2.tgz';p=tools/name;url='https://downloads.apache.org/kafka/4.2.2/'+name
    if not p.exists():
        with requests.get(url,stream=True,timeout=(10,60)) as r:
            r.raise_for_status()
            with p.with_suffix('.part').open('wb') as f:
                size=0
                for chunk in r.iter_content(1024**2):
                    size+=len(chunk)
                    if size>200*1024**2:raise ValueError('Kafka archive exceeds installation bound')
                    f.write(chunk)
        p.with_suffix('.part').replace(p)
    r=requests.get(url+'.sha512',timeout=30);r.raise_for_status();expected=''.join(r.text.split(':',1)[1].split()).lower()
    actual=hashlib.sha512(p.read_bytes()).hexdigest()
    if actual!=expected:raise ValueError('Official Kafka SHA-512 mismatch')
    if not (tools/'kafka_2.13-4.2.2/bin/kafka-server-start.sh').is_file():
        with tarfile.open(p) as archive:archive.extractall(tools,filter='data')
    write_json(ROOT/'reports/week7/kafka_installation.json',{'version':'4.2.2','source':url,'sha512':actual,'verified':True})
    env=ROOT/'.venv-airflow'
    if not (env/'bin/python').exists():subprocess.run([sys.executable,'-m','venv',str(env)],check=True)
    constraints=ROOT/'orchestration/constraints-airflow-3.3.2-python3.12.txt'
    if not constraints.is_file():
        r=requests.get('https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt',timeout=30);r.raise_for_status();constraints.write_text(r.text)
    subprocess.run([str(env/'bin/python'),'-m','pip','install','apache-airflow[postgres]==3.3.2','--constraint',str(constraints)],check=True)
    subprocess.run([sys.executable,'-m','pip','install','confluent-kafka==2.16.0'],check=True)
    install()
    print('Week 7 runtime installation complete')

if __name__=='__main__':setup()
