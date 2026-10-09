"""Fetch exactly the Spark 4.0.1 Kafka connector dependencies from Maven Central."""
import hashlib
from pathlib import Path
import requests
from config.settings import ROOT
from exploration.io import write_json
ARTIFACTS=[('org.apache.spark','spark-sql-kafka-0-10_2.13','4.0.1'),('org.apache.spark','spark-token-provider-kafka-0-10_2.13','4.0.1'),('org.apache.kafka','kafka-clients','3.9.1'),('org.apache.commons','commons-pool2','2.12.0')]

def install():
    root=ROOT/'.tools/streaming-jars';root.mkdir(parents=True,exist_ok=True);receipts=[]
    for group,name,version in ARTIFACTS:
        filename=f'{name}-{version}.jar';url=f'https://repo.maven.apache.org/maven2/{group.replace(".","/")}/{name}/{version}/{filename}'
        p=root/filename
        if not p.exists():
            r=requests.get(url,timeout=60);r.raise_for_status();tmp=p.with_suffix('.part');tmp.write_bytes(r.content);tmp.replace(p)
        r=requests.get(url+'.sha1',timeout=30);r.raise_for_status()
        actual=hashlib.sha1(p.read_bytes()).hexdigest()
        if r.text.strip().split()[0].lower()!=actual:raise ValueError('Maven connector checksum mismatch')
        receipts.append({'file':filename,'url':url,'sha1':actual,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()})
    write_json(ROOT/'reports/week7/connector_installation.json',receipts)
    return [str(root/r['file']) for r in receipts]

def jars():
    root=ROOT/'.tools/streaming-jars';paths=[root/f'{name}-{v}.jar' for _,name,v in ARTIFACTS]
    if not all(p.is_file() for p in paths):raise RuntimeError('Run python -m streaming.dependencies first')
    receipt=__import__('json').loads((ROOT/'reports/week7/connector_installation.json').read_text())
    for p,r in zip(paths,receipt):
        if hashlib.sha256(p.read_bytes()).hexdigest()!=r['sha256']:raise ValueError('Installed connector checksum changed')
    return ','.join(map(str,paths))

if __name__=='__main__':print(install())
