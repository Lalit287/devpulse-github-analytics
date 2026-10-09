"""Restartable complete-hour historical collection, bounded Arrow projection, provenance."""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date,datetime,timedelta,timezone
import gzip
import hashlib
import json
from pathlib import Path
import re
import shutil
import time

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from config.settings import ROOT
from exploration.io import sha256_file,write_json
from ingestion.download_gharchive import download_archive
from ingestion.locking import directory_lock

BASE=ROOT/'data/prediction_history'
START=date(2015,1,1)
DAYS=42
SCHEMA=pa.schema([
    ('event_id',pa.string()),('repo_id',pa.int64()),('repo_name',pa.string()),
    ('actor_id',pa.int64()),('event_type',pa.string()),('payload_action',pa.string()),
    ('event_time',pa.timestamp('us',tz='UTC')),('source_archive_start',pa.timestamp('us',tz='UTC')),
    ('source_filename',pa.string()),('source_row',pa.int64()),('raw_sha256',pa.string())])


def project(event,raw,source,line_number):
    """Lossless IDs; timestamps and source availability remain distinct."""
    if not isinstance(event,dict) or event.get('public') is not True:
        raise ValueError('not_public_object')
    event_id=str(event.get('id',''))
    if not re.fullmatch(r'[1-9][0-9]*',event_id):raise ValueError('invalid_event_id')
    repo=event.get('repo');actor=event.get('actor')
    if not isinstance(repo,dict) or not isinstance(actor,dict):raise ValueError('invalid_identity')
    def identity(value):
        if isinstance(value,bool) or not re.fullmatch(r'[0-9]+',str(value)):
            raise ValueError('invalid_identity')
        value=int(value)
        if not 0<value<=2**63-1:raise ValueError('invalid_identity')
        return value
    name=repo.get('name','')
    if not isinstance(name,str) or not re.fullmatch(r'[^\s/]+/[^\s/]+',name):raise ValueError('invalid_repo_name')
    kind=event.get('type','')
    if not isinstance(kind,str) or not re.fullmatch(r'[A-Z][A-Za-z0-9]*Event',kind):raise ValueError('invalid_event_type')
    when=event.get('created_at','')
    if not isinstance(when,str):raise ValueError('invalid_timestamp')
    try:timestamp=datetime.fromisoformat(when.replace('Z','+00:00'))
    except ValueError:raise ValueError('invalid_timestamp') from None
    if timestamp.tzinfo is None:raise ValueError('invalid_timestamp')
    payload=event.get('payload')
    if not isinstance(payload,dict):raise ValueError('missing_payload')
    action=payload.get('action')
    if action is not None and not isinstance(action,str):raise ValueError('invalid_action')
    return {'event_id':event_id,'repo_id':identity(repo.get('id')),'repo_name':name,
            'actor_id':identity(actor.get('id')),'event_type':kind,'payload_action':action,
            'event_time':timestamp.astimezone(timezone.utc),'source_archive_start':source,
            'source_filename':f'{source:%Y-%m-%d}-{source.hour}.json.gz','source_row':line_number,
            'raw_sha256':hashlib.sha256(raw.encode()).hexdigest()}


def convert(path,source,download):
    target=BASE/'projected'/f'{source:%Y-%m-%d}'/f'{source.hour}.parquet'
    receipt=target.with_suffix('.json')
    code=sha256_file(Path(__file__))
    if receipt.exists() and target.exists():
        saved=json.loads(receipt.read_text())
        if (saved.get('source_sha256')==download['sha256'] and saved.get('code_sha256')==code
                and saved.get('parquet_sha256')==sha256_file(target)):
            return saved
    target.parent.mkdir(parents=True,exist_ok=True)
    temporary=target.with_suffix('.parquet.part')
    quarantine=target.with_suffix('.quarantine.jsonl')
    raw_rows=0;valid_rows=0;reasons=Counter();batch=[]
    minimum=None;maximum=None;future_timestamps=0
    try:
        with pq.ParquetWriter(temporary,SCHEMA,compression='zstd') as writer, gzip.open(path,'rt',encoding='utf-8') as handle, quarantine.open('w') as rejected:
            for raw_rows,line in enumerate(handle,1):
                try:
                    if len(line.encode())>8*1024**2:raise ValueError('record_exceeds_8_mib')
                    row=project(json.loads(line),line,source,raw_rows)
                except (ValueError,TypeError,AttributeError) as exc:
                    reason=str(exc) if isinstance(exc,ValueError) and str(exc) in {
                        'not_public_object','invalid_event_id','invalid_identity','invalid_repo_name',
                        'invalid_event_type','invalid_timestamp','missing_payload','invalid_action','record_exceeds_8_mib'} else 'invalid_json_or_shape'
                    reasons[reason]+=1
                    rejected.write(json.dumps({'source_row':raw_rows,'reason':reason,'raw':line})+'\n')
                    continue
                valid_rows+=1
                when=row['event_time'];minimum=when if minimum is None else min(minimum,when);maximum=when if maximum is None else max(maximum,when)
                future_timestamps+=int(when>=source+timedelta(hours=1))
                batch.append(row)
                if len(batch)==8192:
                    writer.write_table(pa.Table.from_pylist(batch,schema=SCHEMA));batch=[]
            if batch:writer.write_table(pa.Table.from_pylist(batch,schema=SCHEMA))
        if not raw_rows or sum(reasons.values())/raw_rows>.01:
            raise ValueError('Archive is empty or exceeds the one-percent rejection guard')
        temporary.replace(target)
        result={'filename':path.name,'url':download['url'],'source_sha256':download['sha256'],
                'compressed_bytes':download['compressed_bytes'],'code_sha256':code,
                'parquet_sha256':sha256_file(target),'parquet_bytes':target.stat().st_size,
                'raw_rows':raw_rows,'valid_rows':valid_rows,'rejected_rows':sum(reasons.values()),
                'rejection_reasons':dict(reasons),'future_timestamp_rows':future_timestamps,
                'event_time_min':minimum.isoformat() if minimum else None,'event_time_max':maximum.isoformat() if maximum else None,
                'quarantine_sha256':sha256_file(quarantine),'projected_path':str(target.relative_to(BASE))}
        write_json(receipt,result)
        return result
    finally:temporary.unlink(missing_ok=True)


def collect_day(day):
    results=[]
    with requests.Session() as session:
        for hour in range(24):
            source=datetime.combine(day,datetime.min.time(),timezone.utc)+timedelta(hours=hour)
            raw_dir=BASE/'raw'/day.isoformat()
            download=download_archive(day.isoformat(),hour,output_dir=raw_dir,max_disk_mb=1024,timeout=60,retries=3,session=session)
            results.append(convert(raw_dir/download['filename'],source,download))
    print(f'History {day}: all 24 hours, {sum(r["raw_rows"] for r in results):,} raw events',flush=True)
    return results


def collect():
    BASE.mkdir(parents=True,exist_ok=True)
    if shutil.disk_usage(BASE).free<20*1024**3:raise ValueError('Need at least 20 GiB free for the bounded historical corpus and models')
    started=time.monotonic()
    with directory_lock(BASE/'.collection.lock'):
        days=[START+timedelta(days=i) for i in range(DAYS)]
        results=[]
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures=[pool.submit(collect_day,day) for day in days]
            for future in as_completed(futures):results.extend(future.result())
        results.sort(key=lambda r:(r['filename'].rsplit('-',1)[0],int(r['filename'].rsplit('-',1)[1].split('.')[0])))
        if len(results)!=DAYS*24:raise ValueError('Incomplete historical archive coverage')
        identity=hashlib.sha256(json.dumps([{'file':r['filename'],'sha256':r['source_sha256'],'code':r['code_sha256']} for r in results],sort_keys=True).encode()).hexdigest()[:24]
        result={'status':'complete','corpus_id':identity,'start_utc':START.isoformat()+'T00:00:00+00:00',
                'end_utc':(START+timedelta(days=DAYS)).isoformat()+'T00:00:00+00:00','days':DAYS,'hours':DAYS*24,
                'raw_rows':sum(r['raw_rows'] for r in results),'projected_rows':sum(r['valid_rows'] for r in results),
                'rejected_rows':sum(r['rejected_rows'] for r in results),'compressed_bytes':sum(r['compressed_bytes'] for r in results),
                'projected_bytes':sum(r['parquet_bytes'] for r in results),'duration_seconds':round(time.monotonic()-started,3),
                'completed_at_utc':datetime.now(timezone.utc).isoformat(),'sources':results}
        write_json(BASE/'manifest.json',result)
        print(json.dumps({k:v for k,v in result.items() if k!='sources'},indent=2),flush=True)
        return result


if __name__=='__main__':collect()
