"""Deterministic real Silver projection; historical event time is preserved."""
import hashlib,json,re
from datetime import datetime,timezone,timedelta
import pyarrow as pa
import pyarrow.dataset as ds
from config.settings import ROOT
from spark.snapshots import current_manifest
FIELDS=['event_id','event_type','actor_id','actor_login','repo_id','repo_name','event_time','payload_action','public']

def source(date='2025-01-01',hour=12):
    if not re.fullmatch(r'\d{4}-\d{2}-\d{2}',date) or type(hour)!=int or not 0<=hour<=23:raise ValueError('Invalid UTC source hour')
    start=datetime.fromisoformat(date).replace(hour=hour,tzinfo=timezone.utc)
    m=current_manifest(ROOT/'data/silver');path=ROOT/'data/silver/snapshots'/m['snapshot_id']/'events'
    d=ds.dataset(path,format='parquet',partitioning=ds.partitioning(pa.schema([('event_date',pa.date32()),('event_hour',pa.int32())]),flavor='hive'))
    t=d.to_table(filter=(ds.field('event_date')==start.date())&(ds.field('event_hour')==hour),columns=FIELDS)
    if not 1<=t.num_rows<=250000:raise ValueError('Replay supports one populated hour up to 250,000 events')
    t=t.sort_by([('event_time','ascending'),('event_id','ascending')])
    rows=t.to_pylist()
    for row in rows:
        row['event_time']=row['event_time'].isoformat();row['source_snapshot']=m['snapshot_id']
    meta={'source_snapshot':m['snapshot_id'],'start_utc':start.isoformat(),'end_utc':(start+timedelta(hours=1)).isoformat(),'expected_events':len(rows)}
    return meta,rows

def encode(row):return json.dumps(row,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()

def fingerprint(row):return hashlib.sha256(encode({k:row[k] for k in FIELDS+['source_snapshot']})).hexdigest()
