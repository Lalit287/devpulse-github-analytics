"""Pinned benchmark inputs and comparable repository-count workload."""
import hashlib,json,resource,sys,time
from pathlib import Path
import pyarrow as pa,pyarrow.dataset as ds
from config.settings import ROOT
from spark.snapshots import current_manifest
from exploration.io import sha256_file
FIELDS=['repo_id','actor_id','event_type','payload_action']
OUTPUT=['repo_id','total_events','star_events','fork_events','active_accounts']

def source():
    m=current_manifest(ROOT/'data/silver');p=ROOT/'data/silver/snapshots'/m['snapshot_id']/'events'
    return m,p

def dataset(path):
    return ds.dataset(path,format='parquet',partitioning=ds.partitioning(pa.schema([('event_date',pa.date32()),('event_hour',pa.int32())]),flavor='hive'))

def digest(rows):
    h=hashlib.sha256();n=0;events=0
    for row in rows:
        values=[int(row[k]) for k in OUTPUT] if isinstance(row,dict) else [int(v) for v in row]
        h.update((','.join(map(str,values))+'\n').encode());n+=1;events+=values[1]
    return {'sha256':h.hexdigest(),'repository_rows':n,'input_events':events}

def rss():
    # macOS rusage reports bytes, Linux reports KiB. Child max is NOT aggregate RSS.
    factor=1 if sys.platform=='darwin' else 1024
    return {'python_peak_rss_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*factor,
            'largest_reaped_child_peak_rss_bytes':resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss*factor,
            'scope':'individual-process maxima; not concurrent aggregate cluster RSS'}
