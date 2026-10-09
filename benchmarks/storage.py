"""Equal logical projection across JSON, flat Parquet and partitioned Parquet."""
import gzip,json,time,statistics
from collections import Counter
from pathlib import Path
import pyarrow as pa,pyarrow.compute as pc,pyarrow.dataset as ds,pyarrow.parquet as pq,pyarrow.json as paj
from config.settings import ROOT
from exploration.io import write_json
from benchmarks.common import source,dataset,FIELDS
WORK=ROOT/'work/week8/storage'
COLS=FIELDS+['event_hour']

def size(path):return sum(p.stat().st_size for p in path.rglob('*') if p.is_file()) if path.is_dir() else path.stat().st_size

def build():
    m,path=source();table=dataset(path).to_table(columns=COLS)
    if table.num_rows!=m['clean_events']:raise ValueError('Storage projection lost source events')
    WORK.mkdir(parents=True,exist_ok=True)
    raw=WORK/'projection.jsonl';compressed=WORK/'projection.jsonl.gz'
    if not raw.exists():
        with raw.open('w') as f:
            for b in table.to_batches(max_chunksize=10000):
                f.writelines(json.dumps(r,separators=(',',':'))+'\n' for r in b.to_pylist())
    if not compressed.exists():
        with raw.open('rb') as src,gzip.open(compressed,'wb',compresslevel=1) as dst:
            import shutil
            shutil.copyfileobj(src,dst)
    fmt=ds.ParquetFileFormat();options=fmt.make_write_options(compression='snappy')
    for name,partition in [('flat',False),('partitioned',True)]:
        target=WORK/name
        if not target.exists():
            kwargs={'partitioning':['event_hour'],'partitioning_flavor':'hive'} if partition else {}
            ds.write_dataset(table,target,format=fmt,file_options=options,max_rows_per_file=250000,max_rows_per_group=64000,**kwargs)
    return m,table.schema

def read_query(name,schema):
    t=time.perf_counter();candidates=None;bytes_candidates=None
    if name in ['json','gzip_json']:
        path=WORK/('projection.jsonl' if name=='json' else 'projection.jsonl.gz')
        with pa.input_stream(path,compression='gzip' if name=='gzip_json' else None) as f:
            frame=paj.read_json(f,parse_options=paj.ParseOptions(explicit_schema=schema))
        frame=frame.filter(pc.equal(frame['event_hour'],12));candidates=1;bytes_candidates=size(path)
    else:
        path=WORK/name
        d=ds.dataset(path,format='parquet',partitioning=ds.partitioning(pa.schema([('event_hour',pa.int32())]),flavor='hive') if name=='partitioned' else None)
        predicate=ds.field('event_hour')==12
        fragments=list(d.get_fragments(filter=predicate));candidates=len(fragments);bytes_candidates=sum(Path(f.path).stat().st_size for f in fragments)
        frame=d.to_table(columns=['event_type','repo_id'],filter=predicate)
    grouped=frame.group_by('event_type').aggregate([('repo_id','count')]);counts={r['event_type']:r['repo_id_count'] for r in grouped.to_pylist()}
    return {'seconds':time.perf_counter()-t,'rows':frame.num_rows,'counts':counts,'candidate_fragments':candidates,'candidate_file_bytes':bytes_candidates}

def run():
    m,schema=build();records={};results=[]
    for trial in range(3):
        names=['json','gzip_json','flat','partitioned'] if trial%2==0 else ['partitioned','flat','gzip_json','json']
        for name in names:
            r=read_query(name,schema);records.setdefault(name,[]).append(r);results.append(json.dumps({'rows':r['rows'],'counts':r['counts']},sort_keys=True));print(name,trial+1,round(r['seconds'],3),flush=True)
    if len(set(results))!=1:raise ValueError('Storage formats return different historical event counts')
    paths={'json':WORK/'projection.jsonl','gzip_json':WORK/'projection.jsonl.gz','flat':WORK/'flat','partitioned':WORK/'partitioned'}
    summary={n:{'bytes':size(paths[n]),'median_seconds':statistics.median(r['seconds'] for r in rs),'candidate_fragments':rs[0]['candidate_fragments'],'candidate_file_bytes':rs[0]['candidate_file_bytes'],'rows':rs[0]['rows'],'counts':rs[0]['counts'],'trials':rs} for n,rs in records.items()}
    result={'status':'passed','source_snapshot':m['snapshot_id'],'input_events':m['clean_events'],'query_hour_utc':12,
        'projection':COLS,'scope':'equal common-field projection; not full nested archive JSON; warm filesystem caches; PyArrow engine','formats':summary}
    write_json(ROOT/'reports/week8/storage_benchmark.json',result);return result

if __name__=='__main__':run()
