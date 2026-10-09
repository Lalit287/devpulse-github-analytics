"""Bounded Airflow task entry points; all batch outputs use an isolated profile."""
import argparse,json,os,re,shutil,time
from datetime import date,datetime,timedelta,timezone
from pathlib import Path
from config.settings import ROOT
from exploration.io import write_json
from ingestion.locking import directory_lock

def selected_date(value,interval_end=None):
    if value in ['',None,'None','null']:
        if not interval_end:raise ValueError('Scheduled source date needs an aware interval end')
        end=datetime.fromisoformat(interval_end)
        if end.tzinfo is None:raise ValueError('Interval end must be timezone aware')
        value=(end.astimezone(timezone.utc).date()-timedelta(days=1)).isoformat()
    if not isinstance(value,str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}',value) or date.fromisoformat(value).isoformat()!=value:raise ValueError('Source date must be YYYY-MM-DD')
    if date.fromisoformat(value)>=datetime.now(timezone.utc).date():raise ValueError('Only completed UTC dates may be collected')
    return value

def profile(day):return ROOT/'data/orchestrated'/day

def collect(day):
    from ingestion.collect import run_collection
    from ingestion.hour_range import plan_hours
    from storage.backends import LocalBronze
    p=profile(day);raw=p/'raw';raw.mkdir(parents=True,exist_ok=True)
    # A private copy is essential: replacement/retries cannot change original files.
    existing=json.loads((ROOT/'data/raw/download_manifest.json').read_text())
    entries={}
    for h in plan_hours(day):
        name=h['filename'];src=ROOT/'data/raw'/name;dst=raw/name
        if src.is_file() and name in existing['files']:
            if not dst.exists():os.link(src,dst)
            entries[name]=existing['files'][name]
    if entries and not (raw/'download_manifest.json').exists():write_json(raw/'download_manifest.json',{'files':entries})
    result=run_collection(plan_hours(day),raw_dir=raw,metadata_dir=p/'metadata',backend=LocalBronze(p/'bronze',4*1024**3),max_disk_mb=4096,fail_fast=True)
    if result['status']!='complete':raise RuntimeError('Scheduled collection incomplete; inspect isolated collection ledger')
    return {'status':result['status'],'hours':result['completed_hours'],'validated_records':result['validated_records'],'reused_downloads':result['reused_downloads']}

def etl(day):
    from spark.sources import plan_sources
    from spark.clean_events import run_etl
    p=profile(day)
    return run_etl(plan_sources(p/'metadata/latest_collection.json',backend='local',raw_dir=p/'raw'),output_root=p/'silver',shuffle_partitions=32)

def gold(day,requests=0):
    from analytics.pipeline import run_analytics
    p=profile(day);cache=p/'metadata/github_repositories';cache.mkdir(parents=True,exist_ok=True)
    for src in (ROOT/'data/metadata/github_repositories').glob('*.json'):
        if not (cache/src.name).exists():shutil.copy2(src,cache/src.name)
    (p/'core/snapshots').mkdir(parents=True,exist_ok=True);(p/'gold/snapshots').mkdir(parents=True,exist_ok=True)
    return run_analytics(silver_root=p/'silver',core_root=p/'core',gold_root=p/'gold',cache_dir=cache,max_requests=requests,cache_ttl_days=365,enrichment_report=p/'metadata/enrichment_run.json')

def publish(day):
    from database.load_analytics import load_analytics
    p=profile(day);r=load_analytics(p/'gold',p/'silver');return {k:v for k,v in r.items() if k!='audit'}

def check(day):
    from spark.snapshots import current_manifest
    from dashboard.service import current_dataset
    from database.cluster import connect
    p=profile(day);gold=current_manifest(p/'gold');silver=current_manifest(p/'silver');s=current_dataset()
    if s['gold_snapshot_id']!=gold['snapshot_id'] or s['facts']['input_events']!=silver['clean_events']:raise ValueError('Published dataset/source mismatch')
    with connect('reader') as c:
        if c.execute('SELECT sum(total_events) FROM devpulse.repository_metrics').fetchone()[0]!=silver['clean_events']:raise ValueError('Scheduled SQL event parity failed')
    return {'status':'passed','source_date':day,'input_events':silver['clean_events'],'gold_snapshot_id':gold['snapshot_id'],'load_id':s['load_id']}

def run(stage,day,requests=0,fail_once=False):
    p=profile(day);p.mkdir(parents=True,exist_ok=True)
    receipt_dir=ROOT/'reports/week7/batch'/day
    with directory_lock(p/'.airflow-task.lock'):
        if fail_once:
            marker=receipt_dir/'retry_fault_consumed.json'
            if not marker.exists():
                write_json(marker,{'expected':'one controlled collection failure to exercise Airflow retry','stage':stage})
                raise RuntimeError('Controlled Airflow retry verification')
        started=time.monotonic();result=gold(day,requests) if stage=='gold' else globals()[stage](day)
        write_json(receipt_dir/f'{stage}.json',{'stage':stage,'source_date':day,'completed_at_utc':datetime.now(timezone.utc).isoformat(),'duration_seconds':round(time.monotonic()-started,3),'result':result})
        return result

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('stage',choices=['collect','etl','gold','publish','check']);p.add_argument('--date',default=os.environ.get('DEVPULSE_SOURCE_DATE','2025-01-01'));p.add_argument('--requests',type=int,default=int(os.environ.get('DEVPULSE_METADATA_REQUESTS','0')));a=p.parse_args()
    if not 0<=a.requests<=50:raise ValueError('API request cap is 0–50')
    day=selected_date(a.date,os.environ.get('DEVPULSE_INTERVAL_END'));r=run(a.stage,day,a.requests,os.environ.get('DEVPULSE_FAIL_ONCE')=='true' and a.stage=='collect');print(json.dumps(r,default=str,indent=2))
