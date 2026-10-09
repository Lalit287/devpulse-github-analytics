"""Full real-hour streaming parity and additive Week 7 integrity checks."""
import argparse,json
from collections import defaultdict
from datetime import datetime,timezone
from config.settings import ROOT
from database.cluster import connect
from exploration.io import write_json,sha256_file
from streaming.source import source
from streaming.sink import metadata

def parity(stream_id='historical-20250101-12'):
    m,rows=source();state=metadata(stream_id)
    if state['source_snapshot']!=m['source_snapshot'] or state['unique_events']!=len(rows):raise ValueError('Streaming source/unique-event parity failed')
    minute={};repo=defaultdict(lambda:[0,0,0])
    for r in rows:
        t=datetime.fromisoformat(r['event_time']).replace(second=0,microsecond=0)
        v=minute.setdefault(t,{'counts':[0,0,0],'actors':set()});star=int(r['event_type']=='WatchEvent' and r.get('payload_action')=='started');fork=int(r['event_type']=='ForkEvent')
        v['counts'][0]+=1;v['counts'][1]+=star;v['counts'][2]+=fork;v['actors'].add(r['actor_id'])
        rr=repo[(t,r['repo_id'])];rr[0]+=1;rr[1]+=star;rr[2]+=fork
    with connect('reader') as c:
        actual=c.execute('SELECT minute_utc,total_events,star_events,fork_events,active_accounts FROM devpulse_stream.minute_activity WHERE stream_id=%s',(stream_id,)).fetchall()
        wanted=sorted((t,*v['counts'],len(v['actors'])) for t,v in minute.items())
        if sorted(actual)!=wanted:raise ValueError('Every minute must match offline total/star/fork/unique-account counts')
        actual=c.execute('SELECT minute_utc,repo_id,total_events,star_events,fork_events FROM devpulse_stream.repository_minutes WHERE stream_id=%s',(stream_id,)).fetchall()
        if sorted(actual)!=sorted((*k,*v) for k,v in repo.items()):raise ValueError('Every repository-minute must match the real source')
        batch_count=c.execute('SELECT count(*) FROM devpulse_stream.batches WHERE stream_id=%s',(stream_id,)).fetchone()[0]
        ids=c.execute('SELECT event_id FROM devpulse_stream.events WHERE stream_id=%s',(stream_id,)).fetchall()
        if {r[0] for r in ids}!={r['event_id'] for r in rows}:raise ValueError('Event IDs differ from the verified source')
    result={'status':'passed','stream_id':stream_id,'source_snapshot':m['source_snapshot'],'unique_events':len(rows),'minutes':len(minute),'repository_minutes':len(repo),'committed_batches':batch_count,'state':{k:str(v) if isinstance(v,datetime) else v for k,v in state.items()}}
    write_json(ROOT/'reports/week7/parity'/f'{stream_id}.json',result);return result

def preservation():
    expected=json.loads((ROOT/'reports/week7/preservation_before.json').read_text());changed=[]
    for name,digest in expected.items():
        p=ROOT/name
        if not p.is_file() or sha256_file(p)!=digest:changed.append(name)
    if changed:raise ValueError('Previous evidence/source changed: '+', '.join(changed))
    return {'status':'passed','files':len(expected),'changed_files':[]}

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--stream-only',action='store_true');p.add_argument('--stream-id',default='historical-20250101-12');a=p.parse_args();print(json.dumps(parity(a.stream_id),indent=2))
