"""Transactional Kafka offset and event-ID ledger with durable late corrections.

Every foreachBatch commits its messages, unique events, minute aggregates and
receipt together. A crash before Spark's checkpoint commit safely replays SQL.
"""
import hashlib,json,re
from datetime import datetime,timezone,timedelta
from pathlib import Path
from psycopg.types.json import Jsonb
from psycopg.rows import dict_row
from config.settings import ROOT
from database.cluster import connect
from streaming.source import fingerprint
from spark.schema import KNOWN_EVENTS
from streaming.broker import ADDRESS
COLUMNS=['topic','partition_id','offset_id','value_sha256','event_id','fingerprint','event_time','minute_utc','event_type','payload_action','repo_id','repo_name','actor_id','actor_login','reason']

def initialize():
    with connect('writer') as c:c.execute((Path(__file__).with_name('schema.sql')).read_text())

def register(stream_id,topic,meta):
    if not re.fullmatch(r'[a-z0-9][a-z0-9.-]{0,99}',stream_id):raise ValueError('Invalid stream identity')
    if topic!='devpulse.'+stream_id:raise ValueError('Topic and stream identity differ')
    initialize()
    with connect('writer') as c:
        c.execute('INSERT INTO devpulse_stream.runs(stream_id,topic,source_snapshot,start_utc,end_utc,expected_events,bootstrap_servers) VALUES(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING',
                  (stream_id,topic,meta['source_snapshot'],meta['start_utc'],meta['end_utc'],meta['expected_events'],ADDRESS))
        row=c.execute('SELECT topic,source_snapshot,start_utc,end_utc,expected_events,bootstrap_servers FROM devpulse_stream.runs WHERE stream_id=%s',(stream_id,)).fetchone()
        expected=(topic,meta['source_snapshot'],datetime.fromisoformat(meta['start_utc']),datetime.fromisoformat(meta['end_utc']),meta['expected_events'],ADDRESS)
        if row!=expected:raise ValueError('Existing stream has another immutable source contract')

def metadata(stream_id):
    with connect('reader') as c:
        c.row_factory=dict_row;r=c.execute('SELECT * FROM devpulse_stream.runs WHERE stream_id=%s',(stream_id,)).fetchone()
    if not r:raise ValueError('Register the source contract before consuming')
    return r

def normalized(raw,meta):
    """Validate strict positive IDs, aware UTC time, source identity and public events."""
    r=json.loads(raw)
    if not isinstance(r,dict):raise ValueError('not an object')
    if not isinstance(r.get('event_id'),str) or not re.fullmatch('[1-9][0-9]{0,29}',r['event_id']):raise ValueError('event ID')
    for key in ['repo_id','actor_id']:
        if type(r.get(key)) is not int or not 0<r[key]<2**63:raise ValueError('entity ID')
    for key,maxlen in [('repo_name',512),('actor_login',256)]:
        if not isinstance(r.get(key),str) or not 1<=len(r[key])<=maxlen or '\x00' in r[key]:raise ValueError('entity name')
    if r.get('public') is not True or r.get('event_type') not in KNOWN_EVENTS:raise ValueError('event contract')
    action=r.get('payload_action')
    if action is not None and (not isinstance(action,str) or len(action)>128 or '\x00' in action):raise ValueError('payload action')
    if r.get('source_snapshot')!=meta['source_snapshot']:raise ValueError('source contract')
    t=datetime.fromisoformat(r['event_time'])
    if t.tzinfo is None:raise ValueError('event time must include timezone')
    t=t.astimezone(timezone.utc)
    if not meta['start_utc']<=t<meta['end_utc']:raise ValueError('outside source hour')
    r['event_time']=t.isoformat()
    return r,t,t.replace(second=0,microsecond=0)

class InjectedRollback(RuntimeError):pass

def commit_batch(stream_id,batch_id,rows,*,fail_before_commit=False):
    """Rows carry Spark-computed minute microseconds and exact Kafka offsets."""
    if fail_before_commit and not stream_id.startswith('verification-'):raise ValueError('Rollback injection requires a verification stream')
    meta=metadata(stream_id);staged=[];ranges={}
    for row in rows:
        row=row.asDict() if hasattr(row,'asDict') else row
        raw=row['value'];raw=raw.encode() if isinstance(raw,str) else bytes(raw)
        key=(row['topic'],row['partition']);off=row['offset']
        if row['topic']!=meta['topic']:raise ValueError('Unexpected subscribed topic')
        ranges.setdefault(key,[]).append(off)
        base=[row['topic'],row['partition'],off,hashlib.sha256(raw).hexdigest()]
        try:
            r,t,minute=normalized(raw,meta)
            # Timestamp roundtrips never interpret Spark's naive local Python datetime.
            minute_us=int((minute-datetime(1970,1,1,tzinfo=timezone.utc)).total_seconds())*1000000
            if row.get('minute_us')!=minute_us:raise ValueError('Spark UTC minute mismatch')
            staged.append(base+[r['event_id'],fingerprint(r),t,minute,r['event_type'],r.get('payload_action'),r['repo_id'],r['repo_name'],r['actor_id'],r['actor_login'],None])
        except (ValueError,TypeError,KeyError,UnicodeError,OverflowError):
            staged.append(base+[None]*10+['invalid_contract'])
    if len(staged)>50000:raise ValueError('Microbatch exceeds bounded sink admission limit')
    offsets={f'{t}:{p}':{'start':min(v),'end_exclusive':max(v)+1,'rows':len(v)} for (t,p),v in sorted(ranges.items())}
    digest=hashlib.sha256(json.dumps(sorted((r[0],r[1],r[2],r[3]) for r in staged),separators=(',',':')).encode()).hexdigest()
    with connect('writer') as c:
        c.execute('SELECT pg_advisory_xact_lock(hashtext(%s))',('devpulse-stream:'+stream_id,))
        c.row_factory=dict_row
        old=c.execute('SELECT * FROM devpulse_stream.batches WHERE stream_id=%s AND digest=%s',(stream_id,digest)).fetchone()
        if old:return dict(old,reused=True)
        state=c.execute('SELECT * FROM devpulse_stream.runs WHERE stream_id=%s FOR UPDATE',(stream_id,)).fetchone()
        c.execute('CREATE TEMP TABLE stage(topic text,partition_id integer,offset_id bigint,value_sha256 text,event_id text,fingerprint text,event_time timestamptz,minute_utc timestamptz,event_type text,payload_action text,repo_id bigint,repo_name text,actor_id bigint,actor_login text,reason text) ON COMMIT DROP')
        with c.cursor().copy('COPY stage('+','.join(COLUMNS)+') FROM STDIN') as copy:
            for r in staged:copy.write_row(r)
        changed=c.execute('SELECT 1 FROM stage s JOIN devpulse_stream.messages m USING(topic,partition_id,offset_id) WHERE m.stream_id=%s AND m.value_sha256<>s.value_sha256 LIMIT 1',(stream_id,)).fetchone()
        if changed:raise ValueError('Kafka offset content changed; possible topic recreation/data loss')
        c.execute('CREATE TEMP TABLE fresh ON COMMIT DROP AS WITH inserted AS (INSERT INTO devpulse_stream.messages(stream_id,topic,partition_id,offset_id,event_id,disposition,value_sha256) SELECT %s,topic,partition_id,offset_id,event_id,coalesce(reason,\'pending\'),value_sha256 FROM stage ON CONFLICT DO NOTHING RETURNING topic,partition_id,offset_id) SELECT s.* FROM stage s JOIN inserted i USING(topic,partition_id,offset_id)',(stream_id,))
        # The first message in deterministic Kafka order owns an unseen event ID.
        c.execute('CREATE TEMP TABLE added ON COMMIT DROP AS WITH candidate AS (SELECT DISTINCT ON(event_id) * FROM fresh WHERE reason IS NULL ORDER BY event_id,topic,partition_id,offset_id), inserted AS (INSERT INTO devpulse_stream.events SELECT %s,event_id,fingerprint,event_time,minute_utc,event_type,payload_action,repo_id,repo_name,actor_id,actor_login,topic,partition_id,offset_id,coalesce(event_time < %s,False),coalesce(event_time < %s,False) FROM candidate ON CONFLICT DO NOTHING RETURNING *) SELECT * FROM inserted',
                  (stream_id,state['watermark'],state['max_event_time']))
        c.execute("UPDATE devpulse_stream.messages m SET disposition=CASE WHEN f.reason IS NOT NULL THEN 'invalid' WHEN e.fingerprint<>f.fingerprint THEN 'conflict' WHEN a.event_id IS NOT NULL AND a.topic=f.topic AND a.partition_id=f.partition_id AND a.offset_id=f.offset_id THEN 'accepted' ELSE 'duplicate' END FROM fresh f LEFT JOIN devpulse_stream.events e ON e.stream_id=%s AND e.event_id=f.event_id LEFT JOIN added a ON a.event_id=f.event_id WHERE m.stream_id=%s AND m.topic=f.topic AND m.partition_id=f.partition_id AND m.offset_id=f.offset_id",(stream_id,stream_id))
        # Recompute only affected minutes from the durable unique-event ledger.
        c.execute("INSERT INTO devpulse_stream.minute_activity SELECT %s,e.minute_utc,count(*),count(*) FILTER(WHERE event_type='WatchEvent' AND payload_action='started'),count(*) FILTER(WHERE event_type='ForkEvent'),count(DISTINCT actor_id),count(*) FILTER(WHERE is_late) FROM devpulse_stream.events e JOIN (SELECT DISTINCT minute_utc FROM added) a USING(minute_utc) WHERE e.stream_id=%s GROUP BY e.minute_utc ON CONFLICT(stream_id,minute_utc) DO UPDATE SET total_events=excluded.total_events,star_events=excluded.star_events,fork_events=excluded.fork_events,active_accounts=excluded.active_accounts,late_corrections=excluded.late_corrections",(stream_id,stream_id))
        c.execute("INSERT INTO devpulse_stream.repository_minutes SELECT %s,e.minute_utc,e.repo_id,count(*),count(*) FILTER(WHERE event_type='WatchEvent' AND payload_action='started'),count(*) FILTER(WHERE event_type='ForkEvent') FROM devpulse_stream.events e JOIN (SELECT DISTINCT minute_utc,repo_id FROM added) a USING(minute_utc,repo_id) WHERE e.stream_id=%s GROUP BY e.minute_utc,e.repo_id ON CONFLICT(stream_id,minute_utc,repo_id) DO UPDATE SET total_events=excluded.total_events,star_events=excluded.star_events,fork_events=excluded.fork_events",(stream_id,stream_id))
        counts=c.execute("SELECT count(*) AS n,count(*) FILTER(WHERE disposition='duplicate') AS duplicates,count(*) FILTER(WHERE disposition='invalid') AS invalid,count(*) FILTER(WHERE disposition='conflict') AS conflicts FROM devpulse_stream.messages m JOIN fresh f USING(topic,partition_id,offset_id) WHERE m.stream_id=%s",(stream_id,)).fetchone()
        added=c.execute('SELECT count(*) AS n,count(*) FILTER(WHERE is_late) AS late,count(*) FILTER(WHERE is_out_of_order) AS out_of_order,max(event_time) AS maximum FROM added').fetchone()
        c.execute("UPDATE devpulse_stream.runs SET messages=messages+%s,unique_events=unique_events+%s,duplicates=duplicates+%s,invalid=invalid+%s,conflicts=conflicts+%s,late_events=late_events+%s,out_of_order_events=out_of_order_events+%s,max_event_time=greatest(max_event_time,%s),watermark=greatest(max_event_time,%s)-interval '10 minutes',updated_at=now(),status='running' WHERE stream_id=%s",(counts['n'],added['n'],counts['duplicates'],counts['invalid'],counts['conflicts'],added['late'],added['out_of_order'],added['maximum'],added['maximum'],stream_id))
        c.execute('INSERT INTO devpulse_stream.batches(stream_id,digest,spark_batch_id,input_rows,new_messages,new_events,duplicates,invalid,conflicts,late_events,offset_ranges) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',(stream_id,digest,batch_id,len(staged),counts['n'],added['n'],counts['duplicates'],counts['invalid'],counts['conflicts'],added['late'],Jsonb(offsets)))
        if fail_before_commit:raise InjectedRollback('Controlled SQL transaction rollback')
        return {'stream_id':stream_id,'digest':digest,'batch_id':batch_id,'input_rows':len(staged),'new_messages':counts['n'],'new_events':added['n'],'duplicates':counts['duplicates'],'invalid':counts['invalid'],'conflicts':counts['conflicts'],'late_events':added['late'],'reused':False,'offset_ranges':offsets}
