"""Real PostgreSQL offset/event idempotency, late corrections and rollback."""
import json,uuid
from datetime import datetime,timezone
import pytest
from psycopg import errors
from database.cluster import connect
from streaming.sink import register,metadata,commit_batch,InjectedRollback
META={'source_snapshot':'verification','start_utc':'2025-01-01T12:00:00+00:00','end_utc':'2025-01-01T13:00:00+00:00','expected_events':2}

def event(id='1',minute=0,**changes):return dict({'event_id':id,'event_type':'WatchEvent','actor_id':11,'actor_login':'verification','repo_id':22,'repo_name':'verification/repo','event_time':f'2025-01-01T12:{minute:02}:01+00:00','public':True,'source_snapshot':'verification','payload_action':'started'},**changes)

def message(topic,offset,row):
    value=row if isinstance(row,bytes) else json.dumps(row).encode()
    try:t=datetime.fromisoformat(json.loads(value)['event_time']).replace(second=0,microsecond=0);us=int(t.timestamp())*1000000
    except (ValueError,KeyError):us=None
    return {'topic':topic,'partition':0,'offset':offset,'value':value,'minute_us':us}

@pytest.fixture
def stream():
    sid='verification-'+uuid.uuid4().hex[:12];topic='devpulse.'+sid;register(sid,topic,META)
    yield sid,topic
    with connect('writer') as c:c.execute('DELETE FROM devpulse_stream.runs WHERE stream_id=%s',(sid,))

def test_replayed_batches_and_replacement_offsets_do_not_double_count(stream):
    sid,topic=stream;rows=[message(topic,0,event()),message(topic,1,event())]
    first=commit_batch(sid,0,rows);second=commit_batch(sid,0,rows)
    assert first['new_events']==1 and first['duplicates']==1 and second['reused']
    commit_batch(sid,7,[rows[0]]);commit_batch(sid,8,[rows[1]])
    r=metadata(sid);assert (r['messages'],r['unique_events'],r['duplicates'])==(2,1,1)

def test_late_unique_event_corrects_original_minute(stream):
    sid,topic=stream
    commit_batch(sid,0,[message(topic,0,event('2',59))]);commit_batch(sid,1,[message(topic,1,event('1',0))])
    r=metadata(sid);assert r['late_events']==1 and r['out_of_order_events']==1 and r['unique_events']==2
    with connect('reader') as c:assert c.execute('SELECT total_events,star_events,late_corrections FROM devpulse_stream.minute_activity WHERE stream_id=%s ORDER BY minute_utc',(sid,)).fetchall()==[(1,1,1),(1,1,0)]

def test_conflicting_id_and_malformed_message_are_quarantined(stream):
    sid,topic=stream
    commit_batch(sid,0,[message(topic,0,event()),message(topic,1,event(repo_name='verification/conflict')),message(topic,2,b'{bad')])
    r=metadata(sid);assert (r['unique_events'],r['conflicts'],r['invalid'])==(1,1,1)

def test_sql_commit_failure_rolls_back_all_ledgers_and_aggregates(stream):
    sid,topic=stream;rows=[message(topic,0,event())]
    with pytest.raises(InjectedRollback):commit_batch(sid,0,rows,fail_before_commit=True)
    r=metadata(sid);assert r['messages']==0 and r['unique_events']==0
    with connect('reader') as c:
        for table in ['events','messages','minute_activity','repository_minutes','batches']:
            assert c.execute(f'SELECT count(*) FROM devpulse_stream.{table} WHERE stream_id=%s',(sid,)).fetchone()[0]==0
    assert commit_batch(sid,0,rows)['new_events']==1

def test_recreated_offset_content_is_rejected(stream):
    sid,topic=stream;commit_batch(sid,0,[message(topic,0,event())])
    with pytest.raises(ValueError,match='offset content changed'):commit_batch(sid,1,[message(topic,0,event(repo_name='verification/changed'))])
    assert metadata(sid)['unique_events']==1

def test_reader_cannot_change_streaming_or_airflow_state(stream):
    with connect('reader') as c:
        c.execute('SET TRANSACTION READ WRITE')
        with pytest.raises(errors.InsufficientPrivilege):c.execute('UPDATE devpulse_stream.runs SET status=status WHERE false')
    with connect('reader',dbname='devpulse_airflow') as c:
        c.execute('SET TRANSACTION READ WRITE')
        assert c.execute('SELECT count(*) FROM dag_run').fetchone()[0]>0
        with pytest.raises(errors.InsufficientPrivilege):c.execute('UPDATE dag_run SET state=state WHERE false')
