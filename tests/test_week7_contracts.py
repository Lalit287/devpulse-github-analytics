"""Contract edges that can otherwise corrupt offsets, IDs or UTC windows."""
import json
from datetime import datetime,timezone
import pytest
from streaming.sink import normalized
from orchestration.jobs import selected_date
from enrichment.github import GitHubClient,BudgetStopped
from streaming.producer import publish
META={'source_snapshot':'verified','start_utc':datetime(2025,1,1,12,tzinfo=timezone.utc),'end_utc':datetime(2025,1,1,13,tzinfo=timezone.utc)}
ROW={'event_id':'123','event_type':'WatchEvent','actor_id':2,'actor_login':'someone','repo_id':3,'repo_name':'owner/repo','event_time':'2025-01-01T17:30:13+05:30','public':True,'source_snapshot':'verified','payload_action':'started'}

def test_timezone_is_converted_before_source_and_minute_checks():
    r,t,m=normalized(json.dumps(ROW),META)
    assert r['event_time']=='2025-01-01T12:00:13+00:00' and m.isoformat()=='2025-01-01T12:00:00+00:00'

@pytest.mark.parametrize('change',[{'actor_id':True},{'repo_id':2**63},{'repo_id':1.5},{'event_id':'0'},{'public':False},{'source_snapshot':'foreign'},{'event_time':'2025-01-01T12:00:00'},{'event_time':'2025-01-01T13:00:00+00:00'},{'repo_name':'bad\x00name'},{'event_type':'UnexpectedEvent'}])
def test_invalid_event_contract_is_rejected(change):
    with pytest.raises((ValueError,TypeError)):normalized(json.dumps(dict(ROW,**change)),META)

def test_scheduled_date_uses_previous_utc_day():
    assert selected_date(None,'2025-01-03T00:30:00+05:30')=='2025-01-01'
    assert selected_date('2025-01-01')=='2025-01-01'

@pytest.mark.parametrize('value',['../escape','2025-02-30','2025-1-01','2025-01-01; touch something'])
def test_bad_source_dates_fail_before_filesystem_work(value):
    with pytest.raises(ValueError):selected_date(value)

def test_cache_only_client_never_attempts_network(monkeypatch):
    c=GitHubClient(max_requests=0)
    monkeypatch.setattr(c.session,'get',lambda *a,**k:pytest.fail('Cache-only client used network'))
    with pytest.raises(BudgetStopped):c.get_json('/repos/owner/repo')
    assert c.requests==0

@pytest.mark.parametrize('rate',[0,-1,float('inf'),float('nan'),50001])
def test_rate_limit_rejects_unbounded_replay(rate):
    with pytest.raises(ValueError):publish('devpulse.verification',[],rate)

@pytest.mark.parametrize('status',['running','failed'])
def test_new_verification_clears_a_stale_completion_report(tmp_path,monkeypatch,status):
    from scripts import generate_week7_report as report
    monkeypatch.setattr(report,'ROOT',tmp_path)
    path=tmp_path/'reports/week7_report.md';path.parent.mkdir();path.write_text('Week 7 completed successfully')
    assert report.generate({'status':status}) is False
    assert f'Week 7 — {status}' in path.read_text() and 'completion is not confirmed' in path.read_text()
