"""Offline GitHub protocol/cache tests with explicitly fabricated HTTP responses."""
import json
import time
import pytest

from enrichment.github import (BudgetStopped, GitHubClient, checksum, enrich, fetch_repository,
                               fresh, load_cache, repo_path)
from ingestion.state import utc_now


class Response:
    def __init__(self, body=None, status=200, headers=None):
        self.status_code=status
        self.headers=headers or {'X-RateLimit-Remaining':'59'}
        self.content=json.dumps(body).encode()
    def __enter__(self):return self
    def __exit__(self,*args):pass
    def iter_content(self,size):yield self.content


class Session:
    def __init__(self,responses):self.responses=list(responses);self.headers={};self.urls=[]
    def get(self,url,**kwargs):
        self.urls.append((url,kwargs))
        return self.responses.pop(0)


def metadata(identifier=1,private=False):
    return {'id':identifier,'private':private,'full_name':'new/name','language':'Python',
            'topics':['data-engineering'],'description':'A fixture','created_at':'2020-01-01T00:00:00Z',
            'stargazers_count':1,'forks_count':2}


def candidate():return {'repo_id':1,'repo_name':'old/name'}


def test_cache_roundtrip_and_zero_request_rerun(tmp_path):
    session=Session([Response(metadata()),Response({'Python':90,'Rust':10})])
    entries,run=enrich([candidate()],cache_dir=tmp_path,client=GitHubClient(session))
    assert run['http_requests']==2 and entries[0]['status']=='ok'
    assert entries[0]['current_full_name']=='new/name'
    assert session.urls[1][0]=='https://api.github.com/repos/new/name/languages'
    cached,rerun=enrich([candidate()],cache_dir=tmp_path,client=GitHubClient(Session([])))
    assert cached==entries and rerun['http_requests']==0 and rerun['cache_hits']==1
    cache=tmp_path/'1.json'
    edited=json.loads(cache.read_text());edited['primary_language']='Wrong';cache.write_text(json.dumps(edited))
    with pytest.raises(ValueError,match='Corrupted'):load_cache(cache,1)


@pytest.mark.parametrize('body,status',[(metadata(2),'identity_mismatch'),(metadata(private=True),'not_public')])
def test_identity_and_public_scope_checked_before_languages(body,status):
    client=GitHubClient(Session([Response(body)]))
    entry=fetch_repository(client,candidate())
    assert entry['status']==status and client.requests==1 and 'language_bytes' not in entry
    if status=='not_public':assert 'metadata' not in entry


@pytest.mark.parametrize('languages',[{'Python':True},{'Python':-1},['Python']])
def test_invalid_language_bytes_rejected(languages):
    entry=fetch_repository(GitHubClient(Session([Response(metadata()),Response(languages)])),candidate())
    assert entry['status']=='invalid_languages' and 'language_bytes' not in entry


def test_not_found_rate_limit_and_budget():
    entry=fetch_repository(GitHubClient(Session([Response(status=404)])),candidate())
    assert entry['status']=='not_found'
    client=GitHubClient(Session([Response(status=429,headers={'X-RateLimit-Remaining':'0','Retry-After':'60'})]))
    assert client.get_json('/repos/a/b')['status']=='rate_limited'
    with pytest.raises(BudgetStopped):client.get_json('/repos/a/b')
    client=GitHubClient(Session([Response(metadata())]),max_requests=1)
    assert fetch_repository(client,candidate())['status']=='request_budget_exhausted'
    assert client.requests==1


def test_external_redirect_rejected_without_contact():
    session=Session([Response(status=301,headers={'Location':'https://example.com/steal'})])
    with pytest.raises(ValueError,match='outside'):GitHubClient(session).get_json('/repos/a/b')
    assert len(session.urls)==1


def test_reserve_preserved_and_conditional_cache_revalidation():
    client=GitHubClient(Session([Response(metadata(),headers={'X-RateLimit-Remaining':'5'})]))
    assert fetch_repository(client,candidate())['status']=='rate_limit_reserve'
    assert client.requests==1
    previous={'status':'ok','body':metadata(),'body_sha256':checksum(metadata()),'etag':'test-etag',
              'fetched_at_utc':'2020-01-01T00:00:00+00:00'}
    session=Session([Response(status=304)])
    got=GitHubClient(session).get_json('/repos/a/b',previous)
    assert got['body']==previous['body'] and got['fetched_at_utc']==previous['fetched_at_utc']
    assert session.urls[0][1]['headers']['If-None-Match']=='test-etag'


def test_transient_retry_is_counted_and_path_is_safe():
    client=GitHubClient(Session([Response(status=500),Response(metadata())]),max_requests=2)
    assert client.get_json('/repos/a/b')['status']=='ok' and client.requests==2
    for name in ['a/../b','a/..','https://x','a/b?token=x']:
        with pytest.raises(ValueError):repo_path(name)


def test_rate_limited_cache_expires_at_reset_instead_of_waiting_a_day():
    entry={'status':'rate_limited','checked_at_utc':utc_now(),'retry_after_epoch':time.time()+10}
    assert fresh(entry,30)
    entry['retry_after_epoch']=time.time()-1
    assert not fresh(entry,30)


def test_request_budget_cache_does_not_block_the_next_invocation():
    assert not fresh({'status':'request_budget_exhausted','checked_at_utc':utc_now()},30)
