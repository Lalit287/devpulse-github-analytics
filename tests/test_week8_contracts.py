"""Package exclusions and final evidence gates that prevent false completion."""
import math
import pytest
from deployment.package import allowed
from benchmarks.common import digest
from scripts.week8_evidence import valid_seconds,validate_compute,validate_storage,validate_rates

@pytest.mark.parametrize('path',['.runtime/credentials.json','.venv/bin/python','x/__pycache__/x.pyc','../secret','/absolute','reports/run.log','.env','data/../credentials.json','config/hadoop/core-site.xml','reports/week6/test_receipt 2.json'])
def test_private_or_unsafe_members_are_excluded(path):assert not allowed(path)

@pytest.mark.parametrize('path',['README.md','.env.example','streaming/sink.py','docs/architecture.md','reports/week8/verification.json'])
def test_reviewable_source_and_evidence_members_are_allowed(path):assert allowed(path)

@pytest.mark.parametrize('value',[0,-1,float('inf'),float('nan'),True,None])
def test_elapsed_time_cannot_be_missing_or_nonfinite(value):
    with pytest.raises(ValueError):valid_seconds(value)

def test_digest_preserves_large_integer_entity_ids():
    a=digest([(2**63-1,1,0,0,1)]);b=digest([{'repo_id':2**63-1,'total_events':1,'star_events':0,'fork_events':0,'active_accounts':1}])
    assert a==b and a['repository_rows']==a['input_events']==1
    assert a!=digest([(2**63-2,1,0,0,1)])

@pytest.mark.parametrize('validator',[validate_compute,validate_storage,validate_rates])
def test_incomplete_native_evidence_cannot_pass(validator):
    with pytest.raises(ValueError):validator({'status':'not_run'})
