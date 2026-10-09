"""Atomic publication rollback, using synthetic ordinary files, not production tables."""
import pytest
from analytics.artifacts import finish_snapshot, identity
from spark.snapshots import current_manifest, verify_snapshot


def test_gold_quota_preserves_current_and_prior_snapshot(tmp_path):
    root=tmp_path/'gold'
    stage=root/'snapshots/.staging-first'
    stage.mkdir(parents=True)
    (stage/'fixture.bin').write_bytes(b'explicitly synthetic fixture')
    recipe={'fixture_version':1}
    first=finish_snapshot(stage,root,identity(recipe),{'recipe':recipe})
    pointer=(root/'_CURRENT.json').read_bytes()
    next_stage=root/'snapshots/.staging-second'
    next_stage.mkdir()
    (next_stage/'fixture.bin').write_bytes(b'second synthetic fixture')
    with pytest.raises(ValueError,match='quota'):
        finish_snapshot(next_stage,root,identity({'fixture_version':2}),{'recipe':{'fixture_version':2}},max_bytes=1)
    assert (root/'_CURRENT.json').read_bytes()==pointer
    assert current_manifest(root)['snapshot_id']==verify_snapshot(first)['snapshot_id']
    assert (first/'fixture.bin').read_bytes()==b'explicitly synthetic fixture'
