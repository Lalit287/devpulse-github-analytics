"""Run only after the full real Gold dataset is loaded in project PostgreSQL."""
import uuid
import pytest
from psycopg import sql,errors

from database.cluster import connect
from database.load_analytics import InjectedLoadFailure,load_analytics
from database.contracts import KEYS
from dashboard import service


def test_native_binary_copy_preserves_boundary_types():
    from datetime import date,datetime,timezone
    import pyarrow as pa
    import pyarrow.dataset as ds
    from database.load_analytics import create_table,copy_table
    fields = pa.schema([('repo_id',pa.int64()),('total_events',pa.int64()),
                        ('label',pa.string()),('event_date',pa.date32()),
                        ('fetched_at',pa.timestamp('us')),('topics',pa.list_(pa.string()))])
    values = [{'repo_id':2**63-1,'total_events':2**31+7,'label':'漢字 ☕\n\\N',
               'event_date':date(2025,1,1),'fetched_at':datetime(2025,1,1,12),
               'topics':['rust','two words','漢字']},
              {'repo_id':2,'total_events':0,'label':None,'event_date':None,
               'fetched_at':None,'topics':None},
              {'repo_id':3,'total_events':1,'label':'','event_date':date(2025,1,1),
               'fetched_at':datetime(2025,1,1),'topics':[]}]
    with connect('writer') as connection:
        # Force creation of this session's temp namespace; rollback removes all work.
        connection.execute('CREATE TEMP TABLE boundary_namespace (id integer)')
        create_table(connection,'pg_temp','repository_metadata',fields)
        count,totals,samples=copy_table(connection,'pg_temp','repository_metadata',ds.dataset(pa.Table.from_pylist(values,schema=fields)))
        assert count==3 and totals['total_events']==2**31+8
        rows=connection.execute('SELECT * FROM pg_temp.repository_metadata ORDER BY repo_id DESC').fetchall()
        expected=[]
        for row in sorted(values,key=lambda r:r['repo_id'],reverse=True):
            expected.append(tuple(v.replace(tzinfo=timezone.utc) if isinstance(v,datetime) else v for v in row.values()))
        assert rows==expected
        connection.rollback()


def test_loaded_counts_and_read_only_role():
    s=service.current_dataset()
    with connect('reader') as connection:
        assert connection.execute('SELECT current_user').fetchone()[0]=='devpulse_reader'
        for name in KEYS:
            assert connection.execute(sql.SQL('SELECT count(*) FROM {}').format(service.table(s,name))).fetchone()[0]==s['row_counts'][name]
    with connect('reader') as connection:
        connection.execute('SET TRANSACTION READ WRITE')
        with pytest.raises(errors.InsufficientPrivilege):
            connection.execute(sql.SQL('UPDATE {} SET total_events=total_events WHERE false').format(service.table(s,'repository_metrics')))


def test_idempotent_real_load_copies_no_rows():
    before=service.current_dataset()
    result=load_analytics()
    assert result['status']=='reused' and result['copied_rows']==0
    assert service.current_dataset()['load_id']==before['load_id']


def test_mid_load_rollback_preserves_current_and_removes_staging():
    before=service.current_dataset()
    with connect('writer') as connection:
        schemas={r[0] for r in connection.execute("SELECT nspname FROM pg_namespace WHERE nspname LIKE 'dp_%'")}
    with pytest.raises(InjectedLoadFailure):
        load_analytics(verification_nonce=uuid.uuid4().hex,fail_after='repository_metadata')
    assert service.current_dataset()['load_id']==before['load_id']
    with connect('writer') as connection:
        after={r[0] for r in connection.execute("SELECT nspname FROM pg_namespace WHERE nspname LIKE 'dp_%'")}
        assert after==schemas
        assert connection.execute("SELECT count(*) FROM devpulse_control.pipeline_runs WHERE verification_run AND status='failed'").fetchone()[0]>=1


def test_real_search_language_compare_and_injection_boundary():
    s=service.current_dataset()
    rows=service.repository_rankings(s,sort='Star actions',limit=10)
    ids=rows['repo_id'].head(2).astype(int).tolist()
    compared=service.comparison(s,ids)
    assert set(compared['repo_id'])==set(ids)
    assert service.repository_rankings(s,search="' OR 1=1 --").empty
    rust=service.repository_rankings(s,language='Rust')
    assert not rust.empty and set(rust['primary_language'])=={'Rust'}
    assert not service.repository_history(s,ids[0]).empty


def test_language_accounting_and_empty_prediction_table():
    s=service.current_dataset();first,last=service.date_bounds(s)
    primary=service.languages(s,first,last)
    weighted=service.languages(s,first,last,weighted=True)
    assert int(primary['total_events'].sum())==s['facts']['input_events']
    assert float(weighted['attributed_total_events'].sum())==pytest.approx(s['facts']['input_events'],abs=.01)
    predictions=service.predictions()
    assert predictions.empty or predictions['predicted_probability'].between(0,1).all()
    accounts=service.accounts(s)
    assert not accounts.empty and not service.account_history(s,int(accounts.iloc[0]['actor_id'])).empty
