"""Synthetic fixtures test typed Parquet boundaries and SQL parameter isolation."""
from datetime import date,datetime,timezone
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from database.contracts import dataset,pg_type
from database.load_analytics import copy_table
from dashboard.service import like,limit_rows,validate_dates,validate_schema


def test_hive_dates_and_large_identifiers_are_lossless(tmp_path):
    partition=tmp_path/'event_date=2025-01-01';partition.mkdir()
    table=pa.table({'repo_id':pa.array([2**63-1],type=pa.int64()),'value':['Unicode: λ,\n"quoted"']})
    pq.write_table(table,partition/'fixture.parquet')
    actual=dataset(tmp_path)
    assert actual.schema.field('event_date').type==pa.date32()
    assert actual.to_table().to_pylist()==[{'repo_id':2**63-1,'value':'Unicode: λ,\n"quoted"','event_date':date(2025,1,1)}]


@pytest.mark.parametrize('kind,expected',[(pa.int64(),'int8'),(pa.int32(),'int4'),(pa.date32(),'date'),
    (pa.timestamp('us',tz='UTC'),'timestamptz'),(pa.list_(pa.string()),'text[]'),(pa.float64(),'float8')])
def test_postgres_types_avoid_implicit_binary_casts(kind,expected):
    assert pg_type(kind)==expected
    with pytest.raises(ValueError):pg_type(pa.struct([('x',pa.int64())]))


def test_copy_preserves_null_arrays_unicode_dates_and_64bit_values(tmp_path):
    value=pa.table({'repo_id':pa.array([2**63-1,1],pa.int64()),'total_events':pa.array([3,4],pa.int64()),
        'name':['Unicode λ\nline',None],'topics':pa.array([['a','b'],[]],pa.list_(pa.string())),
        'at':pa.array([datetime(2025,1,1,tzinfo=timezone.utc),None],pa.timestamp('us',tz='UTC'))})
    pq.write_table(value,tmp_path/'fixture.parquet')
    class Copy:
        def __init__(self):self.rows=[]
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def set_types(self,types):self.types=types
        def write_row(self,row):self.rows.append(row)
    class Cursor:
        def copy(self,query):return output
    class Connection:
        def cursor(self):return Cursor()
    output=Copy()
    rows,totals,samples=copy_table(Connection(),'fixture','repository_metrics',dataset(tmp_path),batch_size=1)
    assert rows==2 and totals['total_events']==7
    assert output.types==['int8','int8','text','text[]','timestamptz']
    assert output.rows[0]==(2**63-1,3,'Unicode λ\nline',['a','b'],datetime(2025,1,1,tzinfo=timezone.utc))
    assert output.rows[1][2:]==(None,[],None)


def test_untrusted_filter_text_and_invalid_limits():
    assert like("x_%\\' OR 1=1") == "%x\\_\\%\\\\' OR 1=1%"
    for limit in [0,201,-1,True]:
        with pytest.raises(ValueError):limit_rows(limit)
    with pytest.raises(ValueError):validate_dates(date(2025,1,2),date(2025,1,1))
    for schema in ['public','dp_abc; DROP SCHEMA x','dp_'+'g'*24]:
        with pytest.raises(ValueError):validate_schema(schema)
