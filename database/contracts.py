"""Allowlisted tables, keys, and exact Arrow-to-PostgreSQL type mapping."""
import pyarrow as pa
import pyarrow.dataset as ds

KEYS = {
    "repository_metrics": ["repo_id"], "repository_daily": ["event_date", "repo_id"],
    "repository_hourly": ["event_date", "event_hour", "repo_id"], "account_metrics": ["actor_id"],
    "account_daily": ["event_date", "actor_id"], "account_hourly": ["event_date", "event_hour", "actor_id"],
    "activity_calendar": ["event_date"], "hourly_activity": ["event_date", "event_hour"],
    "participation_edges_sample": ["repo_id", "actor_id"], "repository_metadata": ["repo_id"],
    "repository_language_shares": ["repo_id", "language"],
    "language_primary_daily": ["event_date", "primary_language", "enrichment_status"],
    "language_weighted_daily": ["event_date", "language"], "technology_daily": ["event_date", "technology_category"],
    "language_intraday_growth": ["primary_language", "enrichment_status"],
}


def dataset(path):
    has_dates = any(p.name.startswith("event_date=") for p in path.iterdir())
    partition = ds.partitioning(pa.schema([("event_date", pa.date32())]), flavor="hive") if has_dates else None
    return ds.dataset(str(path), format="parquet", partitioning=partition, exclude_invalid_files=True)


def pg_type(kind):
    if pa.types.is_int64(kind): return "int8"
    if pa.types.is_int32(kind): return "int4"
    if pa.types.is_string(kind) or pa.types.is_large_string(kind): return "text"
    if pa.types.is_float64(kind): return "float8"
    if pa.types.is_boolean(kind): return "bool"
    if pa.types.is_date32(kind): return "date"
    if pa.types.is_timestamp(kind): return "timestamptz"
    if (pa.types.is_list(kind) or pa.types.is_large_list(kind)) and pa.types.is_string(kind.value_type): return "text[]"
    raise ValueError(f"Unsupported Gold Arrow type: {kind}")
