-- Control data and stable logical views are owned by devpulse_writer.
-- Typed immutable snapshot tables are generated from the Gold Parquet contract.
CREATE SCHEMA IF NOT EXISTS devpulse_control;
CREATE SCHEMA IF NOT EXISTS devpulse;
CREATE TABLE IF NOT EXISTS devpulse_control.schema_migrations (
    version INTEGER PRIMARY KEY, source_sha256 TEXT NOT NULL, applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS devpulse_control.dataset_versions (
    load_id TEXT PRIMARY KEY, gold_snapshot_id TEXT NOT NULL, source_sha256 TEXT NOT NULL,
    schema_name TEXT UNIQUE NOT NULL, loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    row_counts JSONB NOT NULL, facts JSONB NOT NULL, audit JSONB NOT NULL
);
CREATE TABLE IF NOT EXISTS devpulse_control.current_dataset (
    singleton BOOLEAN PRIMARY KEY CHECK (singleton), load_id TEXT NOT NULL
        REFERENCES devpulse_control.dataset_versions(load_id)
);
CREATE TABLE IF NOT EXISTS devpulse_control.pipeline_runs (
    run_id UUID PRIMARY KEY, gold_snapshot_id TEXT NOT NULL, load_id TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(), finished_at TIMESTAMPTZ,
    status TEXT NOT NULL CHECK (status IN ('running','complete','reused','failed')),
    copied_rows BIGINT NOT NULL DEFAULT 0, duration_seconds DOUBLE PRECISION,
    verification_run BOOLEAN NOT NULL DEFAULT false, error_class TEXT
);
CREATE TABLE IF NOT EXISTS devpulse_control.repository_predictions (
    model_id TEXT NOT NULL, repo_id BIGINT NOT NULL CHECK (repo_id > 0),
    as_of_utc TIMESTAMPTZ NOT NULL, horizon_days INTEGER NOT NULL CHECK (horizon_days > 0),
    predicted_probability DOUBLE PRECISION NOT NULL CHECK (predicted_probability BETWEEN 0 AND 1),
    model_metrics JSONB NOT NULL DEFAULT '{}', created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(model_id, repo_id, as_of_utc)
);
