-- Additive model serving contract; Week 5's control schema is preserved.
CREATE SCHEMA IF NOT EXISTS devpulse_ml;
CREATE TABLE IF NOT EXISTS devpulse_ml.models (
    model_id TEXT PRIMARY KEY, snapshot_sha256 TEXT NOT NULL,
    selected_algorithm TEXT NOT NULL, historical_only BOOLEAN NOT NULL CHECK(historical_only),
    evaluation JSONB NOT NULL, published_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS devpulse_ml.current_model (
    singleton BOOLEAN PRIMARY KEY CHECK(singleton), model_id TEXT NOT NULL REFERENCES devpulse_ml.models(model_id)
);
CREATE TABLE IF NOT EXISTS devpulse_ml.forecast_context (
    model_id TEXT NOT NULL REFERENCES devpulse_ml.models(model_id), repo_id BIGINT NOT NULL CHECK(repo_id>0),
    as_of_utc TIMESTAMPTZ NOT NULL, repo_name TEXT NOT NULL,
    recent_stars BIGINT NOT NULL CHECK(recent_stars>=0), recent_forks BIGINT NOT NULL CHECK(recent_forks>=0),
    recent_pushes BIGINT NOT NULL CHECK(recent_pushes>=0), recent_events BIGINT NOT NULL CHECK(recent_events>=5),
    active_accounts BIGINT NOT NULL CHECK(active_accounts>0),
    PRIMARY KEY(model_id,repo_id,as_of_utc)
);
CREATE TABLE IF NOT EXISTS devpulse_ml.heldout_predictions (
    model_id TEXT NOT NULL REFERENCES devpulse_ml.models(model_id), repo_id BIGINT NOT NULL CHECK(repo_id>0),
    as_of_utc TIMESTAMPTZ NOT NULL, repo_name TEXT NOT NULL,
    past_stars BIGINT NOT NULL CHECK(past_stars>=0), future_stars BIGINT NOT NULL CHECK(future_stars>=0),
    actual_label BOOLEAN NOT NULL, predicted_probability DOUBLE PRECISION NOT NULL CHECK(predicted_probability BETWEEN 0 AND 1),
    PRIMARY KEY(model_id,repo_id,as_of_utc)
);
CREATE INDEX IF NOT EXISTS heldout_ranking ON devpulse_ml.heldout_predictions(model_id,predicted_probability DESC,repo_id);
CREATE INDEX IF NOT EXISTS prediction_model_ranking ON devpulse_control.repository_predictions(model_id,predicted_probability DESC,repo_id);
