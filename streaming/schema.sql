CREATE SCHEMA IF NOT EXISTS devpulse_stream;
REVOKE ALL ON SCHEMA devpulse_stream FROM PUBLIC;
CREATE TABLE IF NOT EXISTS devpulse_stream.runs (
 stream_id text PRIMARY KEY, topic text UNIQUE NOT NULL, source_snapshot text NOT NULL,
 start_utc timestamptz NOT NULL,end_utc timestamptz NOT NULL,expected_events bigint NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(),updated_at timestamptz NOT NULL DEFAULT now(),
 max_event_time timestamptz,watermark timestamptz,status text NOT NULL DEFAULT 'ready',
 messages bigint NOT NULL DEFAULT 0,unique_events bigint NOT NULL DEFAULT 0,
 duplicates bigint NOT NULL DEFAULT 0,invalid bigint NOT NULL DEFAULT 0,conflicts bigint NOT NULL DEFAULT 0,
 late_events bigint NOT NULL DEFAULT 0,out_of_order_events bigint NOT NULL DEFAULT 0,
 CHECK(end_utc>start_utc),CHECK(expected_events>0)
);
CREATE TABLE IF NOT EXISTS devpulse_stream.events (
 stream_id text REFERENCES devpulse_stream.runs ON DELETE CASCADE,
 event_id text, fingerprint text NOT NULL,event_time timestamptz NOT NULL,
 minute_utc timestamptz NOT NULL,event_type text NOT NULL,payload_action text,
 repo_id bigint NOT NULL,repo_name text NOT NULL,actor_id bigint NOT NULL,actor_login text NOT NULL,
 topic text NOT NULL,partition_id integer NOT NULL,offset_id bigint NOT NULL,
 is_late boolean NOT NULL,is_out_of_order boolean NOT NULL,
 PRIMARY KEY(stream_id,event_id)
);
CREATE INDEX IF NOT EXISTS stream_repo_time ON devpulse_stream.events(stream_id,event_time,repo_id);
CREATE INDEX IF NOT EXISTS stream_actor_time ON devpulse_stream.events(stream_id,event_time,actor_id);
CREATE TABLE IF NOT EXISTS devpulse_stream.messages (
 stream_id text REFERENCES devpulse_stream.runs ON DELETE CASCADE,topic text,partition_id integer,offset_id bigint,
 event_id text,disposition text NOT NULL,value_sha256 text NOT NULL,
 PRIMARY KEY(stream_id,topic,partition_id,offset_id)
);
CREATE TABLE IF NOT EXISTS devpulse_stream.minute_activity (
 stream_id text REFERENCES devpulse_stream.runs ON DELETE CASCADE,minute_utc timestamptz,
 total_events bigint NOT NULL,star_events bigint NOT NULL,fork_events bigint NOT NULL,active_accounts bigint NOT NULL,
 late_corrections bigint NOT NULL,PRIMARY KEY(stream_id,minute_utc)
);
CREATE TABLE IF NOT EXISTS devpulse_stream.repository_minutes (
 stream_id text REFERENCES devpulse_stream.runs ON DELETE CASCADE,minute_utc timestamptz,repo_id bigint,
 total_events bigint NOT NULL,star_events bigint NOT NULL,fork_events bigint NOT NULL,
 PRIMARY KEY(stream_id,minute_utc,repo_id)
);
CREATE TABLE IF NOT EXISTS devpulse_stream.batches (
 stream_id text REFERENCES devpulse_stream.runs ON DELETE CASCADE,digest text,
 spark_batch_id bigint NOT NULL,committed_at timestamptz NOT NULL DEFAULT now(),
 input_rows bigint NOT NULL,new_messages bigint NOT NULL,new_events bigint NOT NULL,
 duplicates bigint NOT NULL,invalid bigint NOT NULL,conflicts bigint NOT NULL,late_events bigint NOT NULL,
 offset_ranges jsonb NOT NULL,PRIMARY KEY(stream_id,digest)
);
CREATE TABLE IF NOT EXISTS devpulse_stream.orchestration_runs (
 dag_id text,run_id text,state text NOT NULL,run_type text,started_at timestamptz,ended_at timestamptz,
 tasks jsonb NOT NULL,observed_at timestamptz NOT NULL DEFAULT now(),PRIMARY KEY(dag_id,run_id)
);
GRANT USAGE ON SCHEMA devpulse_stream TO devpulse_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA devpulse_stream TO devpulse_reader;
CREATE INDEX IF NOT EXISTS stream_minute_repo ON devpulse_stream.events(stream_id,minute_utc,repo_id);

ALTER TABLE devpulse_stream.runs ADD COLUMN IF NOT EXISTS bootstrap_servers text NOT NULL DEFAULT '127.0.0.1:19092';
