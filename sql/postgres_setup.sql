-- Run once as an administrator, if the database does not exist:
-- CREATE DATABASE events_db;
-- Then connect to events_db and run the statements below.

CREATE SCHEMA IF NOT EXISTS public;

CREATE TABLE IF NOT EXISTS public.user_events (
	event_id VARCHAR(36) PRIMARY KEY,
	user_id VARCHAR(64) NOT NULL,
	event_type VARCHAR(16) NOT NULL CHECK (event_type IN ('view', 'purchase')),
	product_id VARCHAR(64) NOT NULL,
	product_name VARCHAR(255) NOT NULL,
	price NUMERIC(12, 2) NOT NULL CHECK (price >= 0),
	event_timestamp TIMESTAMPTZ NOT NULL,
	ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
	source_file TEXT);

ALTER TABLE public.user_events
	ADD COLUMN IF NOT EXISTS source_file TEXT;

CREATE TABLE IF NOT EXISTS public.user_events_staging (
	batch_id BIGINT NOT NULL,
	event_id VARCHAR(36) NOT NULL,
	user_id VARCHAR(64) NOT NULL,
	event_type VARCHAR(16) NOT NULL,
	product_id VARCHAR(64) NOT NULL,
	product_name VARCHAR(255) NOT NULL,
	price NUMERIC(12, 2) NOT NULL,
	event_timestamp TIMESTAMPTZ NOT NULL,
	ingested_at TIMESTAMPTZ NOT NULL,
	source_file TEXT);

ALTER TABLE public.user_events_staging
	ADD COLUMN IF NOT EXISTS source_file TEXT;

CREATE INDEX IF NOT EXISTS idx_user_events_staging_batch_id
	ON public.user_events_staging (batch_id);

CREATE INDEX IF NOT EXISTS idx_user_events_timestamp
	ON public.user_events (event_timestamp);
CREATE INDEX IF NOT EXISTS idx_user_events_type
	ON public.user_events (event_type);

CREATE TABLE IF NOT EXISTS public.streaming_batch_log (
	spark_batch_id BIGINT PRIMARY KEY,
	rows_received BIGINT NOT NULL,
	rows_written BIGINT NOT NULL,
	batch_started_at TIMESTAMPTZ NOT NULL,
	batch_completed_at TIMESTAMPTZ NOT NULL);
