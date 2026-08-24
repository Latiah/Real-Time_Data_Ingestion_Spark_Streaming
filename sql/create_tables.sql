-- Run this connected to the realtime_events database.
-- Creates the events table that Spark writes processed records into.

CREATE TABLE IF NOT EXISTS events (
    event_id        VARCHAR(64) PRIMARY KEY,   -- UUID string; PK also enforces
                                                -- cross-batch duplicate protection
    user_id         INTEGER NOT NULL,
    product_id      INTEGER NOT NULL,
    event_type      VARCHAR(20) NOT NULL CHECK (event_type IN ('view', 'purchase')),
    event_timestamp TIMESTAMP NOT NULL,
    product_name    VARCHAR(255),
    quantity        INTEGER NOT NULL DEFAULT 0 CHECK (quantity >= 0),
    price           NUMERIC(10, 2) NOT NULL CHECK (price >= 0),
    total_amount    NUMERIC(12, 2) NOT NULL DEFAULT 0,
    ingested_at     TIMESTAMP NOT NULL DEFAULT NOW()  -- pipeline ingestion metadata
);

-- Indexes to support the analytical/validation queries in validation_queries.sql.
CREATE INDEX IF NOT EXISTS idx_events_event_type ON events (event_type);
CREATE INDEX IF NOT EXISTS idx_events_user_id ON events (user_id);
CREATE INDEX IF NOT EXISTS idx_events_event_timestamp ON events (event_timestamp);


-- Landing table for one Spark micro-batch at a time. Spark's JDBC writer has
-- no "insert, ignoring duplicates" mode, so each batch is written here first
-- and then merged into `events` with ON CONFLICT DO NOTHING (see
-- src/database/postgres.py). That makes the write idempotent: if a batch is
-- retried after a failure -- which Spark will do, because a failed batch's
-- offsets are never committed -- the rows that already landed are skipped
-- instead of raising a primary-key violation and wedging the query.
--
-- LIKE copies the column names and types from `events` so the two cannot drift
-- apart. Constraints are deliberately not copied: `events` remains the sole
-- gatekeeper, and this table holds only already-validated rows.
--
-- UNLOGGED because the contents are disposable -- truncated before every batch
-- and never read after the merge -- so there is no reason to pay for WAL.
CREATE UNLOGGED TABLE IF NOT EXISTS events_staging (LIKE events);
