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
