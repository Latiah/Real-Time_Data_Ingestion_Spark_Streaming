# Project Overview

The pipeline has three stages:

1. `data_generator.py` creates small CSV batches containing user, product, action, price, and UTC timestamp fields. It writes to a temporary filename and renames the completed file, preventing Spark from seeing partial data.
2. Spark Structured Streaming monitors the events directory, applies an explicit schema, trims text, converts timestamps and prices, rejects invalid events, and removes duplicate event IDs within each micro-batch.
3. `foreachBatch` writes valid rows to PostgreSQL over JDBC. Each micro-batch is first written to `public.user_events_staging`, then copied into `public.user_events` with `ON CONFLICT (event_id) DO NOTHING`. The staging rows are deleted after the copy, so retries are idempotent and duplicate event IDs are not inserted twice.

## PostgreSQL Storage

The SQL setup creates three tables:

- `public.user_events` is the final table. It stores accepted events, including `event_id`, user and product fields, price, event timestamp, ingestion timestamp, and source filename. The primary key on `event_id` prevents duplicates.
- `public.user_events_staging` is a temporary landing table for the current Spark micro-batch. Spark writes rows here through JDBC before the PostgreSQL commit step removes them after successful insertion into `user_events`.
- `public.streaming_batch_log` stores one row per Spark batch with rows received, rows written, and batch start and completion timestamps. These values support throughput and latency measurements.

The checkpoint directory stores streaming progress. PostgreSQL constraints provide a second validation boundary and indexes support timestamp and event-type queries. The generator and Spark job can run as separate processes on one machine or on appropriately configured hosts.
