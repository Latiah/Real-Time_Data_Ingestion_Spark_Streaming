# Project Overview

## Problem

Real-world e-commerce platforms generate continuous streams of user
activity (product views, purchases). Turning that raw event stream into
clean, queryable records in near-real-time is a core data engineering
problem: it requires ingesting data as it arrives, transforming/validating
it on the fly, and persisting it durably — all without ever having "all
the data" available at once, unlike a batch job.

This project builds a small-scale version of that pipeline end to end, to
demonstrate the mechanics of stream processing rather than to handle
production traffic volumes.

## Objectives

- Simulate a realistic (if synthetic) event stream.
- Ingest and process that stream with genuine Spark Structured Streaming
  (not a batch loop dressed up as streaming).
- Clean, validate, and enrich each micro-batch before storage.
- Persist results in PostgreSQL with basic duplicate protection.
- Measure real throughput/latency from an actual run, not estimates.

## Architecture (summary)

```text
Python Generator -> CSV files (data/incoming/) -> Spark readStream
    -> transformations.py (cast/validate/dedupe/derive)
    -> foreachBatch -> PostgreSQL (events table)
    -> validation_queries.sql / metrics.py
```

Full diagram and per-component explanation: `docs/architecture.md`.

## Data Flow

1. `src/generator/data_generator.py` writes a new CSV file every
   `interval_seconds` (see `config/config.yaml`), each containing
   `batch_size` synthetic events.
2. Spark's file-source `readStream` detects each new file (tracked via the
   checkpoint directory, so no file is processed twice by Spark itself).
3. `transformations.transform_events()` casts types, drops invalid rows,
   deduplicates within the micro-batch, and adds `total_amount` and
   `ingested_at`.
4. `foreachBatch` hands the cleaned micro-batch to
   `database.postgres.write_batch_to_postgres()`, which writes it via
   Spark's JDBC batch writer.
5. PostgreSQL's `PRIMARY KEY` on `event_id` rejects any record that somehow
   gets retried/duplicated across batches.
6. `sql/validation_queries.sql` and `src/monitoring/metrics.py` let you
   inspect the results and measure performance.

## Components

| Component | Responsibility |
|---|---|
| `src/generator/` | Synthetic event creation, CSV writing |
| `src/streaming/stream_reader.py` | Explicit schema + `readStream` source |
| `src/streaming/transformations.py` | Cleaning, validation, derived fields |
| `src/streaming/streaming_pipeline.py` | Wires reader → transform → sink |
| `src/database/postgres.py` | `foreachBatch` JDBC write, DB connection |
| `src/monitoring/metrics.py` | Per-batch metrics + aggregate report |
| `sql/` | Schema, indexes, validation/analytical queries |

## Design Decisions

- **CSV over JSON**: matches the project brief's deliverables and keeps the
  schema simple and explicit; no nested structures are needed for this
  event model.
- **Explicit Spark schema, not inference**: Structured Streaming does not
  support schema inference on streaming sources, and inference would be
  unreliable and slow even if it did.
- **`foreachBatch` for the PostgreSQL sink**: JDBC writes are inherently
  batch operations; `foreachBatch` is Spark's documented way to apply a
  batch write to each streaming micro-batch.
- **PostgreSQL `PRIMARY KEY` for dedup**, not Spark-side stateful
  aggregation: cross-batch deduplication in Spark would require stateful
  streaming (watermarks, state stores), which is unnecessary complexity
  for this project's scale. The database constraint is simpler and just as
  effective here.
- **A staging table between Spark and `events`**: Spark's JDBC writer can only
  append or overwrite, never "insert and skip duplicates". Since a failed batch
  is replayed by Spark on restart — and may have committed some partitions
  before failing — a plain append would hit the `event_id` primary key on
  replay and crash-loop. Each batch is therefore bulk-loaded into
  `events_staging` and merged with `ON CONFLICT DO NOTHING`, which keeps the
  bulk writer while making the write idempotent.

## Expected Output

- A running Spark streaming query that logs each detected file and batch.
- A steadily growing `events` table in PostgreSQL.
- A `outputs/performance/batch_metrics.csv` file with one row per
  micro-batch, from which `docs/performance_metrics.md` is derived.
