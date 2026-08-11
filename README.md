# Real-Time E-Commerce Data Ingestion (Spark Structured Streaming + PostgreSQL)

A real-time data ingestion pipeline demonstrating Spark Structured Streaming,
data transformation, and PostgreSQL storage. Synthetic e-commerce events
(`view`, `purchase`) are generated as CSV files, picked up by a genuine Spark
Structured Streaming job, cleaned/validated, and written into PostgreSQL.


## Overview

```text
Python Generator -> CSV files -> Spark Structured Streaming
    -> Transformation & Validation -> PostgreSQL -> Validation & Metrics
```

- **Python** generates synthetic events and writes them as CSV files.
- **Spark Structured Streaming** (`spark.readStream`) monitors the CSV
  directory, applies an explicit schema, cleans and validates each
  micro-batch, and writes results via `foreachBatch`.
- **PostgreSQL** stores the processed events, with a `UNIQUE`/primary key
  constraint on `event_id` acting as the durable duplicate-prevention layer.


## Objectives

- Simulate and ingest streaming data
- Process data in real time with Spark Structured Streaming
- Store and verify processed data in PostgreSQL
- Understand the architecture of a real-time pipeline
- Measure and evaluate system performance

## Technologies

- Python 3.10+ (event generation)
- Apache Spark Structured Streaming (PySpark)
- PostgreSQL + JDBC
- SQL (schema, indexes, validation queries)
- pytest (testing)

## Repository Structure

```text
realtime-spark-postgres/
├── config/config.yaml          # non-secret settings
├── .env.example                 # secret settings template (copy to .env)
├── data/incoming/                # CSVs land here; Spark watches this dir
├── data/processed/               # (reserved for archiving, see Limitations)
├── checkpoints/                  # Spark streaming checkpoint state
├── outputs/performance/          # measured metrics CSV + reports
├── sql/                          # schema + validation queries
├── src/
│   ├── config/settings.py        # loads config.yaml + .env
│   ├── generator/data_generator.py
│   ├── streaming/                # spark_session, stream_reader,
│   │                              # transformations, streaming_pipeline
│   ├── database/postgres.py      # JDBC write + connection helper
│   └── monitoring/metrics.py     # performance measurement
├── tests/                        # unit + integration tests
├── docs/                         # detailed documentation (see below)
└── scripts/                      # setup_database.sh, generate_events.sh,
                                   # run_streaming.sh
```

## Prerequisites

- Python 3.10+
- PostgreSQL 13+ running locally (or reachable over the network)
- Java 8/11/17 (required by Spark) and Apache Spark 3.5.x
- The PostgreSQL JDBC driver jar (see [PostgreSQL JDBC Setup](#postgresql-jdbc-setup))

## Installation

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## PostgreSQL Setup

```bash
cp .env.example .env
# edit .env with your real PostgreSQL credentials
./scripts/setup_database.sh
```

This creates the `realtime_events` database and applies `sql/create_tables.sql`.

## Environment Configuration

All secrets (DB host/port/user/password, JDBC jar path) live in `.env`,
which is git-ignored. Non-secret tuning (batch size, intervals, Spark app
name) lives in `config/config.yaml`. See `src/config/settings.py` for how
both are loaded.

## PostgreSQL JDBC Setup

Spark needs the PostgreSQL JDBC driver jar to write via `foreachBatch`.

1. Download it from the official Maven Central page for `org.postgresql:postgresql`
   (e.g. version `42.7.3` — matches `config/config.yaml`).
2. Place the jar anywhere convenient, e.g. `./drivers/postgresql-42.7.3.jar`.
3. Set `POSTGRES_JDBC_JAR_PATH` in `.env` to that path.
4. `scripts/run_streaming.sh` passes it to Spark via `spark-submit --jars`.

## Running the Pipeline

Terminal 1 — start the generator:

```bash
./scripts/generate_events.sh
```

Terminal 2 — start the streaming job:

```bash
./scripts/run_streaming.sh
```

## Verifying Records

```bash
psql -h $POSTGRES_HOST -U $POSTGRES_USER -d $POSTGRES_DB -f sql/validation_queries.sql
```

Or run individual queries from that file — see `docs/user_guide.md`.

## Testing

```bash
pytest
```

Unit tests (generator, transformations, SQL structure) run everywhere.
Integration tests against a live PostgreSQL instance are skipped
automatically if no DB is reachable. See `docs/testing.md`.

## Performance Evaluation

```bash
python -m src.monitoring.metrics
```

Prints aggregated throughput/latency statistics computed from
`outputs/performance/batch_metrics.csv`, which is populated automatically
as the streaming pipeline runs. See `docs/performance_metrics.md` for
methodology and (once measured) actual results.
