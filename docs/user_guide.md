# User Guide

Exact commands to run this project from a clean checkout.

## 1. Create a virtual environment and install dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 2. Configure PostgreSQL

Make sure PostgreSQL is installed and running, then:

```bash
cp .env.example .env
# edit .env: set POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER,
# POSTGRES_PASSWORD to real values for your local PostgreSQL instance.
```

## 3. Set up the database schema

```bash
./scripts/setup_database.sh
```

This runs `sql/postgres_setup.sql`, which creates the database (if missing)
and the `events` table plus indexes.

## 4. Get the PostgreSQL JDBC driver

Spark needs this jar to write to PostgreSQL:

```bash
mkdir -p drivers
curl -L -o drivers/postgresql-42.7.3.jar \
  https://jdbc.postgresql.org/download/postgresql-42.7.3.jar
```

Then set in `.env`:

```text
POSTGRES_JDBC_JAR_PATH=./drivers/postgresql-42.7.3.jar
```

## 5. Generate events

In one terminal:

```bash
source .venv/bin/activate
./scripts/generate_events.sh
```

This writes a new CSV file into `data/incoming/` every few seconds
(configurable via `config/config.yaml` → `generator.interval_seconds`).
Press Ctrl+C to stop.

## 6. Start the streaming pipeline

In a second terminal:

```bash
source .venv/bin/activate
./scripts/run_streaming.sh
```

You should see log lines like:

```text
Starting Spark streaming pipeline...
Streaming query started. Watching data/incoming every 5 seconds. Checkpoint: checkpoints
Processing micro-batch 0 (50 records)...
Batch 0 successfully written (50 records, 0.842s).
```

## 7. Validate records in PostgreSQL

```bash
psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -d $POSTGRES_DB \
  -f sql/validation_queries.sql
```

Or connect interactively and run individual queries from that file, e.g.:

```sql
SELECT COUNT(*) FROM events;
SELECT event_type, COUNT(*) FROM events GROUP BY event_type;
```

## 8. Run tests

```bash
pytest
```

Integration tests that need a live PostgreSQL connection are skipped
automatically if `.env` isn't configured or the DB is unreachable.

## 9. Generate a performance report

After letting the pipeline run for a while (so
`outputs/performance/batch_metrics.csv` has real rows):

```bash
python -m src.monitoring.metrics
```

## 10. Stop the pipeline

Ctrl+C both the generator and the streaming job. Spark's checkpoint in
`checkpoints/` lets you resume later without reprocessing already-ingested
files.
