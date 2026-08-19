# User Guide

Exact commands to run this project from a clean checkout.

There are two paths. **Path A (Docker)** is recommended, and is the only one
that works on Windows. **Path B (native)** needs PostgreSQL, Java, and Spark
installed on the host.

---

# Path A — Docker

Nothing but Docker Desktop (with Compose v2) is required. No Python, Java,
Spark, PostgreSQL, or JDBC jar on the host.

## A1. Configure credentials

```powershell
Copy-Item .env.example .env
```

Edit `.env` and set a real `POSTGRES_PASSWORD`. Compose reads `POSTGRES_USER`,
`POSTGRES_DB`, and `POSTGRES_PASSWORD` from it.

`POSTGRES_HOST` and `POSTGRES_JDBC_JAR_PATH` in `.env` apply only to Path B;
Compose overrides both, since inside a container the database is reached at the
service name `postgres`, not `localhost`.

## A2. Start the pipeline

```powershell
docker compose up --build
```

This starts four services in order: `postgres` → `db-init` (applies the schema)
→ `generator` and `spark`. Use `--build` after any source change, since the
application code is baked into the image.

Detached, following just the streaming job:

```powershell
docker compose up --build -d
docker compose logs -f spark
```

Expected log lines:

```text
Starting Spark streaming pipeline...
Streaming query started. Watching /app/data/incoming every 2 seconds. Checkpoint: /app/checkpoints
Processing micro-batch 0 (3 records)...
Batch 0 successfully written (3 records, 0.842s).
```

The Spark UI is at <http://localhost:4040> while the job runs.

## A3. Validate records

```powershell
docker compose exec postgres psql -U postgres -d realtime_events -f /sql/validation_queries.sql
```

Interactively:

```powershell
docker compose exec postgres psql -U postgres -d realtime_events
```

PostgreSQL is also published on `localhost:5432` for pgAdmin or DBeaver.

## A4. Run tests

```powershell
docker compose run --rm tests
```

## A5. Generate a performance report

After letting the pipeline run long enough for
`outputs/performance/batch_metrics.csv` to accumulate rows:

```powershell
docker compose exec spark python -m src.monitoring.metrics
```

## A6. Stop

```powershell
docker compose down       # database and checkpoints survive
docker compose down -v    # also wipe them, for a clean run
```

Full Docker reference, including troubleshooting: `docs/docker_guide.md`.

---

# Path B — Native (Linux / macOS)

## B1. Create a virtual environment and install dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Use Python 3.10–3.12. PySpark 3.5.1 does not support 3.13.

## B2. Configure PostgreSQL

Make sure PostgreSQL is installed and running, then:

```bash
cp .env.example .env
# edit .env: set POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER,
# POSTGRES_PASSWORD to real values for your local PostgreSQL instance.
```

## B3. Set up the database and schema

```bash
psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U postgres -f sql/postgres_setup.sql
```

This creates the `realtime_events` database (if missing), connects to it, and
applies `sql/create_tables.sql` — the `events` table plus its indexes. It is
safe to re-run.

## B4. Get the PostgreSQL JDBC driver

Spark needs this jar to write to PostgreSQL:

```bash
mkdir -p drivers
curl -L -o drivers/postgresql-42.7.3.jar \
  https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar
```

Then set in `.env`:

```text
POSTGRES_JDBC_JAR_PATH=./drivers/postgresql-42.7.3.jar
```

## B5. Generate events

In one terminal:

```bash
source .venv/bin/activate
export PYTHONPATH="$PWD"
python -m src.generator.data_generator
```

This writes a new `data/incoming/events_<n>.csv` every `interval_seconds`
(see `config/config.yaml` → `generator`), numbering batches upward from 1.
Press Ctrl+C to stop.

## B6. Start the streaming pipeline

In a second terminal:

```bash
source .venv/bin/activate
export PYTHONPATH="$PWD"
set -a && . ./.env && set +a
spark-submit --jars "$POSTGRES_JDBC_JAR_PATH" spark_streaming_to_postgres.py
```

`PYTHONPATH` must include the repository root so Spark's driver process can
import the local `src` package. Expected output is the same as step A2.

## B7. Validate records in PostgreSQL

```bash
psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -f sql/validation_queries.sql
```

Or connect interactively and run individual queries, e.g.:

```sql
SELECT COUNT(*) FROM events;
SELECT event_type, COUNT(*) FROM events GROUP BY event_type;
```

## B8. Run tests

```bash
pytest
```

Integration tests that need a live PostgreSQL connection are skipped
automatically if `.env` isn't configured or the DB is unreachable.

## B9. Generate a performance report

After letting the pipeline run for a while (so
`outputs/performance/batch_metrics.csv` has real rows):

```bash
python -m src.monitoring.metrics
```

## B10. Stop the pipeline

Ctrl+C both the generator and the streaming job. Spark's checkpoint in
`checkpoints/` lets you resume later without reprocessing already-ingested
files.

---

# Starting over from scratch

To re-ingest everything from a clean slate, clear all three pieces of state
together — the generated CSVs, Spark's checkpoint, and the table:

```powershell
docker compose down -v
Remove-Item data/incoming/*.csv
```

Clearing the checkpoint without also clearing `data/incoming/` makes Spark
re-read every existing CSV. The `event_id` primary key rejects the duplicates,
so no bad data lands — but the log fills with constraint errors.
