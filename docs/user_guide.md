# User Guide

Exact commands to run this project from a clean checkout.

---

# Docker

Docker Desktop (with Compose v2) is required. No Python, Java,
Spark, PostgreSQL, or JDBC jar on the host.

## 1. Configure credentials

```powershell
Copy-Item .env.example .env
```

Edit `.env` and set a real `POSTGRES_PASSWORD`. Compose reads `POSTGRES_USER`,
`POSTGRES_DB`, and `POSTGRES_PASSWORD` from it.

`POSTGRES_HOST` and `POSTGRES_JDBC_JAR_PATH` in `.env` are ignored. Compose
overrides both, since inside a container the database is reached at the service
name `postgres` rather than `localhost`, and the JDBC driver jar is already in
the image.

## 2. Start the pipeline

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

## 3. Validate records

```powershell
docker compose exec postgres psql -U postgres -d realtime_events -f /sql/validation_queries.sql
```

Interactively:

```powershell
docker compose exec postgres psql -U postgres -d realtime_events
```

## 4. Run tests

```powershell
docker compose run --rm tests
```

## 5. Generate a performance report

After letting the pipeline run long enough for
`outputs/performance/batch_metrics.csv` to accumulate rows:

```powershell
docker compose exec spark python -m src.monitoring.metrics
```

## 6. Stop

```powershell
docker compose down       # database and checkpoints survive
docker compose down -v    # also wipe them, for a clean run
```

Full Docker reference, including troubleshooting: `docs/docker_guide.md`.
