# Running the Pipeline with Docker

Docker is the supported way to run the pipeline there. Docker also removes
three version traps that bite on any host:

| Requirement | Why the host often fails | How Docker fixes it |
| --- | --- | --- |
| Python 3.10–3.12 | PySpark 3.5.1 does not support Python 3.13 | Image pins Python 3.11 |
| Java 8/11/17 | Spark 3.5.x does not officially support Java 21+ | Image installs JDK 17 |
| PostgreSQL JDBC jar | Manual download into `./drivers/` | Baked into the image |
| `psql` client | Not installed on Windows by default | Provided by the `postgres` image |

## Prerequisites

Only Docker Desktop (with Compose v2). No Python, Java, Spark, or PostgreSQL
install is needed on the host.

## 1. Set credentials

```powershell
Copy-Item .env.example .env
```

Then edit `.env` and set a real `POSTGRES_PASSWORD`. Compose reads
`POSTGRES_USER`, `POSTGRES_DB`, and `POSTGRES_PASSWORD` from this file for both
the database and the application containers.

`POSTGRES_HOST` and `POSTGRES_JDBC_JAR_PATH` in `.env` are only used for native
(non-Docker) runs. Compose overrides both, because inside a container
`localhost` is the container itself, not the database. This works because
`load_dotenv()` does not overwrite variables that are already set in the
environment — see [settings.py](../src/config/settings.py).

## 2. Start everything

```powershell
docker compose up --build
```

Four services come up in order:

1. **`postgres`** — PostgreSQL 16. Creates the `realtime_events` database from
   `POSTGRES_DB` and reports ready via a `pg_isready` healthcheck.
2. **`db-init`** — one-shot; applies [sql/create_tables.sql](../sql/create_tables.sql).
   This replaces `scripts/setup_database.sh`. `sql/postgres_setup.sql`
   (`CREATE DATABASE`) is not needed, because the `postgres` image already
   creates the database.
3. **`generator`** — runs `python -m src.generator.data_generator`, writing CSV
   batches into `data/incoming/`.
4. **`spark`** — runs `spark-submit --jars <jdbc jar> src/streaming/streaming_pipeline.py`.

To run detached and follow just the streaming job:

```powershell
docker compose up --build -d
docker compose logs -f spark
```

The Spark UI is published at <http://localhost:4040> while the job runs.

## 3. Verify the data

Run the validation queries through the database container (`sql/` is mounted
there read-only):

```powershell
docker compose exec postgres psql -U postgres -d realtime_events -f /sql/validation_queries.sql
```

An interactive session:

```powershell
docker compose exec postgres psql -U postgres -d realtime_events
```

PostgreSQL is also published on `localhost:5432`, so pgAdmin, DBeaver, or a
host `psql` can connect using the credentials from `.env`.

## 4. Performance metrics

The streaming job appends a row per micro-batch to
`outputs/performance/batch_metrics.csv` on the host. Aggregate it with:

```powershell
docker compose exec spark python -m src.monitoring.metrics
```

## 5. Tests

`tests` is behind a `tools` profile, so it does not start with `up`:

```powershell
docker compose run --rm tests
```

## Stopping

```powershell
docker compose down       # stop containers; database and checkpoints survive
docker compose down -v    # also delete the pgdata and checkpoints volumes
```

Use `down -v` for a genuinely clean run. Because the Spark checkpoint records
which CSV files were already consumed, deleting `checkpoints` without also
clearing `data/incoming/` will make Spark re-read every existing file — the
`event_id` primary key then rejects the duplicates, which is by design but
fills the log with constraint errors.

## What lives where

| Path | Storage | Why |
| --- | --- | --- |
| `data/`, `logs/`, `outputs/` | host bind mount | inspectable from Windows |
| `config/` | host bind mount, read-only | tune `config.yaml` without a rebuild |
| `checkpoints` | named volume | Spark's checkpoint metadata needs atomic renames, which are less reliable across the Windows/Linux filesystem boundary |
| `pgdata` | named volume | database durability across `down`/`up` |

`data/incoming/` is bind-mounted into both `generator` and `spark`, which is
how the CSV files pass between them. Spark's file source discovers new files by
listing the directory rather than by filesystem notifications, so a bind mount
is reliable here.

## Common issues

**`required variable POSTGRES_PASSWORD is missing`** — `.env` does not exist or
has no `POSTGRES_PASSWORD`. See step 1.

**Source changes seem to have no effect** — application source is copied into
the image, not mounted. Re-run `docker compose up --build`. (`config/config.yaml`
is the exception; it is mounted and picked up on restart.)

**`db-init` exits non-zero** — read `docker compose logs db-init`. It runs with
`ON_ERROR_STOP=1`, so any SQL error stops the dependent services rather than
letting Spark start against a missing schema.

**Port 5432 already in use** — a PostgreSQL service is already running on the
host. Either stop it, or change the published port to e.g. `"5433:5432"` in
`docker-compose.yml`.
