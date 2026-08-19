# Real-Time E-Commerce Data Ingestion (Spark Structured Streaming + PostgreSQL)

A real-time data ingestion pipeline demonstrating Spark Structured Streaming,
data transformation, and PostgreSQL storage. Synthetic e-commerce events
(`view`, `purchase`) are generated as CSV files, picked up by a genuine Spark
Structured Streaming job, cleaned/validated, and written into PostgreSQL.

The whole stack runs in Docker — no Python, Java, Spark, or PostgreSQL install
is needed on the host.


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

- Apache Spark Structured Streaming (PySpark 3.5.1)
- PostgreSQL 16 + JDBC
- Python 3.11 (event generation)
- SQL (schema, indexes, validation queries)
- pytest (testing)
- Docker + Docker Compose

## Deliverables

Where each item from the project brief lives in this repository.

| Deliverable | Location | Notes |
|---|---|---|
| `data_generator.py` | [src/generator/data_generator.py](src/generator/data_generator.py) | Writes `data/incoming/events_<n>.csv` |
| `spark_streaming_to_postgres.py` | [spark_streaming_to_postgres.py](spark_streaming_to_postgres.py) | `spark-submit` entry point; implementation split across [src/streaming/](src/streaming/) so each stage is unit-testable |
| `postgres_setup.sql` | [sql/postgres_setup.sql](sql/postgres_setup.sql) | Creates the database, then applies [sql/create_tables.sql](sql/create_tables.sql) |
| `postgres_connection_details.txt` | [postgres_connection_details.txt](postgres_connection_details.txt) | Working credentials; runtime config comes from `.env` |
| `project_overview.md` | [docs/project_overview.md](docs/project_overview.md) | Components and data flow |
| `user_guide.md` | [docs/user_guide.md](docs/user_guide.md) | Step-by-step run instructions |
| `test_cases.md` | [docs/test_cases.md](docs/test_cases.md) | 27 automated tests + manual E2E plan |
| `performance_metrics.md` | [docs/performance_metrics.md](docs/performance_metrics.md) | Measured throughput and latency |
| `system_architecture.png` | [docs/system_architecture.png](docs/system_architecture.png) | Data-flow diagram (regenerate with [tools/render_architecture.py](tools/render_architecture.py)) |

## Repository Structure

```text
Real-Time_Data_Ingestion_Spark_Streaming/
├── .dockerignore                     # keeps .env and build cruft out of the image
├── .env                              # real credentials (git-ignored)
├── .env.example                      # credentials template (copy to .env)
├── .gitignore
├── README.md
├── docker-compose.yml                # postgres + db-init + generator + spark
├── pyproject.toml                    # pytest config (testpaths, pythonpath)
├── requirements.txt                  # Python dependencies
├── spark_streaming_to_postgres.py    # spark-submit entry point
├── postgres_connection_details.txt   # host, port, user, password
│
├── docker/
│   └── Dockerfile                    # Python 3.11 + JDK 17 + PySpark + JDBC jar
│
├── config/
│   └── config.yaml                   # non-secret settings (batch size, intervals)
│
├── sql/
│   ├── postgres_setup.sql            # creates database, then includes create_tables.sql
│   ├── create_tables.sql             # events table + indexes
│   └── validation_queries.sql        # verification / analytical queries
│
├── src/
│   ├── __init__.py
│   ├── config/
│   │   ├── __init__.py
│   │   └── settings.py               # loads config.yaml + .env
│   ├── generator/
│   │   ├── __init__.py
│   │   └── data_generator.py         # writes data/incoming/events_<n>.csv
│   ├── streaming/
│   │   ├── __init__.py
│   │   ├── spark_session.py          # SparkSession construction
│   │   ├── stream_reader.py          # explicit schema + readStream source
│   │   ├── transformations.py        # cast / validate / dedupe / derive
│   │   └── streaming_pipeline.py     # wires reader -> transform -> sink
│   ├── database/
│   │   ├── __init__.py
│   │   └── postgres.py               # foreachBatch JDBC write + connection helper
│   └── monitoring/
│       ├── __init__.py
│       └── metrics.py                # per-batch metrics + aggregate report
│
├── tests/
│   ├── test_data_generator.py        # 12 tests
│   ├── test_transformations.py       # 10 tests
│   └── test_database.py              # 5 tests (2 need a live DB)
│
├── tools/
│   └── render_architecture.py        # regenerates docs/system_architecture.png
│
├── docs/
│   ├── project_overview.md           # components and data flow
│   ├── architecture.md               # per-component walkthrough
│   ├── user_guide.md                 # step-by-step run instructions
│   ├── docker_guide.md               # Docker reference + troubleshooting
│   ├── test_cases.md                 # 27 automated tests + manual E2E plan
│   ├── performance_metrics.md        # measured throughput and latency
│   └── system_architecture.png       # data-flow diagram
│
├── data/
│   ├── incoming/                     # CSVs land here; Spark watches this dir
│   └── processed/                    # reserved for archiving
├── checkpoints/                      # Spark streaming checkpoint state
├── logs/
└── outputs/
    └── performance/                  # batch_metrics.csv written here at run time
```

The four runtime directories (`data/`, `checkpoints/`, `logs/`, `outputs/`) ship
empty — their contents are generated by running the pipeline and are
git-ignored.

## Prerequisites

Docker Desktop with Compose v2. Nothing else.

## Quick Start

```powershell
Copy-Item .env.example .env      # then set a real POSTGRES_PASSWORD
docker compose up --build
```

That starts four services in order: `postgres`, `db-init` (applies the schema),
then `generator` and `spark`. Use `--build` after any source change — the
application code is baked into the image.

```powershell
docker compose logs -f spark     # watch the streaming job
docker compose down              # stop (add -v to also wipe the database)
```

The Spark UI is at <http://localhost:4040> while the job runs.

Full details, including troubleshooting: [docs/docker_guide.md](docs/docker_guide.md).
Step-by-step instructions: [docs/user_guide.md](docs/user_guide.md).

## Services

| Service | Role |
|---|---|
| `postgres` | PostgreSQL 16; creates the `realtime_events` database, published on `localhost:5432` |
| `db-init` | One-shot; applies [sql/create_tables.sql](sql/create_tables.sql), then exits |
| `generator` | Writes `data/incoming/events_<n>.csv` every `interval_seconds` |
| `spark` | Runs `spark-submit … spark_streaming_to_postgres.py` |
| `tests` | Opt-in (`tools` profile); `docker compose run --rm tests` |

## Configuration

Secrets (DB host/port/user/password) live in `.env`, which is git-ignored.
Non-secret tuning — batch size, intervals, Spark app name — lives in
[config/config.yaml](config/config.yaml). See
[src/config/settings.py](src/config/settings.py) for how both are loaded.

`config/` is bind-mounted read-only, so `config.yaml` changes take effect on
`docker compose restart` without a rebuild.

Compose overrides `POSTGRES_HOST` and `POSTGRES_JDBC_JAR_PATH` from `.env`:
inside a container the database is reached at the service name `postgres`
rather than `localhost`, and the JDBC driver jar lives at a fixed path in the
image. Database setup and the JDBC driver both need no manual steps — the
`postgres` and `db-init` services handle the schema, and
[docker/Dockerfile](docker/Dockerfile) downloads the jar at build time.

## Verifying Records

```powershell
docker compose exec postgres psql -U postgres -d realtime_events -f /sql/validation_queries.sql
```

Or interactively:

```powershell
docker compose exec postgres psql -U postgres -d realtime_events
```

PostgreSQL is published on `localhost:5432`, so pgAdmin or DBeaver can also
connect using the credentials from `.env`.

## Testing

```powershell
docker compose run --rm tests
```

27 automated tests: generator and SQL-structure units run anywhere,
transformation tests spin up a local PySpark session, and database integration
tests are skipped automatically if no DB is reachable. See
[docs/test_cases.md](docs/test_cases.md).

## Performance Evaluation

```powershell
docker compose exec spark python -m src.monitoring.metrics
```

Prints aggregated throughput/latency statistics computed from
`outputs/performance/batch_metrics.csv`, which is populated automatically as
the streaming pipeline runs. See
[docs/performance_metrics.md](docs/performance_metrics.md) for methodology and
measured results.

## Starting Over

To re-ingest from a clean slate, clear all three pieces of state together —
generated CSVs, Spark's checkpoint, and the table:

```powershell
docker compose down -v
Remove-Item data/incoming/*.csv
```

Clearing the checkpoint without also clearing `data/incoming/` makes Spark
re-read every existing CSV. The `event_id` primary key rejects the duplicates,
so no bad data lands — but the log fills with constraint errors.
