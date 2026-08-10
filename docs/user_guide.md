# User Guide

## Prerequisites

- Python 3.9+ and Apache Spark 3.4+
- OpenJDK 17+
- PostgreSQL 13+
- PostgreSQL JDBC driver available to Spark

On Ubuntu/Debian, install Java separately from Python packages:

```bash
sudo apt update
xargs sudo apt install -y < system-requirements.txt
java -version
```

## 1. Configure PostgreSQL

Create a database named `events_db`, then run `sql/postgres_setup.sql` while connected to it:

```bash
createdb events_db
psql -d events_db -f sql/postgres_setup.sql
```

The file `docs/postgres_connection_details.txt` is a template. Set the real password only in the shell:

```bash
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DATABASE=events_db
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD='your-password'
```

## 2. Start the Spark job

Supply the PostgreSQL JDBC driver with `spark-submit`:

```bash
spark-submit \
	--packages org.postgresql:postgresql:42.7.4 \
	src/spark_streaming_to_postgres.py
```

The job watches `data/events`, processes at most one file every two seconds, and checkpoints in `data/checkpoint`.

## 3. Generate events

In a second terminal, run a finite smoke test:

```bash
python src/data_generator.py --batches 5 --events-per-batch 25 --interval-seconds 2 --seed 7
```

For continuous generation, omit `--batches`. Stop either process with `Ctrl+C`. Do not delete the checkpoint directory while reusing the same input stream; use a new checkpoint when intentionally replaying files.

## 4. Verify the data

```bash
psql -d events_db -c "SELECT event_type, COUNT(*) FROM public.user_events GROUP BY event_type;"
psql -d events_db -c "SELECT * FROM public.user_events ORDER BY ingested_at DESC LIMIT 10;"
```

## 5. Understand the table writes

Spark stores each valid micro-batch in three stages:

1. Rows are written temporarily to `public.user_events_staging` through JDBC.
2. PostgreSQL copies them into `public.user_events`. Duplicate `event_id` values are ignored with `ON CONFLICT DO NOTHING`.
3. The processed staging rows are deleted, and batch counts and timestamps are recorded in `public.streaming_batch_log`.

Check the final event table:

```bash
psql -d events_db -c "SELECT COUNT(*) AS event_count FROM public.user_events;"
psql -d events_db -c "SELECT event_id, event_type, price, source_file, ingested_at FROM public.user_events ORDER BY ingested_at DESC LIMIT 10;"
```

Check the staging and batch-log tables:

```bash
psql -d events_db -c "SELECT COUNT(*) AS remaining_staging_rows FROM public.user_events_staging;"
psql -d events_db -c "SELECT * FROM public.streaming_batch_log ORDER BY spark_batch_id DESC LIMIT 10;"
```

After a successful batch, `remaining_staging_rows` should normally be zero,
while `event_count` and the batch log should increase.

## Useful options

The generator supports `--output-dir`, `--events-per-batch`, `--interval-seconds`, `--batches`, and `--seed`. The Spark job supports `--input-dir`, `--checkpoint-dir`, PostgreSQL connection options, `--trigger-seconds`, and `--max-files-per-trigger`. PostgreSQL options also read `POSTGRES_*` environment variables.
