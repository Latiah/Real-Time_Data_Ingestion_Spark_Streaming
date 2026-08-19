# Performance Metrics

## What Is Measured

For every micro-batch processed by `foreachBatch` in
`src/database/postgres.py`:

- `record_count` — number of rows in that micro-batch after cleaning/filtering
- `elapsed_seconds` — wall-clock time to write the batch to PostgreSQL
  (measured with `time.perf_counter()` immediately around the JDBC write)
- `throughput_rows_per_sec` — `record_count / elapsed_seconds`

These are appended to `outputs/performance/batch_metrics.csv` as the
pipeline runs (see `src/monitoring/metrics.py`).

## Methodology

- **Processing time** is measured only around the PostgreSQL write itself,
  not the full micro-batch (which would also include time already
  accounted for by Spark's own trigger scheduling). This isolates the cost
  this project's code is actually responsible for.
- **Throughput** is computed per batch and also as an overall figure
  (`total_records_processed / total_processing_time_seconds`) — the two
  can differ meaningfully if batch sizes vary.
- **Latency** (see `sql/validation_queries.sql`, query 9) is approximated
  as `ingested_at - event_timestamp`, i.e. the time between when an event
  was generated and when it was durably written to PostgreSQL. This
  captures generator interval + Spark trigger interval + processing time
  combined, not just the PostgreSQL write.
- Metrics are only ever recorded from real runs (`record_batch_metrics` is
  called only from live `foreachBatch` execution) — nothing in this report
  is estimated or fabricated.


## Measured Results

From a real pipeline run. To refresh after another run, the first six figures
come from:

```bash
python -m src.monitoring.metrics          # or: docker compose exec spark python -m src.monitoring.metrics
```

`average_latency_seconds` is **not** produced by that command — it comes from
query 9 in `sql/validation_queries.sql`:

```sql
SELECT AVG(EXTRACT(EPOCH FROM (ingested_at - event_timestamp))) AS avg_lag_seconds FROM events;
```

```text
total_batches: 729
total_records_processed: 10787
total_processing_time_seconds: 644.687
average_batch_processing_time_seconds: 0.8843
average_throughput_rows_per_sec: 15.98
overall_throughput_rows_per_sec: 16.73
average_latency_seconds: 508.1782969768946396
```

## Interpretation

- **Throughput (~16 rows/sec)** is bounded by the generator, not the pipeline.
  At `batch_size: 3` every `interval_seconds: 2`, only ~1.5 rows/sec are
  produced, so this figure measures how fast PostgreSQL writes accepted a batch
  once one arrived — it is not a saturation benchmark. Raising `batch_size` and
  lowering `interval_seconds` in `config/config.yaml` is what would push toward
  the real ceiling.
- **Average batch write ~0.88s** for ~15 rows is dominated by per-batch fixed
  cost (JDBC connection setup and Spark job scheduling), not row volume. Larger
  batches should therefore raise throughput substantially without a
  proportional rise in per-batch time.
- **Average latency ~508s (8.5 min)** is far larger than the per-batch write
  time because it measures `ingested_at - event_timestamp` across *every* row
  in the table. Any backlog is included: files sitting in `data/incoming/`
  before the streaming job started, or accumulated while it was stopped, are
  counted from their original event timestamp. It is a measure of end-to-end
  freshness over the whole table, not of steady-state pipeline lag. For
  steady-state latency, restrict the query to recent rows, e.g.
  `WHERE ingested_at > NOW() - INTERVAL '5 minutes'`.
