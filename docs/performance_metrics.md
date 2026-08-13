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

*(Run the pipeline, then regenerate this section with:*
`python -m src.monitoring.metrics`*)*

```text
total_batches: 729
total_records_processed: 10787
total_processing_time_seconds: 644.687
average_batch_processing_time_seconds: 0.8843
average_throughput_rows_per_sec: 15.98
overall_throughput_rows_per_sec: 16.73
average_latency_seconds: 508.1782969768946396
```
