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

  > **This was not always true.** Run A below (2026-08-19) was recorded when the
  > timer started *before* `batch_df.count()`, so each `elapsed_seconds` value
  > included a full extra pass over the batch — re-reading the source CSVs and
  > re-running `dropDuplicates` — on top of the write. The batch is now
  > `persist()`ed and the timer starts immediately before the write, so Run A's
  > times are inflated by an unknown margin and are not comparable with Run B's.
- **Throughput** is reported two ways. The per-batch average weights every
  batch equally; the overall figure
  (`total_records_processed / total_processing_time_seconds`) weights each batch
  by the time it consumed. They diverge whenever batch times vary — see
  Interpretation for what that looks like in this run.
- **Latency** (see `sql/validation_queries.sql`, query 9) is approximated
  as `ingested_at - event_timestamp`, i.e. the time between when an event
  was generated and when it was durably written to PostgreSQL. This
  captures generator interval + Spark trigger interval + processing time
  combined, not just the PostgreSQL write.
- Metrics are only ever recorded from real runs (`record_batch_metrics` is
  called only from live `foreachBatch` execution) — nothing in this report
  is estimated or fabricated.


## Measured Results

Reproduce with:

```bash
docker compose exec spark python -m src.monitoring.metrics
```

`batch_metrics.csv` is append-only and now spans **two runs measured
differently**, so the whole-file aggregate below blends them and should not be
quoted as a single result:

```text
total_batches: 34
total_records_processed: 510
total_processing_time_seconds: 35.2064
average_batch_processing_time_seconds: 1.0355
average_throughput_rows_per_sec: 14.53
overall_throughput_rows_per_sec: 14.49
```

Split by run, which is what the numbers actually mean:

| | Run A (batches 0–31) | Run B (batches 32–33) |
|---|---|---|
| Date | 2026-08-19 | 2026-08-24 |
| Batches | 32 | 2 |
| Records | 165 | 345 |
| Total time | 28.34s | 6.87s |
| Mean batch time | 0.886s | 3.43s |
| Overall throughput | 5.82 rows/s | 50.2 rows/s |
| Timer encloses `count()`? | **Yes** (inflated) | No |

Run A's per-batch times each include a full recomputation of the batch, so they
overstate the write by an unknown margin. Run B measures the write alone.

Run B is only two batches and both are atypical: batch 32 was a **replay** of
an already-stored batch (150 rows, 0 inserted, 6.04s — all conflict-checking
plus JVM warm-up), and batch 33 drained a 195-row backlog in 0.83s, giving
**235.6 rows/sec** — the highest figure measured, and the one closest to a real
write-path number.

For a clean, quotable benchmark, archive the existing file and record a fresh
continuous run:

```powershell
Move-Item outputs/performance/batch_metrics.csv outputs/performance/batch_metrics_run_a.csv
docker compose up -d generator
```

### Latency

Not included above, because `metrics.py` does not compute it. Latency comes from
query 9 in `sql/validation_queries.sql`, run against a populated database:

```sql
SELECT AVG(EXTRACT(EPOCH FROM (ingested_at - event_timestamp))) AS avg_lag_seconds FROM events;
```

An earlier run measured ~508s, but the table it was measured against no longer
exists, so that figure cannot be re-derived and is not reported here as
current. Re-run the query after a fresh ingest — and read the caveat under
Interpretation before quoting the result.

## Interpretation

- **None of these figures is a ceiling.** The generator produces `batch_size: 3`
  events every `interval_seconds: 2` — about 1.5 rows/sec — so the pipeline
  spent both runs waiting for input rather than saturated by it. The numbers
  describe how quickly an arriving batch was accepted, not how much the pipeline
  could absorb. Raising `batch_size` and lowering `interval_seconds` in
  `config/config.yaml` is what would push toward an actual limit.
- **Batch size dominates throughput, because the cost is mostly fixed.** Run A
  averaged ~5 rows per batch and reached 5.8 rows/sec; batch 33 handled 195 rows
  in one go and reached 235.6. Per-batch overhead — setting up the JDBC write,
  scheduling the Spark job, the staging truncate and merge — is paid once per
  batch regardless of row count, so larger batches amortise it. This is the
  single most useful lever in the whole report.
- **The two throughput figures can disagree, for a structural reason.**
  `average_throughput_rows_per_sec` averages each batch's own rows-per-second,
  weighting every batch equally; `overall` divides total rows by total time, so
  slow batches pull it down in proportion to the time they consumed. In Run A
  batch 0 alone took 5.46s for 10 rows, which the overall figure feels and the
  per-batch average dilutes (7.3 vs 5.82). The overall figure is the more honest
  summary of a run; the per-batch average is better for spotting outliers.
- **The first batch of any run is always the slowest** — Spark's JVM start-up,
  the first JDBC connection, and Spark's own query planning all land on it.
  Batch 0 of Run A took 5.46s; exclude it when reasoning about steady state.
- **On latency, whatever the query returns.** `AVG(ingested_at -
  event_timestamp)` spans *every* row in the table, so it includes any backlog:
  files that sat in `data/incoming/` before the job started, or accumulated
  while it was stopped, are still measured from their original event timestamp.
  That makes it a measure of end-to-end freshness across the whole table rather
  than steady-state pipeline lag, and it is why the earlier run reported minutes
  rather than seconds. For steady-state latency, restrict to recent rows:
  `WHERE ingested_at > NOW() - INTERVAL '5 minutes'`.
