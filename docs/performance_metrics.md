# Performance Metrics

## Measurement method

Run the generator with a known batch size and interval while Spark is running. Capture the generator timestamp, Spark `batch_id` log, and PostgreSQL `ingested_at` timestamp. Query row counts and timestamps after the run.

## Metrics

| Metric | Definition | Target for local development | Observed result |
|---|---|---:|---|
| End-to-end latency | Time from CSV creation to PostgreSQL `ingested_at` | Less than 10 seconds | Record during execution |
| Throughput | Rows written divided by elapsed seconds | At least 10 rows/second | Record during execution |
| Completeness | PostgreSQL rows divided by generated valid rows | 100% | Record during execution |
| Duplicate rate | Duplicate `event_id` rows divided by total rows | 0% | Record during execution |
| Error rate | Failed rows or failed batches divided by attempted rows/batches | 0% | Record during execution |

Example measurement query:

```sql
SELECT COUNT(*) AS rows_written,
	   MIN(event_timestamp) AS first_event,
	   MAX(ingested_at) AS last_ingestion,
	   AVG(EXTRACT(EPOCH FROM (ingested_at - event_timestamp))) AS avg_ingestion_lag_seconds
FROM public.user_events
WHERE ingested_at >= CURRENT_TIMESTAMP - INTERVAL '15 minutes';
```

These are local-development targets, not production capacity claims. Results depend on CPU, memory, disk, PostgreSQL settings, Spark trigger interval, and JDBC batch size. For a production benchmark, repeat each workload at least three times and report median and p95 latency.
