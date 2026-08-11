"""
PostgreSQL integration.

Two responsibilities:
1. write_batch_to_postgres(): called from Spark's foreachBatch for each
   streaming micro-batch. Uses Spark's JDBC writer (bulk, not row-by-row).
2. get_connection(): a plain psycopg2 connection, used by non-Spark code
   (setup scripts, validation queries, tests) that don't need a Spark
   DataFrame writer.

Why foreachBatch instead of a native streaming sink:
Spark Structured Streaming has no built-in JDBC streaming sink (JDBC
writes are inherently a batch operation -- a single INSERT statement
against a set of rows). foreachBatch is the documented, supported way to
apply a batch-style write (here, DataFrame.write.jdbc) to each micro-batch
produced by a streaming query. It also gives access to the batch's numeric
id, which we use for logging/metrics.
"""

from __future__ import annotations

import logging
import time

import psycopg
from pyspark.sql import DataFrame

from src.config.settings import get_database_config
from src.monitoring.metrics import record_batch_metrics

logger = logging.getLogger(__name__)

OUTPUT_COLUMNS = [
    "event_id", "user_id", "product_id", "event_type", "event_timestamp",
    "product_name", "quantity", "price", "total_amount", "ingested_at",
]


def get_connection():
    """Open a plain psycopg2 connection using credentials from settings."""
    cfg = get_database_config()
    return psycopg.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.name,
        user=cfg.user,
        password=cfg.password,
    )


def write_batch_to_postgres(batch_df: DataFrame, batch_id: int) -> None:
    """
    Write one streaming micro-batch to PostgreSQL.

    Passed to `writeStream.foreachBatch(write_batch_to_postgres)`. Spark
    calls this once per micro-batch with a plain (non-streaming) DataFrame,
    which is what allows the regular batch JDBC writer to be used here.
    """
    cfg = get_database_config()
    start = time.perf_counter()

    # foreachBatch may be called with an empty batch (no new files since
    # the last trigger) -- skip the write entirely in that case.
    record_count = batch_df.count()
    if record_count == 0:
        logger.info("Batch %s: no records, skipping write.", batch_id)
        return

    logger.info("Processing micro-batch %s (%d records)...", batch_id, record_count)

    try:
        (
            batch_df.select(*OUTPUT_COLUMNS)
            .write
            .format("jdbc")
            .option("url", cfg.jdbc_url)
            .option("dbtable", cfg.table_name)
            .option("user", cfg.user)
            .option("password", cfg.password)
            .option("driver", cfg.jdbc_driver_class)
            # append: PostgreSQL's UNIQUE constraint on event_id is the
            # real duplicate-prevention mechanism (see create_tables.sql).
            # A plain append here keeps foreachBatch simple; if PostgreSQL
            # rejects a duplicate primary key, that single failed batch is
            # logged and retried on the next trigger without crashing the
            # whole streaming query (see except block below for details).
            .mode("append")
            .save()
        )
        elapsed = time.perf_counter() - start
        logger.info("Batch %s successfully written (%d records, %.3fs).", batch_id, record_count, elapsed)
        record_batch_metrics(batch_id=batch_id, record_count=record_count, elapsed_seconds=elapsed)

    except Exception as exc:
        # A broad except here is intentional and documented: foreachBatch
        # runs inside the Spark streaming query thread, and letting an
        # unexpected exception escape (e.g. PostgreSQL momentarily
        # unavailable, or a UNIQUE constraint violation from a retried
        # batch) would kill the entire streaming job. We log with full
        # detail and let the query continue; the next trigger will pick up
        # any files that still need processing.
        logger.error("Failed to write batch %s to PostgreSQL: %s", batch_id, exc, exc_info=True)
