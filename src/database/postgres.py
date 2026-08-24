"""
PostgreSQL integration.

Two responsibilities:
1. write_batch_to_postgres(): called from Spark's foreachBatch for each
   streaming micro-batch. Uses Spark's JDBC writer (bulk, not row-by-row).
2. get_connection(): a plain psycopg (v3) connection, used by non-Spark code
   (setup scripts, validation queries, tests) that don't need a Spark
   DataFrame writer.

Why foreachBatch instead of a native streaming sink:
Spark Structured Streaming has no built-in JDBC streaming sink (JDBC
writes are inherently a batch operation -- a single INSERT statement
against a set of rows). foreachBatch is the documented, supported way to
apply a batch-style write (here, DataFrame.write.format("jdbc")) to each
micro-batch produced by a streaming query. It also gives access to the batch's
numeric id, which we use for logging/metrics.

Why each batch goes through a staging table:
Spark's JDBC writer supports only append and overwrite, with no way to skip
rows that already exist. A plain append therefore fails on any duplicate
event_id -- and duplicates are guaranteed whenever a batch is retried, because
a batch that fails partway has still committed some partitions while its
offsets remain uncommitted. Writing to events_staging and merging with
ON CONFLICT DO NOTHING keeps Spark's bulk writer while making the overall
write idempotent, so a replay converges instead of crash-looping. See
merge_staging_into_events().
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


def staging_table(table_name: str) -> str:
    """Name of the per-batch landing table for `table_name`."""
    return f"{table_name}_staging"


def merge_staging_into_events(cfg) -> int:
    """
    Move the staged micro-batch into the target table, ignoring duplicates.

    Spark's JDBC writer offers only append/overwrite -- there is no
    "insert, skipping conflicts" mode -- so a plain append raises a
    primary-key violation on any row that already exists. That matters because
    a batch which fails partway has still committed some of its partitions,
    and Spark, having never committed that batch's offsets, will replay all of
    it on restart. With a plain append the replay fails forever: a crash loop
    caused by the pipeline's own retry.

    Routing each batch through a staging table and merging with
    ON CONFLICT DO NOTHING makes the write idempotent instead, so replaying a
    batch is always safe and always converges.

    Returns:
        The number of rows actually inserted. Fewer than the batch size means
        the difference were duplicates already present -- expected on a replay,
        and worth noticing otherwise.
    """
    columns = ", ".join(f'"{column}"' for column in OUTPUT_COLUMNS)
    statement = (
        f'INSERT INTO "{cfg.table_name}" ({columns}) '
        f'SELECT {columns} FROM "{staging_table(cfg.table_name)}" '
        f"ON CONFLICT (event_id) DO NOTHING"
    )

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(statement)
            inserted = cur.rowcount
        conn.commit()

    return inserted


def get_connection(connect_timeout: int | None = None):
    """
    Open a plain psycopg (v3) connection using credentials from settings.

    Args:
        connect_timeout: seconds to wait before giving up. Left unset by
            default (libpq's own default applies). Callers that only want to
            probe reachability should pass a small value, so an unreachable
            host fails fast instead of blocking on the OS-level TCP timeout.
    """
    cfg = get_database_config()
    optional: dict[str, int] = {}
    if connect_timeout is not None:
        optional["connect_timeout"] = connect_timeout

    return psycopg.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.name,
        user=cfg.user,
        password=cfg.password,
        **optional,
    )


def write_batch_to_postgres(batch_df: DataFrame, batch_id: int) -> None:
    """
    Write one streaming micro-batch to PostgreSQL.

    Passed to `writeStream.foreachBatch(write_batch_to_postgres)`. Spark
    calls this once per micro-batch with a plain (non-streaming) DataFrame,
    which is what allows the regular batch JDBC writer to be used here.

    Raises:
        Exception: whatever the JDBC write raised. Deliberately propagated --
            see the comment on the except block below.
    """
    cfg = get_database_config()

    # count() and the write below are two separate Spark actions. Without
    # persisting, the entire batch would be recomputed for the second one --
    # re-reading the source CSV files and re-running dropDuplicates.
    batch_df.persist()
    try:
        # foreachBatch may be called with an empty batch (no new files since
        # the last trigger) -- skip the write entirely in that case.
        record_count = batch_df.count()
        if record_count == 0:
            logger.info("Batch %s: no records, skipping write.", batch_id)
            return

        logger.info("Processing micro-batch %s (%d records)...", batch_id, record_count)

        # The clock starts here, not before count(), so the recorded figure
        # covers the PostgreSQL write alone. Starting it earlier would fold a
        # full extra pass over the batch into every reported measurement.
        start = time.perf_counter()

        try:
            # Step 1: bulk-load the batch into the staging table using Spark's
            # JDBC writer. overwrite + truncate=true empties the table in place
            # rather than dropping and recreating it, so the schema that
            # create_tables.sql derived from `events` survives.
            (
                batch_df.select(*OUTPUT_COLUMNS)
                .write
                .format("jdbc")
                .option("url", cfg.jdbc_url)
                .option("dbtable", staging_table(cfg.table_name))
                .option("user", cfg.user)
                .option("password", cfg.password)
                .option("driver", cfg.jdbc_driver_class)
                .option("truncate", "true")
                .mode("overwrite")
                .save()
            )

            # Step 2: merge staging into the real table, skipping rows already
            # present. This is what makes the whole write idempotent -- see
            # merge_staging_into_events().
            inserted = merge_staging_into_events(cfg)

        except Exception:
            # Re-raised on purpose. foreachBatch signals success by returning
            # normally: Spark then commits this batch's offsets to the
            # checkpoint and will never re-read those files. Catching the
            # error and returning would therefore convert any transient
            # PostgreSQL problem into permanent, silent data loss.
            #
            # Failing loudly stops the streaming query with the batch still
            # uncommitted, so restarting it reprocesses the batch -- which is
            # safe precisely because the merge above is idempotent.
            logger.exception("Failed to write batch %s to PostgreSQL.", batch_id)
            raise

        elapsed = time.perf_counter() - start

        if inserted == record_count:
            logger.info(
                "Batch %s successfully written (%d records, %.3fs).",
                batch_id, record_count, elapsed,
            )
        else:
            # Normal on a replayed batch; unexpected otherwise, so it is worth
            # saying out loud rather than hiding inside the success message.
            logger.info(
                "Batch %s written (%d of %d records inserted, %d already present, %.3fs).",
                batch_id, inserted, record_count, record_count - inserted, elapsed,
            )

        try:
            record_batch_metrics(batch_id=batch_id, record_count=record_count, elapsed_seconds=elapsed)
        except Exception:
            # Metrics are observability, never correctness: the rows are already
            # committed in PostgreSQL by this point. Letting a metrics failure
            # escape would kill the streaming query *after* a successful write,
            # so the batch's offsets would never be committed and the restart
            # would replay rows that are already there -- a primary-key crash
            # loop caused by nothing more than an unwritable CSV. Observed in
            # practice as PermissionError on batch_metrics.csv.
            logger.warning(
                "Batch %s was written successfully, but recording its metrics failed. "
                "Continuing.", batch_id, exc_info=True,
            )

    finally:
        batch_df.unpersist()
