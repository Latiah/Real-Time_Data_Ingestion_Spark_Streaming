"""
Transformation and validation logic for streaming events.

Kept separate from the streaming pipeline wiring so it can be unit tested
with plain (batch) DataFrames -- these functions have no dependency on
streaming-specific APIs, which is what makes them testable without
spinning up a real streaming query.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

logger = logging.getLogger(__name__)

VALID_EVENT_TYPES = ("view", "purchase")


def cast_and_clean_types(df: DataFrame) -> DataFrame:
    """
    Convert the raw string columns from CSV into their proper types.

    Uses try_cast semantics (cast() returns null on failure rather than
    throwing) so a single malformed row doesn't crash the whole batch --
    it becomes a null that later validation steps can filter out.
    """
    return (
        df
        .withColumn("user_id", F.col("user_id").cast(IntegerType()))
        .withColumn("product_id", F.col("product_id").cast(IntegerType()))
        .withColumn("quantity", F.col("quantity").cast(IntegerType()))
        .withColumn("price", F.col("price").cast(DoubleType()))
        .withColumn("event_timestamp", F.to_timestamp(F.col("event_timestamp")))
    )


def filter_invalid_records(df: DataFrame) -> DataFrame:
    """
    Drop records that are structurally invalid:
      - missing event_id (can't be deduplicated or tracked without it)
      - failed type casts (user_id/product_id/price/event_timestamp null)
      - unrecognized event_type
      - negative price or negative quantity
      - purchase events with quantity <= 0 (a purchase must involve >=1 item)

    Each dropped record represents bad input data (not a bug), so this is
    filtering, not error handling; logging the count is enough for now
    since the acceptable columns are also written back out for debugging.
    """
    valid = (
        df
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("product_id").isNotNull())
        .filter(F.col("event_timestamp").isNotNull())
        .filter(F.col("event_type").isin(list(VALID_EVENT_TYPES)))
        .filter((F.col("price").isNotNull()) & (F.col("price") >= 0))
        .filter(F.col("quantity").isNotNull() & (F.col("quantity") >= 0))
        .filter(
            (F.col("event_type") != "purchase") | (F.col("quantity") > 0)
        )
    )
    return valid


def deduplicate_events(df: DataFrame) -> DataFrame:
    """
    Drop duplicate event_ids within a micro-batch.

    Note: this only deduplicates *within* the current micro-batch (a
    streaming DataFrame transformation can't look across batches without
    stateful aggregation, which would add complexity this project doesn't
    need). Cross-batch duplicate protection is instead handled at the
    PostgreSQL layer via a UNIQUE constraint on event_id (see
    sql/create_tables.sql and src/database/postgres.py).
    """
    return df.dropDuplicates(["event_id"])


def add_derived_fields(df: DataFrame) -> DataFrame:
    """
    Add derived/computed columns.

    total_amount = quantity * price, meaningful only for purchase events;
    views get 0.0 rather than null so downstream SQL aggregations
    (SUM(total_amount)) don't need extra null-handling.
    """
    return df.withColumn(
        "total_amount",
        F.when(F.col("event_type") == "purchase", F.col("quantity") * F.col("price")).otherwise(F.lit(0.0)),
    )


def add_ingestion_metadata(df: DataFrame) -> DataFrame:
    """Attach the time this record was processed by the pipeline."""
    return df.withColumn("ingested_at", F.current_timestamp())


def transform_events(df: DataFrame) -> DataFrame:
    """
    Full transformation pipeline applied to each micro-batch:
    cast types -> filter invalid -> deduplicate -> derive fields -> add metadata.
    """
    df = cast_and_clean_types(df)
    df = filter_invalid_records(df)
    df = deduplicate_events(df)
    df = add_derived_fields(df)
    df = add_ingestion_metadata(df)
    return df
