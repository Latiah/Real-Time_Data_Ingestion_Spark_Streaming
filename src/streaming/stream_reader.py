"""
Streaming source definition.

Defines the explicit schema for incoming CSV events and wraps
spark.readStream so the rest of the pipeline doesn't need to know the
details of how files are discovered.

Why an explicit schema instead of schema inference:
Structured Streaming does not support schema inference for streaming
sources (Spark will raise an error if you try). Even if it did, inference
requires reading data upfront, which defeats the point of streaming, and
silently produces the wrong types (e.g. "quantity" as string) whenever a
file happens to have unusual values in its sample rows.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.config.settings import get_paths_config

logger = logging.getLogger(__name__)

# Matches CSV_FIELDNAMES in src/generator/data_generator.py.
# All fields are read as strings/loosely-typed here; strict validation and
# type coercion happens explicitly in transformations.py, where invalid
# values can be logged and handled rather than silently becoming null.
EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("user_id", StringType(), nullable=True),
        StructField("product_id", StringType(), nullable=True),
        StructField("event_type", StringType(), nullable=True),
        StructField("event_timestamp", StringType(), nullable=True),
        StructField("product_name", StringType(), nullable=True),
        StructField("quantity", StringType(), nullable=True),
        StructField("price", StringType(), nullable=True),
    ]
)


def read_event_stream(spark: SparkSession) -> DataFrame:
    """
    Create a streaming DataFrame that monitors the incoming CSV directory.

    Uses spark.readStream (genuine Structured Streaming), not a polling
    batch loop. Spark's own file-source machinery tracks which files have
    already been processed via the checkpoint directory, so this function
    stays purely declarative.
    """
    paths_cfg = get_paths_config()
    incoming_dir = str(paths_cfg.incoming_dir)

    logger.info("Configuring streaming source on directory: %s", incoming_dir)

    stream_df = (
        spark.readStream
        .format("csv")
        .option("header", "true")
        .option("mode", "DROPMALFORMED")  # rows that don't match the schema shape are dropped, not crash the job
        .schema(EVENT_SCHEMA)
        .load(incoming_dir)
    )

    return stream_df
