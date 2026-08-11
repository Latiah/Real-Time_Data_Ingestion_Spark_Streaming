"""
Entry point for the Spark Structured Streaming job.

Wires together:
    stream_reader.read_event_stream()   -- readStream over the CSV directory
    transformations.transform_events()  -- cleaning/validation/derived fields
    database.postgres.write_batch_to_postgres() -- foreachBatch sink

Run with:
    spark-submit --jars <path-to-postgres-jdbc.jar> \
        src/streaming/streaming_pipeline.py

See docs/user_guide.md for the exact command, including how the JDBC jar
is obtained and referenced.
"""

from __future__ import annotations

import logging

from src.config.settings import get_paths_config, get_spark_config
from src.database.postgres import write_batch_to_postgres
from src.streaming.spark_session import get_spark_session
from src.streaming.stream_reader import read_event_stream
from src.streaming.transformations import transform_events

logger = logging.getLogger(__name__)


def run_pipeline() -> None:
    spark_cfg = get_spark_config()
    paths_cfg = get_paths_config()

    spark = get_spark_session()

    logger.info("Starting Spark streaming pipeline...")
    raw_stream = read_event_stream(spark)
    transformed_stream = transform_events(raw_stream)

    query = (
        transformed_stream.writeStream
        .foreachBatch(write_batch_to_postgres)
        .option("checkpointLocation", str(paths_cfg.checkpoint_dir))
        .trigger(processingTime=spark_cfg.trigger_interval)
        .start()
    )

    logger.info(
        "Streaming query started. Watching %s every %s. Checkpoint: %s",
        paths_cfg.incoming_dir, spark_cfg.trigger_interval, paths_cfg.checkpoint_dir,
    )

    query.awaitTermination()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    run_pipeline()
