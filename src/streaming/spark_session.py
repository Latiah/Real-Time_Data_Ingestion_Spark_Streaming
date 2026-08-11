"""
Spark session creation.

Kept in its own module so the streaming pipeline, tests, and any ad-hoc
scripts can all get a consistently configured SparkSession without
duplicating setup logic.
"""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession

from src.config.settings import get_spark_config

logger = logging.getLogger(__name__)


def get_spark_session() -> SparkSession:
    """
    Build (or fetch, if already created in this process) the SparkSession
    used for the streaming pipeline.

    shuffle partitions are deliberately kept low (see config.yaml) because
    this project runs on a single local machine with modest data volumes --
    the Spark default of 200 shuffle partitions would create excessive
    overhead for small micro-batches.
    """
    cfg = get_spark_config()

    spark = (
        SparkSession.builder
        .appName(cfg.app_name)
        .config("spark.sql.shuffle.partitions", cfg.shuffle_partitions)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session created: appName=%s", cfg.app_name)
    return spark
