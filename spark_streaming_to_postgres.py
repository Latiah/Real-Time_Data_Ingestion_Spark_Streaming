"""
Spark Structured Streaming job: CSV events -> transform -> PostgreSQL.

This is the spark-submit entry point named in the project brief. The
implementation is split across src/streaming/ rather than living in one file,
so each stage can be unit-tested on its own:

    src/streaming/spark_session.py       SparkSession construction
    src/streaming/stream_reader.py       explicit schema + readStream source
    src/streaming/transformations.py     cast / validate / dedupe / derive
    src/streaming/streaming_pipeline.py  wires reader -> transform -> sink
    src/database/postgres.py             foreachBatch JDBC write

Run with:

    docker compose up --build

See docs/user_guide.md for the full run instructions.
"""

from __future__ import annotations

import logging

from src.streaming.streaming_pipeline import run_pipeline

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    run_pipeline()
