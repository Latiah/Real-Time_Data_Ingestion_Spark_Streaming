#!/usr/bin/env bash
# Launches the Spark Structured Streaming job with the PostgreSQL JDBC driver.
# Usage: ./scripts/run_streaming.sh
set -euo pipefail
cd "$(dirname "$0")/.."

if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
fi

# Ensure Spark can import the local src package from the repo root.
export PYTHONPATH="$PWD"

: "${POSTGRES_JDBC_JAR_PATH:?Set POSTGRES_JDBC_JAR_PATH in .env (see docs/user_guide.md)}"

if [ ! -f "$POSTGRES_JDBC_JAR_PATH" ]; then
  echo "JDBC driver jar not found at $POSTGRES_JDBC_JAR_PATH"
  echo "See docs/user_guide.md for download instructions."
  exit 1
fi

spark-submit --jars "$POSTGRES_JDBC_JAR_PATH" src/streaming/streaming_pipeline.py
