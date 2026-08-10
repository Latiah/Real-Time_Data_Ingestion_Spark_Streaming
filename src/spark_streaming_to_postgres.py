"""Clean CSV event files with Spark Structured Streaming and write to PostgreSQL."""

import argparse
import os
import time
from datetime import datetime, timezone
from typing import Any

import psycopg
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, input_file_name, lit, trim, to_timestamp
from pyspark.sql.types import StringType, StructField, StructType


EVENT_SCHEMA = StructType(
	[
		StructField("event_id", StringType(), False),
		StructField("user_id", StringType(), False),
		StructField("event_type", StringType(), False),
		StructField("product_id", StringType(), False),
		StructField("product_name", StringType(), False),
		StructField("price", StringType(), True),
		StructField("event_timestamp", StringType(), True),
	]
)


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(description=__doc__)
	parser.add_argument("--input-dir", default="data/events")
	parser.add_argument("--checkpoint-dir", default="data/checkpoint")
	parser.add_argument("--postgres-host", default=os.getenv("POSTGRES_HOST", "localhost"))
	parser.add_argument("--postgres-port", default=os.getenv("POSTGRES_PORT", "5432"))
	parser.add_argument("--postgres-database", default=os.getenv("POSTGRES_DATABASE", "events_db"))
	parser.add_argument("--postgres-user", default=os.getenv("POSTGRES_USER", "postgres"))
	parser.add_argument("--postgres-password", default=os.getenv("POSTGRES_PASSWORD"))
	parser.add_argument("--postgres-table", default=os.getenv("POSTGRES_TABLE", "public.user_events"))
	parser.add_argument("--postgres-staging-table", default=os.getenv("POSTGRES_STAGING_TABLE", "public.user_events_staging"))
	parser.add_argument("--postgres-batch-log-table", default=os.getenv("POSTGRES_BATCH_LOG_TABLE", "public.streaming_batch_log"))
	parser.add_argument("--trigger-seconds", type=int, default=2)
	parser.add_argument("--max-files-per-trigger", type=int, default=1)
	args = parser.parse_args()
	if args.trigger_seconds < 1:
		parser.error("--trigger-seconds must be at least 1")
	if args.max_files_per_trigger < 1:
		parser.error("--max-files-per-trigger must be at least 1")
	return args


def clean_events(events: DataFrame) -> DataFrame:
	"""Trim text, retain valid business values, and add the ingestion time."""
	cleaned = events.select(
		trim(col("event_id")).alias("event_id"),
		trim(col("user_id")).alias("user_id"),
		trim(col("event_type")).alias("event_type"),
		trim(col("product_id")).alias("product_id"),
		trim(col("product_name")).alias("product_name"),
		col("price").cast("double").alias("price"),
		to_timestamp(col("event_timestamp")).alias("event_timestamp"),
		trim(col("source_file")).alias("source_file"),
	)
	return (
		cleaned.filter(
			col("event_id").isNotNull()
			& col("user_id").isNotNull()
			& col("event_type").isin("view", "purchase")
			& col("product_id").isNotNull()
			& col("price").isNotNull()
			& (col("price") >= 0)
			& col("event_timestamp").isNotNull()
		)
		.dropDuplicates(["event_id"])
		.withColumn("ingested_at", current_timestamp())
	)



def write_batch(batch: DataFrame, batch_id: int, jdbc_url: str, table: str, staging_table: str, batch_log_table: str, properties: dict[str, Any], postgres_dsn: str) -> None:
	"""Stage and commit a micro-batch, then record its processing metrics."""
	batch_started_at = time.time()
	rows = batch.count()
	committed_rows = 0
	if rows:
		(batch.withColumn("batch_id", lit(batch_id))
			.write.jdbc(url=jdbc_url, table=staging_table, mode="append", properties=properties))
	with psycopg.connect(postgres_dsn) as connection:
		with connection.cursor() as cursor:
			if rows:
				cursor.execute(
					f"""INSERT INTO {table}
					(event_id, user_id, event_type, product_id, product_name, price, event_timestamp, ingested_at, source_file)
					SELECT event_id, user_id, event_type, product_id, product_name, price, event_timestamp, ingested_at, source_file
					FROM {staging_table} WHERE batch_id = %s
					ON CONFLICT (event_id) DO NOTHING""",
					(batch_id,),
				)
				committed_rows = cursor.rowcount
				cursor.execute(f"DELETE FROM {staging_table} WHERE batch_id = %s", (batch_id,))
			batch_completed_at = time.time()
			cursor.execute(
				f"""INSERT INTO {batch_log_table}
				(spark_batch_id, rows_received, rows_written, batch_started_at, batch_completed_at)
				VALUES (%s, %s, %s, %s, %s)
				ON CONFLICT (spark_batch_id) DO UPDATE SET
				rows_received = EXCLUDED.rows_received,
				rows_written = EXCLUDED.rows_written,
				batch_started_at = EXCLUDED.batch_started_at,
				batch_completed_at = EXCLUDED.batch_completed_at""",
				(batch_id, rows, committed_rows, datetime.fromtimestamp(batch_started_at, timezone.utc), datetime.fromtimestamp(batch_completed_at, timezone.utc)),
			)
	print(f"batch_id={batch_id} rows_received={rows} rows_written={committed_rows} elapsed_seconds={batch_completed_at - batch_started_at:.2f}", flush=True)


def main() -> None:
	args = parse_args()
	if not args.postgres_password:
		raise SystemExit("Set POSTGRES_PASSWORD or pass --postgres-password")

	spark = (
		SparkSession.builder.appName("RealTimeEventsToPostgres")
		.config("spark.sql.session.timeZone", "UTC")
		.getOrCreate()
	)
	spark.sparkContext.setLogLevel("WARN")
	jdbc_url = f"jdbc:postgresql://{args.postgres_host}:{args.postgres_port}/{args.postgres_database}"
	properties = {"user": args.postgres_user, "password": args.postgres_password, "driver": "org.postgresql.Driver"}
	postgres_dsn = f"host={args.postgres_host} port={args.postgres_port} dbname={args.postgres_database} user={args.postgres_user} password={args.postgres_password}"

	stream = (
		spark.readStream.schema(EVENT_SCHEMA)
		.option("header", "true")
		.option("maxFilesPerTrigger", args.max_files_per_trigger)
		.csv(args.input_dir)
		.withColumn("source_file", input_file_name())
	)
	query = (
		clean_events(stream)
		.writeStream.outputMode("append")
		.option("checkpointLocation", args.checkpoint_dir)
		.trigger(processingTime=f"{args.trigger_seconds} seconds")
		.foreachBatch(lambda batch, batch_id: write_batch(batch, batch_id, jdbc_url, args.postgres_table, args.postgres_staging_table, args.postgres_batch_log_table, properties, postgres_dsn))
		.start()
	)
	query.awaitTermination()


if __name__ == "__main__":
	main()
