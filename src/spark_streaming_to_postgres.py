"""Clean CSV event files with Spark Structured Streaming and write to PostgreSQL."""

import argparse
import os
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, trim, to_timestamp
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType


EVENT_SCHEMA = StructType(
	[
		StructField("event_id", StringType(), False),
		StructField("user_id", StringType(), False),
		StructField("event_type", StringType(), False),
		StructField("product_id", StringType(), False),
		StructField("product_name", StringType(), False),
		StructField("price", DoubleType(), False),
		StructField("event_timestamp", TimestampType(), False),
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
		col("price").cast(DoubleType()).alias("price"),
		to_timestamp(col("event_timestamp")).alias("event_timestamp"),
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


def write_batch(batch: DataFrame, batch_id: int, jdbc_url: str, table: str, properties: dict[str, Any]) -> None:
	"""Append a micro-batch and log the committed row count."""
	rows = batch.count()
	if not rows:
		print(f"batch_id={batch_id} rows_written=0", flush=True)
		return

	batch.write.jdbc(url=jdbc_url, table=table, mode="append", properties=properties)
	print(f"batch_id={batch_id} rows_written={rows}", flush=True)


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

	stream = (
		spark.readStream.schema(EVENT_SCHEMA)
		.option("header", "true")
		.option("maxFilesPerTrigger", args.max_files_per_trigger)
		.csv(args.input_dir)
	)
	query = (
		clean_events(stream)
		.writeStream.outputMode("append")
		.option("checkpointLocation", args.checkpoint_dir)
		.trigger(processingTime=f"{args.trigger_seconds} seconds")
		.foreachBatch(lambda batch, batch_id: write_batch(batch, batch_id, jdbc_url, args.postgres_table, properties))
		.start()
	)
	query.awaitTermination()


if __name__ == "__main__":
	main()
