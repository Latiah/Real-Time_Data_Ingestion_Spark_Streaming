"""Generate small CSV batches for the Spark file stream."""

import argparse
import csv
import os
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path


PRODUCTS = (
	("P1001", "Wireless headphones", 79.99),
	("P1002", "Mechanical keyboard", 119.00),
	("P1003", "USB-C monitor", 249.50),
	("P1004", "Laptop stand", 42.00),
	("P1005", "Webcam", 64.95),
)
EVENT_TYPES = ("view", "purchase")
FIELDNAMES = (
	"event_id",
	"user_id",
	"event_type",
	"product_id",
	"product_name",
	"price",
	"event_timestamp",
)


def generate_batch(output_dir: Path, batch_number: int, event_count: int, rng: random.Random) -> Path:
	"""Write one complete batch atomically so Spark never reads a partial CSV."""
	output_dir.mkdir(parents=True, exist_ok=True)
	final_path = output_dir / f"events_{batch_number:06d}.csv"
	temporary_path = output_dir / f".events_{batch_number:06d}.csv.tmp"

	with temporary_path.open("w", newline="", encoding="utf-8") as file:
		writer = csv.DictWriter(file, fieldnames=FIELDNAMES)
		writer.writeheader()
		for _ in range(event_count):
			product_id, product_name, price = rng.choice(PRODUCTS)
			writer.writerow(
				{
					"event_id": str(uuid.uuid4()),
					"user_id": f"U{rng.randint(1, 1000):04d}",
					"event_type": rng.choice(EVENT_TYPES),
					"product_id": product_id,
					"product_name": product_name,
					"price": f"{price:.2f}",
					"event_timestamp": datetime.now(timezone.utc).isoformat(),
				}
			)

	os.replace(temporary_path, final_path)
	return final_path


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(description=__doc__)
	parser.add_argument("--output-dir", type=Path, default=Path("data/events"))
	parser.add_argument("--events-per-batch", type=int, default=25)
	parser.add_argument("--interval-seconds", type=float, default=2.0)
	parser.add_argument("--batches", type=int, default=0, help="0 runs continuously")
	parser.add_argument("--seed", type=int, default=None)
	return parser.parse_args()


def main() -> None:
	args = parse_args()
	if args.events_per_batch < 1 or args.interval_seconds < 0 or args.batches < 0:
		raise SystemExit("events-per-batch must be positive; interval and batches cannot be negative")

	rng = random.Random(args.seed)
	batch_number = 1
	while args.batches == 0 or batch_number <= args.batches:
		path = generate_batch(args.output_dir, batch_number, args.events_per_batch, rng)
		print(f"Generated {args.events_per_batch} events in {path}", flush=True)
		batch_number += 1
		if args.batches == 0 or batch_number <= args.batches:
			time.sleep(args.interval_seconds)


if __name__ == "__main__":
	main()
