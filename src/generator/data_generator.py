"""
Synthetic e-commerce event generator.

Generates realistic "view" and "purchase" events and writes them as CSV
files into the streaming input directory (config.paths.incoming_dir).
Each call to run_generator() produces one CSV file per interval, which is
what Spark Structured Streaming will pick up as a new streaming source file.

Design notes:
- Logic is split into small, testable functions rather than one big main().
- Randomness is intentionally simple (uniform/random.choice) since the goal
  is a plausible event stream, not a statistically rigorous simulation.
"""

from __future__ import annotations

import argparse
import csv
import logging
import random
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from src.config.settings import get_generator_config, get_paths_config

logger = logging.getLogger(__name__)

EventType = Literal["view", "purchase"]

CSV_FIELDNAMES = [
    "event_id",
    "user_id",
    "product_id",
    "event_type",
    "event_timestamp",
    "product_name",
    "quantity",
    "price",
]

_PRODUCT_NAMES = [
    "Wireless Mouse", "Mechanical Keyboard", "USB-C Hub", "Laptop Stand",
    "Noise Cancelling Headphones", "Webcam", "Monitor Arm", "Desk Lamp",
    "Bluetooth Speaker", "External SSD", "Phone Case", "Charging Cable",
    "Backpack", "Water Bottle", "Notebook", "Office Chair", "Standing Desk",
    "Coffee Mug", "Wall Charger", "Power Bank",
]


@dataclass
class Event:
    event_id: str
    user_id: int
    product_id: int
    event_type: EventType
    event_timestamp: str
    product_name: str
    quantity: int
    price: float


def generate_event(num_users: int, num_products: int, purchase_probability: float) -> Event:
    """Generate a single synthetic event with realistic field values."""
    event_type: EventType = "purchase" if random.random() < purchase_probability else "view"
    product_id = random.randint(1, num_products)

    return Event(
        event_id=str(uuid.uuid4()),
        user_id=random.randint(1, num_users),
        product_id=product_id,
        event_type=event_type,
        event_timestamp=datetime.now(timezone.utc).isoformat(),
        product_name=_PRODUCT_NAMES[product_id % len(_PRODUCT_NAMES)],
        # views don't really have a "quantity" being purchased, but we keep
        # the field present (as 0) so the CSV schema stays uniform.
        quantity=random.randint(1, 5) if event_type == "purchase" else 0,
        price=round(random.uniform(5.0, 500.0), 2),
    )


def generate_events(count: int, num_users: int, num_products: int, purchase_probability: float) -> list[Event]:
    """Generate a batch of `count` events."""
    return [generate_event(num_users, num_products, purchase_probability) for _ in range(count)]


def write_events_to_csv(events: list[Event], output_dir: Path) -> Path:
    """
    Write a list of events to a new, uniquely-named CSV file in output_dir.

    A unique filename per batch is important: Spark's file-based streaming
    source detects new files by name, so each write must produce a distinct
    file rather than appending to an existing one.
    """
    filename = f"events_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')}.csv"
    filepath = output_dir / filename

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        for event in events:
            writer.writerow(asdict(event))

    return filepath


def run_generator(continuous: bool = True, max_batches: int | None = None) -> None:
    """
    Run the generator loop.

    Args:
        continuous: if True, keep generating until interrupted (or max_batches
            is reached). If False, generate exactly one batch and return.
        max_batches: optional cap on the number of batches to write. Useful
            for tests and demos so the process terminates on its own.
    """
    gen_cfg = get_generator_config()
    paths_cfg = get_paths_config()

    logger.info("Starting event generator...")
    logger.info(
        "Config: batch_size=%s interval=%ss users=%s products=%s purchase_prob=%s",
        gen_cfg.batch_size, gen_cfg.interval_seconds, gen_cfg.num_users,
        gen_cfg.num_products, gen_cfg.purchase_probability,
    )

    batches_written = 0
    while True:
        events = generate_events(
            count=gen_cfg.batch_size,
            num_users=gen_cfg.num_users,
            num_products=gen_cfg.num_products,
            purchase_probability=gen_cfg.purchase_probability,
        )
        filepath = write_events_to_csv(events, paths_cfg.incoming_dir)
        batches_written += 1
        logger.info("Generated %d events -> %s", len(events), filepath.name)

        if not continuous:
            break
        if max_batches is not None and batches_written >= max_batches:
            logger.info("Reached max_batches=%s, stopping generator.", max_batches)
            break

        time.sleep(gen_cfg.interval_seconds)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    run_generator(continuous=True)
