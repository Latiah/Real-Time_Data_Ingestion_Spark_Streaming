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
import os
import random
import re
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
    "Wireless Mouse", "Keyboard", "USB-C Hub", "Laptop Stand",
    "Headphones", "Webcam", "Monitor Arm", "Desk Lamp",
    "Bluetooth Speaker", "External SSD", "Phone Case", "Charging Cable",
    "Bag", "Water Bottle", "Notebook", "Office Chair", "Standing Desk",
    "Coffee Mug", "Charger", "Power Bank",
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


FILENAME_PATTERN = re.compile(r"^events_(\d+)\.csv$")


def next_sequence_number(output_dir: Path) -> int:
    """
    Return the next unused batch number for output_dir: 1 for an empty
    directory, otherwise one past the highest number already present.

    The number is derived from the files on disk rather than an in-memory
    counter, because a restarted generator must not reuse a number. Spark's
    file-based streaming source tracks which files it has consumed *by name*,
    so overwriting an existing events_N.csv would leave the new contents
    silently unprocessed.

    Files that don't match the events_N.csv pattern are ignored, so a
    directory holding output from the older timestamp-based scheme is still
    handled correctly.
    """
    highest = 0
    for existing in output_dir.glob("events_*.csv"):
        match = FILENAME_PATTERN.match(existing.name)
        if match:
            highest = max(highest, int(match.group(1)))
    return highest + 1


def write_events_to_csv(
    events: list[Event],
    output_dir: Path,
    sequence: int | None = None,
) -> Path:
    """
    Write a list of events to a new CSV file named events_<n>.csv, numbering
    batches sequentially from 1.

    A distinct filename per batch is important: Spark's file-based streaming
    source detects new files by name, so each write must produce a new file
    rather than appending to an existing one.

    The file is written under a temporary name and then renamed into place.
    Spark polls the directory, so a batch written directly to its final name
    can be picked up while only partially flushed -- the trailing row would be
    truncated mid-line and silently dropped by the reader's DROPMALFORMED mode.
    The temporary name is dot-prefixed because Spark's file source ignores
    entries starting with "." or "_", so the batch stays invisible until it is
    complete, and the rename publishes it atomically.

    Args:
        events: the batch to write.
        output_dir: directory to write into (Spark's streaming input dir).
        sequence: batch number to use. Defaults to the next unused number for
            output_dir; pass an explicit value to control numbering.

    Raises:
        FileExistsError: if the target file already exists. This is a guard
            against silently overwriting a batch Spark has already consumed.
    """
    if sequence is None:
        sequence = next_sequence_number(output_dir)

    filepath = output_dir / f"events_{sequence}.csv"

    # Checked up front rather than relying on open(..., "x"), because the
    # write now goes to the temporary name: an existing final file means the
    # sequence number was wrong, and overwriting it would lose data Spark
    # cannot re-detect (it tracks consumed files by name).
    if filepath.exists():
        raise FileExistsError(f"Refusing to overwrite existing batch file: {filepath}")

    # "w", not "x": a leftover temp file from a previously killed run is
    # incomplete by definition and safe to discard.
    tmp_path = output_dir / f".{filepath.name}.tmp"
    with open(tmp_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        for event in events:
            writer.writerow(asdict(event))
        # Flush Python's buffer and the OS write-back cache before the rename,
        # so the published file cannot be visible-but-empty after a crash.
        f.flush()
        os.fsync(f.fileno())

    # os.replace rather than Path.rename: atomic on POSIX and Windows alike.
    os.replace(tmp_path, filepath)

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

    # Scanned once, then incremented in memory: the directory only grows while
    # this loop is the sole writer, so re-scanning every batch would be an
    # increasingly expensive way to compute a number we already know.
    sequence = next_sequence_number(paths_cfg.incoming_dir)
    if sequence > 1:
        logger.info("Resuming batch numbering at %d (existing files found).", sequence)

    batches_written = 0
    while True:
        events = generate_events(
            count=gen_cfg.batch_size,
            num_users=gen_cfg.num_users,
            num_products=gen_cfg.num_products,
            purchase_probability=gen_cfg.purchase_probability,
        )
        filepath = write_events_to_csv(events, paths_cfg.incoming_dir, sequence=sequence)
        sequence += 1
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
