"""
Unit tests for src/generator/data_generator.py.

These are pure-Python tests (no Spark, no PostgreSQL) and run fast.
"""

import csv

from src.generator.data_generator import (
    CSV_FIELDNAMES,
    generate_event,
    generate_events,
    write_events_to_csv,
)

NUM_USERS = 50
NUM_PRODUCTS = 20


def test_generate_event_has_all_required_fields():
    event = generate_event(NUM_USERS, NUM_PRODUCTS, purchase_probability=0.5)
    event_fields = set(event.__dict__.keys())
    assert event_fields == set(CSV_FIELDNAMES)


def test_generate_event_type_is_valid():
    for _ in range(50):
        event = generate_event(NUM_USERS, NUM_PRODUCTS, purchase_probability=0.5)
        assert event.event_type in ("view", "purchase")


def test_generate_event_ids_are_unique_within_a_batch():
    events = generate_events(200, NUM_USERS, NUM_PRODUCTS, purchase_probability=0.3)
    ids = [e.event_id for e in events]
    assert len(ids) == len(set(ids))


def test_purchase_events_have_positive_quantity():
    events = generate_events(200, NUM_USERS, NUM_PRODUCTS, purchase_probability=1.0)
    assert all(e.quantity >= 1 for e in events if e.event_type == "purchase")


def test_price_is_non_negative():
    events = generate_events(100, NUM_USERS, NUM_PRODUCTS, purchase_probability=0.5)
    assert all(e.price >= 0 for e in events)


def test_write_events_to_csv_produces_valid_csv(tmp_path):
    events = generate_events(10, NUM_USERS, NUM_PRODUCTS, purchase_probability=0.5)
    filepath = write_events_to_csv(events, tmp_path)

    assert filepath.exists()

    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        assert reader.fieldnames == CSV_FIELDNAMES

    assert len(rows) == 10


def test_write_events_to_csv_creates_unique_filenames(tmp_path):
    events = generate_events(5, NUM_USERS, NUM_PRODUCTS, purchase_probability=0.5)
    path1 = write_events_to_csv(events, tmp_path)
    path2 = write_events_to_csv(events, tmp_path)
    assert path1 != path2
