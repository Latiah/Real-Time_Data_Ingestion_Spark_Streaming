"""Unit tests for the synthetic event generator.

No database or Spark session is needed: every function under test either
returns plain objects or writes to a caller-supplied directory.
"""

import csv
import tempfile
import unittest
from pathlib import Path

from src.generator.data_generator import (
    CSV_FIELDNAMES,
    generate_event,
    generate_events,
    next_sequence_number,
    write_events_to_csv,
)


def _batch(count: int = 4):
    return generate_events(count, num_users=10, num_products=10, purchase_probability=0.3)


class GenerateEventTests(unittest.TestCase):
    def test_event_fields_are_within_configured_bounds(self):
        for _ in range(50):
            event = generate_event(num_users=10, num_products=10, purchase_probability=0.3)
            self.assertIn(event.event_type, ("view", "purchase"))
            self.assertTrue(1 <= event.user_id <= 10)
            self.assertTrue(1 <= event.product_id <= 10)
            self.assertGreaterEqual(event.price, 0)

    def test_views_have_zero_quantity_and_purchases_do_not(self):
        # purchase_probability at the extremes makes the event type deterministic.
        self.assertEqual(generate_event(10, 10, 0.0).quantity, 0)
        self.assertGreaterEqual(generate_event(10, 10, 1.0).quantity, 1)

    def test_batch_event_ids_are_unique(self):
        events = _batch(20)
        self.assertEqual(len({event.event_id for event in events}), 20)


class WriteEventsToCsvTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.out = Path(self._tmp.name)
        self.addCleanup(self._tmp.cleanup)

    def test_file_has_expected_rows_and_columns(self):
        path = write_events_to_csv(_batch(4), self.out)

        with path.open(newline="", encoding="utf-8") as file:
            rows = list(csv.DictReader(file))

        self.assertEqual(len(rows), 4)
        self.assertEqual(list(rows[0]), CSV_FIELDNAMES)
        self.assertEqual(len({row["event_id"] for row in rows}), 4)

    def test_batches_are_numbered_sequentially_from_one(self):
        names = [write_events_to_csv(_batch(1), self.out).name for _ in range(3)]
        self.assertEqual(names, ["events_1.csv", "events_2.csv", "events_3.csv"])

    def test_numbering_resumes_from_existing_files(self):
        # A restarted generator must not reuse a number: Spark tracks consumed
        # files by name, so a reused name would go unnoticed.
        for _ in range(3):
            write_events_to_csv(_batch(1), self.out)

        self.assertEqual(next_sequence_number(self.out), 4)
        self.assertEqual(write_events_to_csv(_batch(1), self.out).name, "events_4.csv")

    def test_explicit_sequence_is_honoured(self):
        path = write_events_to_csv(_batch(1), self.out, sequence=42)
        self.assertEqual(path.name, "events_42.csv")
        self.assertEqual(next_sequence_number(self.out), 43)

    def test_existing_file_is_not_overwritten(self):
        write_events_to_csv(_batch(1), self.out, sequence=1)

        with self.assertRaises(FileExistsError):
            write_events_to_csv(_batch(1), self.out, sequence=1)

    def test_temp_file_is_renamed_away_after_write(self):
        # The batch is written under a dot-prefixed temp name (which Spark's
        # file source ignores) and only then renamed into place, so Spark never
        # reads a partially flushed file.
        path = write_events_to_csv(_batch(3), self.out)

        self.assertTrue(path.exists())
        self.assertFalse((self.out / f".{path.name}.tmp").exists())
        self.assertEqual([p.name for p in self.out.iterdir()], ["events_1.csv"])


class NextSequenceNumberTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.out = Path(self._tmp.name)
        self.addCleanup(self._tmp.cleanup)

    def test_empty_directory_starts_at_one(self):
        self.assertEqual(next_sequence_number(self.out), 1)

    def test_non_matching_filenames_are_ignored(self):
        (self.out / "events_notanumber.csv").write_text("x", encoding="utf-8")
        (self.out / "other.csv").write_text("x", encoding="utf-8")
        # Output from the previous timestamp-based naming scheme.
        (self.out / "events_20260819T120000000000.csv").write_text("x", encoding="utf-8")

        self.assertEqual(next_sequence_number(self.out), 1)

    def test_highest_number_wins_regardless_of_lexical_order(self):
        # "events_10.csv" sorts before "events_9.csv" as a string, so a
        # name-sorted implementation would answer 10 here instead of 11.
        for sequence in (1, 9, 10):
            write_events_to_csv(_batch(1), self.out, sequence=sequence)

        self.assertEqual(next_sequence_number(self.out), 11)


if __name__ == "__main__":
    unittest.main()
