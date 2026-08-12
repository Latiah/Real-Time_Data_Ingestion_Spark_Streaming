import csv
import tempfile
import unittest
from pathlib import Path
import random

from src.data_generator import FIELDNAMES, generate_batch


class DataGeneratorTests(unittest.TestCase):
	def test_batch_has_expected_rows_and_columns(self):
		with tempfile.TemporaryDirectory() as directory:
			path = generate_batch(Path(directory), 1, 4, random.Random(7))
			with path.open(newline="", encoding="utf-8") as file:
				rows = list(csv.DictReader(file))

		self.assertEqual(len(rows), 4)
		self.assertEqual(tuple(rows[0]), FIELDNAMES)
		self.assertEqual(len({row["event_id"] for row in rows}), 4)

	def test_batch_is_renamed_after_write(self):
		with tempfile.TemporaryDirectory() as directory:
			path = generate_batch(Path(directory), 3, 1, random.Random(1))

		self.assertTrue(path.exists())
		self.assertFalse(path.with_name(".events_000003.csv.tmp").exists())


if __name__ == "__main__":
	unittest.main()