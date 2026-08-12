# Testing

## Test Categories

| Category | Location | Requires | Notes |
|---|---|---|---|
| Unit — generator | `tests/test_data_generator.py` | Nothing | Pure Python |
| Unit — transformations | `tests/test_transformations.py` | Local PySpark (`local[1]`) | No real streaming query, no DB |
| Unit — SQL structure | `tests/test_database.py` (top section) | Nothing | Checks `sql/*.sql` contents |
| Integration — database | `tests/test_database.py` (bottom section) | Live PostgreSQL, schema applied | Auto-skipped if DB unreachable |
| Manual — end-to-end | See below | Generator + Spark + PostgreSQL all running | Not automated |

## Running Automated Tests

```bash
pytest -v
```

## Test Table

| # | Test | What It Verifies | Expected Result |
|---|---|---|---|
| 1 | `test_generate_event_has_all_required_fields` | Generated event has exactly the CSV schema fields | Pass |
| 2 | `test_generate_event_type_is_valid` | `event_type` is always `view` or `purchase` | Pass |
| 3 | `test_generate_event_ids_are_unique_within_a_batch` | No duplicate `event_id`s in one batch | Pass |
| 4 | `test_purchase_events_have_positive_quantity` | Purchases never have `quantity` 0 | Pass |
| 5 | `test_write_events_to_csv_produces_valid_csv` | Written CSV is well-formed, correct header | Pass |
| 6 | `test_write_events_to_csv_creates_unique_filenames` | Two writes → two distinct files | Pass |
| 7 | `test_cast_and_clean_types_converts_numeric_strings` | String columns become correct numeric/timestamp types | Pass |
| 8 | `test_cast_and_clean_types_null_on_bad_value` | Malformed numeric string casts to null, not a crash | Pass |
| 9 | `test_filter_invalid_records_drops_unknown_event_type` | Unknown `event_type` rows are dropped | Pass |
| 10 | `test_filter_invalid_records_drops_negative_price` | Negative price rows are dropped | Pass |
| 11 | `test_filter_invalid_records_drops_purchase_with_zero_quantity` | Purchase with `quantity=0` is dropped | Pass |
| 12 | `test_deduplicate_events_removes_duplicate_ids` | Duplicate `event_id` within a batch collapses to 1 row | Pass |
| 13 | `test_add_derived_fields_computes_total_amount_for_purchase` | `total_amount = quantity * price` for purchases | Pass |
| 14 | `test_transform_events_end_to_end` | Full pipeline: invalid dropped, dupes collapsed, fields added | Pass |
| 15 | `test_events_table_exists` (integration) | `events` table exists in the target DB | Pass/Skip |
| 16 | `test_duplicate_event_id_is_rejected` (integration) | DB rejects a second insert with the same `event_id` | Pass/Skip |

*(Actual results should be filled in after running the suite in your
environment — this table intentionally does not pre-fill "Actual Result"
since that depends on your local run.)*

## Manual End-to-End Test Plan

| Step | Action | Expected Outcome |
|---|---|---|
| 1 | Run `./scripts/generate_events.sh` | New CSV files appear in `data/incoming/` every `interval_seconds` |
| 2 | Run `./scripts/run_streaming.sh` | Log shows "Streaming query started" and periodic "Processing micro-batch" lines |
| 3 | Query `SELECT COUNT(*) FROM events;` | Count increases over time, roughly matching generated events minus intentionally-invalid ones |
| 4 | Stop and restart the streaming job | No duplicate rows appear (checkpoint + `PRIMARY KEY` both protect against this) |
| 5 | Manually drop a malformed CSV into `data/incoming/` (e.g. missing columns) | Job logs the issue via `DROPMALFORMED`/filtering, does not crash |

