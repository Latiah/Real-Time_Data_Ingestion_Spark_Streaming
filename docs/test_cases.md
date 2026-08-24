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

Under Docker:

```powershell
docker compose run --rm tests
```

## Test Table

27 automated tests. "Actual Result" records what was observed on the last run
in this environment — see the note below the table before reading it.

### Unit — generator (`tests/test_data_generator.py`)

| # | Test | What It Verifies | Expected Result | Actual Result |
|---|---|---|---|---|
| 1 | `test_event_fields_are_within_configured_bounds` | `event_type`, `user_id`, `product_id`, `price` stay within configured ranges | Pass | Pass |
| 2 | `test_views_have_zero_quantity_and_purchases_do_not` | Views have `quantity` 0; purchases have ≥ 1 | Pass | Pass |
| 3 | `test_batch_event_ids_are_unique` | No duplicate `event_id`s in one batch | Pass | Pass |
| 4 | `test_file_has_expected_rows_and_columns` | Written CSV is well-formed, correct header and row count | Pass | Pass |
| 5 | `test_batches_are_numbered_sequentially_from_one` | Three writes produce `events_1.csv`, `events_2.csv`, `events_3.csv` | Pass | Pass |
| 6 | `test_numbering_resumes_from_existing_files` | A restarted generator continues numbering instead of reusing a name Spark already consumed | Pass | Pass |
| 7 | `test_explicit_sequence_is_honoured` | An explicit `sequence` argument sets the filename | Pass | Pass |
| 8 | `test_existing_file_is_not_overwritten` | A colliding batch number raises `FileExistsError` rather than clobbering data | Pass | Pass |
| 9 | `test_temp_file_is_renamed_away_after_write` | Batch is published by atomic rename; no `.tmp` file left behind | Pass | Pass |
| 10 | `test_empty_directory_starts_at_one` | Numbering starts at 1 in a fresh directory | Pass | Pass |
| 11 | `test_non_matching_filenames_are_ignored` | Unrelated files (including old timestamp-named output) don't affect numbering | Pass | Pass |
| 12 | `test_highest_number_wins_regardless_of_lexical_order` | Scan compares numerically, so `events_10.csv` beats `events_9.csv` | Pass | Pass |

### Unit — transformations (`tests/test_transformations.py`)

| # | Test | What It Verifies | Expected Result | Actual Result |
|---|---|---|---|---|
| 13 | `test_cast_and_clean_types_converts_numeric_strings` | String columns become correct numeric/timestamp types | Pass | Pass |
| 14 | `test_cast_and_clean_types_null_on_bad_value` | Malformed numeric string casts to null, not a crash | Pass | Pass |
| 15 | `test_filter_invalid_records_drops_unknown_event_type` | Unknown `event_type` rows are dropped | Pass | Pass |
| 16 | `test_filter_invalid_records_drops_negative_price` | Negative price rows are dropped | Pass | Pass |
| 17 | `test_filter_invalid_records_drops_purchase_with_zero_quantity` | Purchase with `quantity=0` is dropped | Pass | Pass |
| 18 | `test_filter_invalid_records_keeps_valid_view_with_zero_quantity` | A view with `quantity=0` is kept — the zero-quantity rule applies to purchases only | Pass | Pass |
| 19 | `test_deduplicate_events_removes_duplicate_ids` | Duplicate `event_id` within a batch collapses to 1 row | Pass | Pass |
| 20 | `test_add_derived_fields_computes_total_amount_for_purchase` | `total_amount = quantity * price` for purchases | Pass | Pass |
| 21 | `test_add_derived_fields_zero_total_amount_for_view` | `total_amount` is 0 for views | Pass | Pass |
| 22 | `test_transform_events_end_to_end` | Full transform: invalid dropped, dupes collapsed, fields added | Pass | Pass |

### Unit — SQL structure (`tests/test_database.py`)

| # | Test | What It Verifies | Expected Result | Actual Result |
|---|---|---|---|---|
| 23 | `test_create_tables_sql_defines_events_table` | `create_tables.sql` defines the table with a `PRIMARY KEY` | Pass | Pass |
| 24 | `test_create_tables_sql_has_expected_columns` | All nine expected columns are present | Pass | Pass |
| 25 | `test_validation_queries_file_is_nonempty_and_has_select_statements` | `validation_queries.sql` holds at least five `SELECT`s | Pass | Pass |

### Integration — database (`tests/test_database.py`)

| # | Test | What It Verifies | Expected Result | Actual Result |
|---|---|---|---|---|
| 26 | `test_events_table_exists` | `events` table exists in the target DB | Pass / Skip if DB unreachable | Pass |
| 27 | `test_duplicate_event_id_is_rejected` | DB rejects a second insert with the same `event_id` | Pass / Skip if DB unreachable | Pass |

> **Provenance of "Actual Result".** All 27 tests were executed together via
> `docker compose run --build --rm tests` and all 27 passed:

## Manual End-to-End Test Plan

Fill in "Actual Outcome" as you work through the steps.

| Step | Action | Expected Outcome | Actual Outcome |
|---|---|---|---|
| 1 | Start the generator (`docker compose up -d generator`, or `python -m src.generator.data_generator`) | New `events_<n>.csv` files appear in `data/incoming/`, numbered upward, every `interval_seconds` | |
| 2 | Start the streaming job (`docker compose up -d spark`, or `spark-submit ... spark_streaming_to_postgres.py`) | Log shows "Streaming query started" then periodic "Processing micro-batch" lines | |
| 3 | Query `SELECT COUNT(*) FROM events;` | Count increases over time, roughly matching generated events minus intentionally-invalid ones | |
| 4 | Stop and restart the streaming job | No duplicate rows appear (checkpoint + `PRIMARY KEY` both protect against this) | |
| 5 | Restart the generator | Numbering resumes above the highest existing file; no CSV is overwritten | |
| 6 | Manually drop a malformed CSV into `data/incoming/` (e.g. missing columns) | Job logs the issue via `DROPMALFORMED`/filtering, does not crash | |
| 7 | Stop PostgreSQL briefly while the job runs | Batch write fails, is logged, and the exception propagates — the streaming query **stops** rather than continuing. Deliberate: returning normally from `foreachBatch` would tell Spark the batch succeeded, committing offsets for rows never written | |
| 8 | Restart the job after step 7 | The uncommitted batch is replayed and converges rather than crash-looping. Rows already stored are skipped by the `ON CONFLICT DO NOTHING` merge, logged as `Batch N written (0 of M records inserted, M already present)` | |
| 9 | Check `outputs/performance/batch_metrics.csv` | One row per successfully written batch, with plausible timings | |

## What the Brief Asks Us to Test

Mapping the brief's checklist onto the tests above:

| Question from the brief | Covered by |
|---|---|
| Are the CSV files being generated correctly? | Tests 1–12; manual step 1 |
| Is Spark detecting and processing new files? | Manual steps 1–2 |
| Are the data transformations correct? | Tests 13–22 |
| Is data being written into PostgreSQL without errors? | Tests 26–27; manual steps 3–4 |
| Are performance metrics within expected limits? | Manual step 8; `docs/performance_metrics.md` |
