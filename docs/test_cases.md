# Manual Test Plan

| ID | Test | Expected outcome | Actual outcome |
|---|---|---|---|
| TC-01 | Run `python src/data_generator.py --batches 1 --events-per-batch 5 --output-dir /tmp/events` | One CSV contains a header and five complete rows; all event IDs are unique | Record after execution |
| TC-02 | Start Spark, then generate TC-01 data | Spark detects the new file and logs rows written | Record after execution |
| TC-03 | Query PostgreSQL after TC-02 | Five rows exist with valid `view` or `purchase` values and UTC timestamps | Record after execution |
| TC-04 | Add a row with a negative price or invalid event type | Spark filters it and PostgreSQL constraints also reject invalid direct inserts | Record after execution |
| TC-05 | Copy a processed CSV into the input directory again | PostgreSQL ignores duplicate event IDs; Spark completes the batch and logs `rows_written=0` for duplicates | Record after execution |
| TC-06 | Stop and restart Spark using the same checkpoint | Previously committed files are not reprocessed | Record after execution |
| TC-07 | Run generator with `--batches 3 --seed 7` | Three numbered CSV files are produced reproducibly in shape and count | Record after execution |

For each run, record the command, date, row count, errors, and observed latency in the Actual outcome column.
