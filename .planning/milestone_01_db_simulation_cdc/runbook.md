# Goal
Implement an operational SQLite database simulation that models orders and customer updates, and write a CDC extraction process that writes transactional mutation payloads as JSON change-feed files into a local buffer directory.

# Implementation Steps
1. Create a script named `scripts/db_simulator.py` that:
   - Initializes a SQLite database (`data/source_oltp.db`) with tables: `customers` and `orders`.
   - Populates initial mock tables.
   - Runs an infinite loop (sleeping 5s between iterations) that performs a random number (1 to 5) of SQL Insert, Update, or Delete operations on both tables.
2. Enable SQLite WAL (Write-Ahead Logging) mode on startup to avoid concurrency blocks.
3. Write a second script `scripts/cdc_extractor.py` that:
   - Connects to `data/source_oltp.db`.
   - Tracks a state variable `last_processed_timestamp`.
   - Queries tables for any modifications where `updated_at > last_processed_timestamp`.
   - Outputs the mutations as JSON files in `data/cdc_buffer/` with filenames like `cdc_batch_{timestamp}.json`.
   - Populates standard CDC attributes: `_change_type`, `_commit_timestamp`, and `_row_sequence_id`.
   - Updates `last_processed_timestamp` state file upon success.

# Validation Checklist
- [ ] Run `python scripts/db_simulator.py` and verify `data/source_oltp.db` is created and actively populated.
- [ ] Verify SQL updates write new records and modify `updated_at` values.
- [ ] Run `python scripts/cdc_extractor.py` and check if files appear in `data/cdc_buffer/`.
- [ ] Verify a CDC JSON file contains the custom properties: `_change_type`, `_commit_timestamp`, and `_row_sequence_id`.
- [ ] Assert that deleted database records appear in the JSON output marked with `_change_type = 'DELETE'`.

# Rollback Plan
- Stop all running python simulator scripts.
- Delete the created operational database file `data/source_oltp.db`.
- Remove all files inside the `data/cdc_buffer/` directory.
- Use Git to checkout clean state: `git checkout -- scripts/`

# Expected Git Commit Message
feat(cdc): implement source simulation and delta change data feed
