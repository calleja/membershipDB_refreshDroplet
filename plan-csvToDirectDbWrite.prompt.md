# Plan: Refactor Pipeline from CSV to Direct DB Write

**TL;DR** — Eliminate CSV as an intermediary. The pipeline becomes: query source DB → validate resultset in-memory → INSERT directly into `selectActivityReport_temp` on `membership_ard`. Column renaming (14 SQL cols → 9 downstream) done via SQL aliases in the CTE. CLI simplified from 3 subcommands to a single `sync` command.

---

## Steps

### Phase 1 — SQL: Rename & Reduce Columns in CTE

1. **Modify `sql/cte_select_activity_detail.sql`** final SELECT — keep only 9 columns and alias them to downstream names:
   - `civicrm_contact_contact_assignee` → `Assignee_Name_act`
   - `civicrm_contact_contact_target` → `Target_Name_act`
   - `civicrm_email_contact_source_email` → `Source_Email_act`
   - `civicrm_email_contact_target_email` → `Target_Email_act`
   - `civicrm_activity_activity_type_id` → `Activity_Type_act`
   - `civicrm_activity_activity_subject` → `Subject_act`
   - `civicrm_activity_activity_date_time` → `Activity_Date_act`
   - `civicrm_activity_status_id` → `Activity_Status_act`
   - `civicrm_activity_details` → `Activity_Details_act`
   
   Drop the 5 unused columns (`source_id`, `assignee_id`, `target_id`, `activity_id`, `source_record_id`).

   **Recommendation: Column renaming in SQL** is the most straightforward approach. Aliases in the CTE's final SELECT become the single source of truth — no Python mapping dict needed, and the SQL is self-documenting.

2. **Update `SQL_PATH` in `src/pipeline/query_runner.py`** to point at `cte_select_activity_detail.sql` instead of `select_activity_details_parameterized.sql`. This adopts the CTE rewrite (MySQL 8.0+) as the production query.

### Phase 2 — Python: Refactor `query_runner.py` *(depends on Phase 1)*

3. **Refactor `run()` in `src/pipeline/query_runner.py`**:
   - Remove all CSV writing logic (`csv.writer`, `--output` param, file open/close)
   - Return `(cols, rows)` tuple — `cols` from `cursor.description`, `rows` from `cursor.fetchall()`
   - Add a guard: if `rows` is empty, raise `ValueError("Source query returned no rows")` — mirrors the existing if-else pattern the user specified
   - Keep `src_creds()`, `load_sql()`, `to_timestamp()` unchanged

4. **`run_query()` — no signature change needed**. It already returns `cursor.fetchall()`. Verify it works with the new 9-column CTE.

### Phase 3 — Python: Refactor `importer.py` *(depends on Phase 2)*

5. **Rewrite `run()` in `src/pipeline/importer.py`**:
   - Change signature: `run(rows: list, cols: list)` — accept in-memory data, not a CSV path
   - Guard: assert `rows` is not empty before proceeding (defense-in-depth, `query_runner.run()` already guards)
   - Connect to target via existing `tgt_creds()`
   - Call `ensure_table()` (step 6) — this drops and recreates the table, guaranteeing a clean schema every run
   - `cursor.executemany()` with parameterized INSERT
   - `conn.commit()` on success, `conn.rollback()` on exception
   - Return inserted row count

6. **Add `ensure_table()` helper** *(parallel with step 5)*:
   - Run `DROP TABLE IF EXISTS selectActivityReport_temp` followed by `CREATE TABLE selectActivityReport_temp` with the 9 columns and appropriate MySQL types (TEXT for names, VARCHAR(255) for emails, INT for type/status, DATETIME for date, LONGTEXT for details)
   - This guarantees the schema always matches the current column definitions — no stale columns, no type mismatches from prior runs or manual edits
   - Both statements are DDL (implicit commit in MySQL), so they execute outside the subsequent INSERT transaction. This is acceptable because the table is a staging table rebuilt from scratch every run

### Phase 4 — Python: Refactor `validator.py` *(depends on Phase 2)*

7. **Refactor `run()` in `src/pipeline/validator.py`**:
   - Remove CSV file validation branch
   - New signature: `run(rows: list, cols: list = None)`
   - Assert `rows` is a list, `len(rows) > 0`, each row is a tuple of length 9
   - Keep the no-argument smoke-test fallback (query source with hardcoded dates) for CI

### Phase 5 — Python: Refactor `cli.py` *(depends on Phases 2–4)*

8. **Simplify `src/pipeline/cli.py`**:
   - **Remove**: `query` subcommand (no standalone CSV output)
   - **Remove**: `import --input` subcommand (no CSV import)
   - **Remove**: `validate --input` subcommand (no CSV to validate)
   - **Add**: `sync` command — single orchestration entrypoint:
     - `--start` (required): YYYYMMDD
     - `--end` (required): YYYYMMDD
     - `--dry-run` (optional flag): query + validate, skip INSERT
   - **Keep**: `validate` subcommand (no args) for CI smoke tests
   - `sync` flow: `query_runner.run()` → `validator.run()` → `importer.run()` → print summary

### Phase 6 — Tests: Additions to `test_completed_monkey_patch.py` *(parallel with Phases 2–5)*

9. **Add `test_resultset_row_width`**:
   - Call `run_query()` with a known date range
   - Assert **every** tuple in the returned list has exactly 9 elements
   - Validates structural integrity before injection — catches schema drift or SQL alias miscounts

10. **Add `test_resultset_column_types`**:
    - Inspect the first row returned by `run_query()`:
      - Positions 0–3 (names/emails): `isinstance(val, (str, type(None)))`
      - Position 4 (Activity_Type_act): `isinstance(val, int)`
      - Position 5 (Subject_act): `isinstance(val, (str, type(None)))`
      - Position 6 (Activity_Date_act): `isinstance(val, datetime.datetime)`
      - Position 7 (Activity_Status_act): `isinstance(val, int)`
      - Position 8 (Activity_Details_act): `isinstance(val, (str, type(None)))`
    - Ensures the tuple is compatible with the target table's column types and safe for `executemany()`

11. **Update test infrastructure**:
    - Update `CUSTOM_SQL` path — the test SQL must also be updated to return 9 aliased columns
    - Update `test_load_sql_without_override` assertion — production SQL is now the CTE, so `civicrm_tmp_e_dflt` should NOT be found (reverse the assertion)

### Phase 7 — SQL: Target Table DDL *(parallel with Phase 3)*

12. **Create `sql/create_selectActivityReport_temp.sql`** for documentation and `ensure_table()`:
    ```sql
    CREATE TABLE IF NOT EXISTS selectActivityReport_temp (
        Assignee_Name_act   TEXT,
        Target_Name_act     TEXT,
        Source_Email_act     VARCHAR(255),
        Target_Email_act     VARCHAR(255),
        Activity_Type_act    INT,
        Subject_act          VARCHAR(255),
        Activity_Date_act    DATETIME,
        Activity_Status_act  INT,
        Activity_Details_act LONGTEXT
    );
    ```
   Use `DROP TABLE IF EXISTS` + `CREATE TABLE` (not `CREATE TABLE IF NOT EXISTS`) so that schema drift from manual alterations is automatically corrected each run.

---

## Relevant Files

- `sql/cte_select_activity_detail.sql` — Modify final SELECT: alias 9 columns, drop 5
- `sql/select_activity_details_parameterized.sql` — Retired, kept for reference only
- `sql/create_selectActivityReport_temp.sql` — New DDL for target table (to be created)
- `src/pipeline/query_runner.py` — Update `SQL_PATH`, remove CSV from `run()`, return `(cols, rows)`
- `src/pipeline/importer.py` — Rewrite: accept in-memory data, DELETE+INSERT into `selectActivityReport_temp`
- `src/pipeline/validator.py` — Rewrite: validate list-of-tuples instead of CSV
- `src/pipeline/cli.py` — Replace `query`/`import`/`validate --input` with single `sync` command
- `tests/test_completed_monkey_patch.py` — Add 2 tests, update `CUSTOM_SQL` path and override assertions

## Verification

1. **Unit tests** — `pytest -m custom_sql`: monkeypatch loads CTE correctly; `run_query()` returns 9-element tuples; column types match schema
2. **Integration tests** — `pytest tests/test_integration.py`: target connection works; CTE compiles (`EXPLAIN`); resultset non-empty
3. **Manual E2E** — `uv sync --start 20220101 --end 20220101`: verify `selectActivityReport_temp` exists in `membership_ard`, row count matches source, column names match 9 expected
4. **Dry-run** — `uv sync --start 20220101 --end 20220101 --dry-run`: confirms query + validation without INSERT
5. **Empty resultset guard** — Run with a date range known to return 0 rows; pipeline aborts with clear message, does NOT delete existing target data

## Decisions

- **Column renaming in SQL** — aliases in CTE final SELECT, not a Python mapping dict
- **CTE replaces temp-table SQL** — `cte_select_activity_detail.sql` is production; requires MySQL 8.0+ on source
- **`DROP TABLE IF EXISTS` + `CREATE TABLE`** — the staging table is rebuilt from scratch each run, so `ensure_table()` drops and recreates it. This eliminates schema drift risk (e.g., someone manually altered a column). Both are DDL with implicit commits in MySQL, but since the table is staging-only and always fully rewritten, this is acceptable. The subsequent `executemany()` + `commit()` is still wrapped in try/except with `rollback()` to protect the INSERT phase
- **No CSV anywhere** — `--output` flag, `csv.writer`, `csv.reader` all removed
- **`sync` is the orchestration command** — replaces the 3-step CLI workflow

## Further Considerations

1. **`tests/sql/` directory** — the test file references `tests/sql/parameterizedSQL.sql` but that directory doesn't exist. Options: (A) create it and put a 9-column test query there, or (B) point `CUSTOM_SQL` at `sql/parameterizedSQL.sql` in project root and update that file to match the 9-column schema. **Recommend (A)** — keeps test SQL isolated from production SQL.

2. **SSH tunnel** — `plan-sshBridgeDecorator.prompt.md` suggests an SSH bridge may be needed for DB connectivity from the Droplet. This refactor doesn't address networking — both source and target must be reachable.

3. **Transaction safety** — `ensure_table()` uses DDL (`DROP` + `CREATE`) which auto-commits in MySQL, so the table is empty before `executemany()` begins. If `executemany()` fails, `rollback()` leaves the table empty (no partial data). This is acceptable for a staging table that is always fully rewritten. If partial-failure recovery is ever needed, consider inserting into a shadow table and swapping via `RENAME TABLE`.
