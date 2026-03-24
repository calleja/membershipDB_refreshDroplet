"""test_importer.py
==================
Integration tests for ``importer.run()`` and its helpers.

All target-side credentials come from TGT_DB_* env vars via ``importer.tgt_creds()``.
Source-side credentials come from SRC_DB_* env vars via ``query_runner.src_creds()``.

Test 3 fetches live rows from the source DB through the production
``query_runner.run_query2()`` and passes them directly to ``importer.run()``
so no query logic is duplicated here.

Pattern reference: test_completed_monkey_patch.py
"""

import datetime
import pytest
import mysql.connector
from dotenv import load_dotenv
from mysql.connector.abstracts import MySQLConnectionAbstract

from src.pipeline import importer, query_runner


# ══════════════════════════════════════════════════════════════════════════
# Fixtures
# ══════════════════════════════════════════════════════════════════════════


@pytest.fixture(scope="module")
def tgt_conn():
    """Module-scoped connection to the target database via importer.tgt_creds().

    All three tests share this connection to avoid repeated connect/disconnect
    overhead.  importer.run() (Test 3) creates its own internal connection, so
    this fixture is used only for direct table inspection and DDL helpers.
    """
    load_dotenv()
    conn = mysql.connector.connect(**importer.tgt_creds())
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def src_conn():
    """Module-scoped connection to the source database via query_runner.src_creds().

    Provided for Test 3 (and any future tests) that need to pull live rows
    from the source CiviCRM DB.  Source credentials are kept out of this file;
    they come from SRC_DB_* env vars loaded by load_dotenv().
    """
    load_dotenv()
    conn = mysql.connector.connect(**query_runner.src_creds())
    yield conn
    conn.close()


# ══════════════════════════════════════════════════════════════════════════
# Test 1 — Target DB Connectivity
# ══════════════════════════════════════════════════════════════════════════


@pytest.mark.importer
def test_target_db_connection(tgt_conn):
    """Verify importer.tgt_creds() yields a valid, live connection to the target DB."""
    assert tgt_conn.is_connected(), "Target DB connection should be active"
    assert isinstance(tgt_conn, MySQLConnectionAbstract), (
        "Expected a MySQLConnectionAbstract instance from mysql.connector.connect()"
    )


# ══════════════════════════════════════════════════════════════════════════
# Test 2 — executemany SQL Preparation
# ══════════════════════════════════════════════════════════════════════════


@pytest.mark.importer
def test_executemany_preparation(tgt_conn):
    """Test the INSERT SQL string (unit) and its schema compatibility (integration).

    Part A — pure unit, no DB: _build_insert_sql() must produce a correctly
    structured statement containing all 9 TARGET_COLS and 9 %s placeholders.

    Part B — integration: the built SQL must execute without error against the
    live target schema.  A synthetic 9-element row (matching column types) is
    inserted then rolled back so no test data persists.
    """
    # ── Part A: unit — validate the SQL string structure ────────────────
    insert_sql = importer._build_insert_sql()

    assert f"INSERT INTO {importer.TARGET_TABLE}" in insert_sql, (
        "INSERT statement must reference TARGET_TABLE"
    )
    assert insert_sql.count("%s") == len(importer.TARGET_COLS), (
        f"Expected {len(importer.TARGET_COLS)} %s placeholders, "
        f"got {insert_sql.count('%s')}"
    )
    for col in importer.TARGET_COLS:
        assert col in insert_sql, f"Column '{col}' missing from INSERT statement"

    # ── Part B: integration — prove SQL compiles against the live schema ─
    # Synthetic row: types match the CREATE TABLE DDL in ensure_table().
    #   0-3: TEXT / VARCHAR(255)   4: INT   5: VARCHAR(255)
    #   6: DATETIME                7: INT   8: LONGTEXT
    synthetic_row = (
        "Test Assignee",                         # Assignee_Name_act
        "Test Target",                           # Target_Name_act
        "source@test.com",                       # Source_Email_act
        "target@test.com",                       # Target_Email_act
        35,                                      # Activity_Type_act
        "Test Subject",                          # Subject_act
        datetime.datetime(2022, 1, 1, 12, 0, 0), # Activity_Date_act
        1,                                       # Activity_Status_act
        "Test details",                          # Activity_Details_act
    )

    cursor = tgt_conn.cursor()
    importer.ensure_table(cursor)               # DDL: implicit commit in MySQL
    cursor.execute(insert_sql, synthetic_row)   # DML: proves SQL compiles vs schema
    tgt_conn.rollback()                         # discard the single test row


# ══════════════════════════════════════════════════════════════════════════
# Test 3 — Write to Target via Production importer.run()
# ══════════════════════════════════════════════════════════════════════════


@pytest.mark.importer
def test_run_writes_to_target(src_conn, tgt_conn):
    """End-to-end write test using the production importer.run() function.

    Fetches live rows from the source DB via query_runner.run_query2(), hands
    them to importer.run() without modification, then uses tgt_conn to confirm
    that the expected number of rows landed in the staging table.

    importer.run() opens and closes its own target connection internally; tgt_conn
    is used only for the post-insert COUNT(*) verification.
    """
    START, END = "20220101000000", "20220101235959"

    # Pull live source data through the production query function.
    rows = query_runner.run_query2(src_conn, START, END)
    assert len(rows) > 0, f"Source query returned no rows for range {START}–{END}"

    # Delegate entirely to the production importer — no SQL recreated here.
    inserted = importer.run(rows, importer.TARGET_COLS)
    assert inserted == len(rows), (
        f"importer.run() reported {inserted} inserted rows, expected {len(rows)}"
    )

    # Verify the write using the fixture connection (separate from run()'s connection).
    cur = tgt_conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {importer.TARGET_TABLE}")
    (count,) = cur.fetchone()
    assert count == len(rows), (
        f"Target table has {count} rows after import; expected {len(rows)}"
    )
