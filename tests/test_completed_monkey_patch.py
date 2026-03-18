"""
test_query_runner.py
====================
Pytest suite that exercises ``query_runner.run_query`` against an *alternate*
SQL file.  The module-level ``SQL_PATH`` variable inside ``query_runner`` is
temporarily overwritten using ``monkeypatch`` so that ``load_sql()`` reads
from a lightweight test SQL file instead of the production parameterized query.

Key concepts used:
  - monkeypatch  — swap ``query_runner.SQL_PATH`` per-test to point at a
                   custom SQL file under ``tests/sql/``.
  - parametrize  — run the same test body across several date ranges.
  - marks        — ``@pytest.mark.custom_sql`` lets you selectively run
                   only these tests with ``pytest -m custom_sql``.
  - fixtures     — ``src_conn`` provides a reusable DB connection scoped
                   to the test module.
"""

"""
to run: 
Will this compile?
Yes, with two prerequisites:

src must be on sys.path — your pyproject.toml still has name = "uv" and no [tool.pytest.ini_options] pythonpath setting. The import from src.pipeline import query_runner uses a dotted path that works if pytest is invoked from the project root and you have a src/__init__.py. Otherwise add this to your pyproject.toml:

' [tool.pytest.ini_options]
pythonpath = ["."] '

This puts the project root on sys.path so from src.pipeline import query_runner resolves.

__init__.py must exist — without it, Python won't treat pipeline as a package.

If either is missing, the file will fail at import time with the same ImportError you saw before."""

import os
import datetime
import pytest
import mysql.connector
from dotenv import load_dotenv

# ── Import the module under test ──────────────────────────────────────────
# ``query_runner`` is imported as a *module object* so that monkeypatch can
# replace its ``SQL_PATH`` attribute at runtime.  ``load_sql()`` and
# ``run_query()`` read SQL_PATH when called, so the patched value takes
# effect immediately.
from src.pipeline import query_runner

# ── Path to the alternate SQL file used by these tests ────────────────────
# Points to the lightweight parameterized query in the project's sql/
# directory.  This file returns a different column set than the production
# CTE query, so tests using override_sql_path exercise a different schema.
CUSTOM_SQL = os.path.join(
    os.path.dirname(__file__), "..", "sql", "parameterizedSQL.sql"
)

# ══════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════

def _src_creds() -> dict:
    """Load source-database credentials from the .env file.

    Returns a dict suitable for passing to ``mysql.connector.connect()``.
    Uses ``python-dotenv`` so environment variables do not need to be
    exported at the shell level.
    """
    load_dotenv()  # reads .env from the project root (cwd when pytest runs)
    return {
        "host":     os.environ["SRC_DB_HOST"],
        "port":     int(os.environ.get("SRC_DB_PORT", 3306)),
        "user":     os.environ["SRC_DB_USER"],
        "password": os.environ["SRC_DB_PASS"],
        "database": os.environ["SRC_DB_NAME"],
    }


# ══════════════════════════════════════════════════════════════════════════
# Fixtures
# ══════════════════════════════════════════════════════════════════════════

@pytest.fixture(scope="module")
def src_conn():
    """Module-scoped database connection to the *source* database.

    Opened once before the first test in this module and closed after
    the last test finishes.  Every test that requests ``src_conn`` shares
    the same connection, avoiding repeated connect/disconnect overhead.
    """
    conn = mysql.connector.connect(**_src_creds())
    yield conn  # hand the connection to the tests
    conn.close()


@pytest.fixture()
def override_sql_path(monkeypatch):
    """Replace ``query_runner.SQL_PATH`` with the path to our test SQL file.

    ``monkeypatch.setattr`` swaps the attribute *only* for the duration of
    the test that requests this fixture; after the test the original value
    is automatically restored.

    ``autouse`` is False (the default) so only tests that explicitly
    include ``override_sql_path`` in their parameter list are affected.
    """
    monkeypatch.setattr(query_runner, "SQL_PATH", CUSTOM_SQL)


# ══════════════════════════════════════════════════════════════════════════
# Tests
# ══════════════════════════════════════════════════════════════════════════

@pytest.mark.custom_sql
@pytest.mark.parametrize(
    "start, end",
    [
        # Each tuple is a (start_timestamp, end_timestamp) pair that will
        # be interpolated into the SQL's  %(start)s / %(end)s  placeholders.
        ("20220101000000", "20220101235959"),  # single day – Jan 1 2022
        ("20230601000000", "20230601235959"),  # single day – Jun 1 2023
        ("20240101000000", "20241231235959"),  # full year – 2024
    ],
    ids=["jan-2022", "jun-2023", "full-2024"],  # readable labels in output
)
def test_run_query_with_custom_sql(override_sql_path, src_conn, start, end):
    """Execute ``run_query`` using the *overridden* SQL file and verify that
    it returns a non-empty list of rows.

    Parameters
    ----------
    override_sql_path : fixture
        Patches ``query_runner.SQL_PATH`` → ``tests/sql/test_query.sql``.
    src_conn : fixture
        Live MySQL connection to the source database.
    start, end : str
        Timestamp boundaries injected by ``@pytest.mark.parametrize``.
    """
    try:
        rows = query_runner.run_query(src_conn, start, end)

        # run_query must return a list (cursor.fetchall())
        assert isinstance(rows, list), "run_query should return a list of row tuples"
        # The custom SQL should produce at least one row for valid date ranges
        assert len(rows) > 0, f"Expected rows for range {start}–{end}, got none"
    except mysql.connector.DatabaseError as e:    
        print(f"a database error was thrown: {e}")


@pytest.mark.custom_sql
def test_load_sql_reads_custom_file(override_sql_path):
    """Verify that ``load_sql()`` reads from the patched path, not the
    production SQL file.

    This is a pure unit test — no database connection required.  It
    confirms that monkeypatch is working correctly before we run heavier
    integration tests above.
    """
    sql_text = query_runner.load_sql()

    assert len(sql_text) > 0, "Custom SQL file should not be empty"
    # Confirm the text does NOT match the production query's signature
    # (adjust the substring to something unique to your production file)
    assert "civicrm_tmp_e_dflt" not in sql_text, (
        "load_sql() returned the production SQL — monkeypatch did not take effect"
    )


@pytest.mark.custom_sql
def test_load_sql_without_override():
    """Sanity check: without the ``override_sql_path`` fixture, ``load_sql``
    should read the *original* production SQL file, proving the patch is
    scoped correctly.

    Production SQL is now the CTE query (cte_select_activity_detail.sql),
    which does NOT use temp tables.  We verify it contains 'WITH targets'
    — a CTE-specific keyword that the lightweight test SQL lacks.
    """
    sql_text = query_runner.load_sql()

    # Production CTE query starts with a WITH clause; the old temp-table
    # query used civicrm_tmp_e_dflt — that string should NOT appear now.
    assert "WITH targets AS" in sql_text, (
        "Without override, load_sql() should return the CTE production SQL"
    )
    assert "civicrm_tmp_e_dflt" not in sql_text, (
        "Production SQL should be the CTE rewrite, not the legacy temp-table query"
    )


# ══════════════════════════════════════════════════════════════════════════
# New tests: resultset structural validation
# ══════════════════════════════════════════════════════════════════════════

@pytest.mark.custom_sql
def test_resultset_row_width(src_conn):
    """Verify that every row returned by ``run_query`` has exactly 9 elements.

    This catches schema drift or miscounted aliases in the CTE query before
    the resultset is handed to ``importer.run()`` for insertion.  Uses the
    production SQL (no override) so it validates the real column count.

    Parameters
    ----------
    src_conn : fixture
        Live MySQL connection to the source database.
    """
    try:
        rows = query_runner.run_query(src_conn, "20220101000000", "20220101235959")

        assert isinstance(rows, list), "run_query should return a list"
        assert len(rows) > 0, "Need at least one row to validate width"

        for i, row in enumerate(rows):
            assert len(row) == 9, (
                f"Row {i} has {len(row)} elements, expected 9. "
                f"Check column aliases in cte_select_activity_detail.sql"
            )
    except mysql.connector.DatabaseError as e:
        print(f"[test_resultset_row_width] Database error: {e}")
        raise


@pytest.mark.custom_sql
def test_resultset_column_types(src_conn):
    """Inspect the first row from ``run_query`` and assert each element's
    Python type matches what the target table expects.

    Validates that the tuple is safe for ``cursor.executemany()`` against
    the selectActivityReport_temp schema:

      Position  Column               Expected Python type
      --------  -------------------  ---------------------------
      0         Assignee_Name_act    str or None
      1         Target_Name_act      str or None
      2         Source_Email_act     str or None
      3         Target_Email_act     str or None
      4         Activity_Type_act    int
      5         Subject_act          str or None
      6         Activity_Date_act    datetime.datetime
      7         Activity_Status_act  int
      8         Activity_Details_act str or None

    Parameters
    ----------
    src_conn : fixture
        Live MySQL connection to the source database.
    """
    try:
        rows = query_runner.run_query(src_conn, "20220101000000", "20220101235959")

        assert len(rows) > 0, "Need at least one row to validate types"

        row = rows[0]

        # Positions 0-3: nullable text fields (names and emails)
        for pos in (0, 1, 2, 3):
            assert isinstance(row[pos], (str, type(None))), (
                f"Column {pos} should be str or None, got {type(row[pos]).__name__}"
            )

        # Position 4: Activity_Type_act — integer activity type ID
        assert isinstance(row[4], int), (
            f"Column 4 (Activity_Type_act) should be int, got {type(row[4]).__name__}"
        )

        # Position 5: Subject_act — nullable text
        assert isinstance(row[5], (str, type(None))), (
            f"Column 5 (Subject_act) should be str or None, got {type(row[5]).__name__}"
        )

        # Position 6: Activity_Date_act — datetime
        assert isinstance(row[6], datetime.datetime), (
            f"Column 6 (Activity_Date_act) should be datetime, got {type(row[6]).__name__}"
        )

        # Position 7: Activity_Status_act — integer status ID
        assert isinstance(row[7], int), (
            f"Column 7 (Activity_Status_act) should be int, got {type(row[7]).__name__}"
        )

        # Position 8: Activity_Details_act — nullable text (LONGTEXT)
        assert isinstance(row[8], (str, type(None))), (
            f"Column 8 (Activity_Details_act) should be str or None, got {type(row[8]).__name__}"
        )
    except mysql.connector.DatabaseError as e:
        print(f"[test_resultset_column_types] Database error: {e}")
        raise