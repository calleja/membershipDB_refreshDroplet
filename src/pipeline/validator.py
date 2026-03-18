"""Validation utilities for the in-memory resultset pipeline.

Validates that the list-of-tuples returned by query_runner.run() has the
correct shape (9 columns per row, non-empty) before it is handed to
importer.run() for insertion into the target database.

Also provides a no-argument smoke-test mode for CI that queries the source
database with a hardcoded date range and asserts a non-empty result.
"""

import os
import mysql.connector


# Expected number of columns per row (matches the 9 aliased columns
# in cte_select_activity_detail.sql).
EXPECTED_WIDTH = 9


def run(rows: list = None, cols: list = None):
    """Validate the in-memory resultset or run a CI smoke test.

    When called with rows (and optionally cols), validates that:
      - rows is a non-empty list
      - every row is a tuple with exactly EXPECTED_WIDTH elements

    When called with no arguments, performs a live smoke test against the
    source database using a hardcoded date range (Jan 1 2022) and asserts
    that at least one row is returned.

    Parameters
    ----------
    rows : list[tuple], optional
        The resultset from query_runner.run().  If None, smoke-test mode.
    cols : list[str], optional
        Column names from cursor.description (used for error messages).

    Raises
    ------
    ValueError
        If rows is empty or any row has the wrong number of elements.
    AssertionError
        If the smoke-test query returns no rows.
    """
    if rows is not None:
        # ── In-memory validation mode ─────────────────────────────────
        if not isinstance(rows, list):
            raise ValueError(
                f"[validator.run] Expected list of tuples, got {type(rows).__name__}"
            )
        if len(rows) == 0:
            raise ValueError("[validator.run] Resultset is empty — nothing to import")

        for i, row in enumerate(rows):
            if not isinstance(row, tuple):
                raise ValueError(
                    f"[validator.run] Row {i} is {type(row).__name__}, expected tuple"
                )
            if len(row) != EXPECTED_WIDTH:
                raise ValueError(
                    f"[validator.run] Row {i} has {len(row)} elements, "
                    f"expected {EXPECTED_WIDTH}"
                )
        return  # all checks passed

    # ── Smoke-test mode (no arguments) ────────────────────────────────
    # Exercises the source query with a known date range to verify
    # connectivity and SQL syntax.  Useful in CI/CD pipelines.
    from .query_runner import src_creds, load_sql

    try:
        conn = mysql.connector.connect(**src_creds())
    except mysql.connector.Error as e:
        print(f"[validator.run] Smoke test failed — cannot connect to source DB")
        raise

    try:
        cur = conn.cursor()
        start = '20220101000000'
        end = '20220101235959'
        sql = load_sql() % {'start': start, 'end': end}
        cur.execute(sql)
        rows = cur.fetchall()
        assert rows, (
            "[validator.run] Smoke test failed — source query returned no rows "
            f"for range {start}–{end}"
        )
    except mysql.connector.Error as e:
        print(f"[validator.run] Smoke test query execution failed")
        raise
    finally:
        conn.close()
