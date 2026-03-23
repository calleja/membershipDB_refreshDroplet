"""Helpers for executing the CTE-based SQL query against the source CiviCRM database.

This module connects to the source database, runs the parameterized CTE query
from sql/cte_select_activity_detail.sql, and returns the resultset as an
in-memory (cols, rows) tuple for downstream validation and import.

No CSV is produced; the data stays in-memory throughout the pipeline.
"""

import os
import mysql.connector
from mysql.connector.abstracts import MySQLConnectionAbstract

# Path to the CTE-based SQL query (replaces the legacy temp-table query).
# Requires MySQL 8.0+ on the source server.
SQL_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), "..", "..", "sql", "cte_select_activity_detail.sql"
    )
)


def load_sql():
    """Read and return the raw SQL text from the file at SQL_PATH.

    The returned string contains %(start)s and %(end)s placeholders
    that must be interpolated before execution.
    """
    try:
        with open(SQL_PATH, "r") as f:
            return f.read()
    except FileNotFoundError as e:
        print(f"[query_runner.load_sql] SQL file not found at: {SQL_PATH}")
        raise


def to_timestamp(date_str: str, is_start: bool) -> str:
    """Convert a YYYYMMDD date string into a YYYYMMDDhhmmss timestamp edge.

    Parameters
    ----------
    date_str : str
        Eight-digit date string, e.g. '20220101'.
    is_start : bool
        True  → append '000000' (start of day).
        False → append '235959' (end of day).

    Returns
    -------
    str
        Fourteen-character timestamp string.

    Raises
    ------
    ValueError
        If date_str is not exactly 8 digits.
    """
    if len(date_str) != 8 or not date_str.isdigit():
        raise ValueError("date must be YYYYMMDD")
    return date_str + ("000000" if is_start else "235959")


def src_creds():
    """Build a credentials dict for the source database from environment variables.

    Expected env vars: SRC_DB_HOST, SRC_DB_PORT (default 3306),
    SRC_DB_USER, SRC_DB_PASS, SRC_DB_NAME.

    Returns
    -------
    dict
        Keyword arguments for mysql.connector.connect().
    """
    return {
        "host": os.environ["SRC_DB_HOST"],
        "port": int(os.environ.get("SRC_DB_PORT", 3306)),
        "user": os.environ["SRC_DB_USER"],
        "password": os.environ["SRC_DB_PASS"],
        "database": os.environ["SRC_DB_NAME"],
    }


def run(start: str, end: str):
    """Query the source database and return the resultset in-memory.

    Connects to the source CiviCRM database, executes the CTE query with
    the given timestamp boundaries, and returns (cols, rows).

    Parameters
    ----------
    start : str
        Start timestamp in YYYYMMDDhhmmss format.
    end : str
        End timestamp in YYYYMMDDhhmmss format.

    Returns
    -------
    tuple[list[str], list[tuple]]
        (cols, rows) where cols is a list of column name strings and
        rows is cursor.fetchall() — a list of tuples.

    Raises
    ------
    ValueError
        If the query returns zero rows.
    """
    sql = load_sql()
    formatted = sql % {"start": start, "end": end}

    try:
        conn = mysql.connector.connect(**src_creds())
    except mysql.connector.Error as e:
        print(f"[query_runner.run] Failed to connect to source database")
        raise

    try:
        cursor = conn.cursor()
        cursor.execute(formatted)
        cols = [d[0] for d in cursor.description]
        rows = cursor.fetchall()
    except mysql.connector.Error as e:
        print(f"[query_runner.run] Query execution failed")
        raise
    finally:
        conn.close()

    # Guard: do not proceed with an empty resultset
    if not rows:
        raise ValueError(
            f"[query_runner.run] Source query returned no rows for "
            f"range {start}–{end}. Aborting to protect target data."
        )

    return cols, rows


def run_query(conn, start: str, end: str):
    """Execute the CTE query on an existing connection and return raw rows.

    This is the test-friendly variant — it accepts a pre-existing connection
    so tests can share a module-scoped connection and monkeypatch SQL_PATH.

    Parameters
    ----------
    conn : mysql.connector.connection.MySQLConnection
        An open connection to the source database.
    start : str
        Start timestamp in YYYYMMDDhhmmss format.
    end : str
        End timestamp in YYYYMMDDhhmmss format.

    Returns
    -------
    list[tuple]
        cursor.fetchall() — each tuple has 9 elements matching the
        aliased columns in the CTE query.
    """
    sql = load_sql()
    formatted = sql % {"start": start, "end": end}
    try:
        cur = conn.cursor()
        cur.execute(formatted)
        return cur.fetchall()
    except mysql.connector.Error as e:
        print(f"[query_runner.run_query] Query execution failed on provided connection")
        raise


def run_query2(conn, start: str, end: str):
    # this version of the function passed the pytests
    # start, end = ("20240101000000", "20240301235959")
    # start, end = ("20240101000000", "20240301235959")
    sql = load_sql()  # just loading the SQL that's identified at top of module
    formatted = sql % {"start": start, "end": end}

    if conn == None:
        try:
            conn = mysql.connector.connect(**src_creds())
        except mysql.connector.Error as e:
            print(f"[query_runner.run] Failed to connect to source database")
            raise
    else:
        None

    try:
        cursor = conn.cursor()  # cursor returns a MySQLCursor object
        cursor.execute(
            formatted
        )  # execute() returns None; results must be fetched separately
        rows = cursor.fetchall()
        # assert (len(rows) > 0, "rows is not greater than 0") <- assert statement for a pytest
    except mysql.connector.Error as e:
        # this error type catches all mysql.connector Exceptions, so it's ambiguous
        print(f"error thrown in run_query2: {e}")
        # raise
    finally:
        conn.close()
