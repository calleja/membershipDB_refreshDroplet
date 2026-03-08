"""Helpers for executing the parameterized SQL against the source database."""

import os
import mysql.connector
import csv

SQL_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "select_activity_details_parameterized.sql"))


def load_sql():
    """Return the raw SQL text from the parameterized file."""
    with open(SQL_PATH, "r") as f:
        return f.read()


def to_timestamp(date_str: str, is_start: bool) -> str:
    """Convert YYYYMMDD into YYYYMMDDhhmmss edge value.

    * start of day -> append 000000
    * end of day   -> append 235959
    """
    if len(date_str) != 8 or not date_str.isdigit():
        raise ValueError("date must be YYYYMMDD")
    return date_str + ("000000" if is_start else "235959")


def src_creds():
    """Read source DB credentials from environment."""
    return {
        'host': os.environ['SRC_DB_HOST'],
        'port': int(os.environ.get('SRC_DB_PORT', 3306)),
        'user': os.environ['SRC_DB_USER'],
        'password': os.environ['SRC_DB_PASS'],
        'database': os.environ['SRC_DB_NAME'],
    }


def run(start: str, end: str, output: str = "-"):
    """Execute the parameterized SQL with given start/end timestamps.

    If ``output`` is ``"-"`` the rows are written to stdout, otherwise to the
    named CSV file.
    """
    sql = load_sql()
    formatted = sql % {'start': start, 'end': end}

    conn = mysql.connector.connect(**src_creds())
    try:
        cursor = conn.cursor()
        cursor.execute(formatted)
        cols = [d[0] for d in cursor.description]
        rows = cursor.fetchall()
    finally:
        conn.close()

    if output == "-":
        writer = csv.writer(os.sys.stdout)
    else:
        f = open(output, "w", newline="")
        writer = csv.writer(f)
    writer.writerow(cols)
    writer.writerows(rows)
    if output != "-":
        f.close()


def run_query(conn, start: str, end: str):
    """Helper for tests: run query on given connection and return list of rows."""
    sql = load_sql()
    formatted = sql % {'start': start, 'end': end}
    cur = conn.cursor()
    cur.execute(formatted)
    return cur.fetchall()
