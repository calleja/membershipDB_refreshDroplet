"""Simple validation utilities and a CLI entrypoint for `uv validate`."""

import os
import csv
import mysql.connector


def run(input_path: str = None):
    """Validate either a CSV file or perform smoke checks against environment.

    If ``input_path`` is provided the file is parsed and basic checks run.
    Otherwise the function will simply ensure the source query returns at
    least one row (useful for CI jobs).
    """
    if input_path:
        with open(input_path, newline="") as f:
            reader = csv.reader(f)
            headers = next(reader, [])
            assert headers, "no headers found in CSV"
            count = 0
            for _ in reader:
                count += 1
            assert count > 0, "CSV contained no rows"
    else:
        # fallback: run the query against the source and expect >0 rows
        from .query_runner import src_creds, load_sql
        conn = mysql.connector.connect(**src_creds())
        try:
            cur = conn.cursor()
            start = '20220101000000'
            end = '20220101235959'
            sql = load_sql() % {'start': start, 'end': end}
            cur.execute(sql)
            rows = cur.fetchall()
            assert rows, "source query returned no rows"
        finally:
            conn.close()
