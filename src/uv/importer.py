"""Placeholder for import functionality."""

import os
import mysql.connector
import csv


def tgt_creds():
    return {
        'host': os.environ['TGT_DB_HOST'],
        'port': int(os.environ.get('TGT_DB_PORT', 3306)),
        'user': os.environ['TGT_DB_USER'],
        'password': os.environ['TGT_DB_PASS'],
        'database': os.environ['TGT_DB_NAME'],
    }


def run(input_path: str):
    """Load CSV into staging table on target database.

    This is a simple row-by-row insert; real code would batch and truncate
    as described in the plan.
    """
    creds = tgt_creds()
    conn = mysql.connector.connect(**creds)
    try:
        cur = conn.cursor()
        # for now just read file and count rows
        with open(input_path, newline="") as f:
            reader = csv.reader(f)
            headers = next(reader)
            for row in reader:
                # placeholder: would build INSERT statement here
                pass
    finally:
        conn.close()
