import os
import pytest
import mysql.connector

from uv import query_runner


def src_creds():
    return {
        'host': os.environ['SRC_DB_HOST'],
        'port': int(os.environ.get('SRC_DB_PORT', 3306)),
        'user': os.environ['SRC_DB_USER'],
        'password': os.environ['SRC_DB_PASS'],
        'database': os.environ['SRC_DB_NAME'],
    }


def tgt_creds():
    return {
        'host': os.environ['TGT_DB_HOST'],
        'port': int(os.environ.get('TGT_DB_PORT', 3306)),
        'user': os.environ['TGT_DB_USER'],
        'password': os.environ['TGT_DB_PASS'],
        'database': os.environ['TGT_DB_NAME'],
    }


@pytest.fixture(scope="module")
def tgt_conn():
    conn = mysql.connector.connect(**tgt_creds())
    yield conn
    conn.close()


def test_target_connection(tgt_conn):
    assert tgt_conn.is_connected()


def test_query_compiles(tgt_conn):
    sql = query_runner.load_sql()
    # use innocuous dates to force parsing
    compiled = f"EXPLAIN {sql % {'start': '20220101000000', 'end': '20220101235959'}}"
    cur = tgt_conn.cursor()
    cur.execute(compiled)
    # no exception means compilation succeeded


def test_resultset_not_null():
    conn = mysql.connector.connect(**src_creds())
    try:
        rows = query_runner.run_query(conn, '20220101000000', '20220101235959')
        assert len(rows) > 0
    finally:
        conn.close()
