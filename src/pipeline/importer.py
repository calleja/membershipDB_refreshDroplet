"""Import module: writes in-memory resultset rows directly into the
selectActivityReport_temp staging table on the target database.

No CSV intermediary — data flows from query_runner.run() → validator.run()
→ importer.run() entirely in-memory.
"""

import os
import mysql.connector


# ── Column names for the 9-column staging table ──────────────────────────
# Must match the aliases in the CTE query (cte_select_activity_detail.sql)
# and the DDL in sql/create_selectActivityReport_temp.sql.
TARGET_TABLE = "selectActivityReport_temp"
TARGET_COLS = [
    "Assignee_Name_act",
    "Target_Name_act",
    "Source_Email_act",
    "Target_Email_act",
    "Activity_Type_act",
    "Subject_act",
    "Activity_Date_act",
    "Activity_Status_act",
    "Activity_Details_act",
]


def tgt_creds():
    """Build a credentials dict for the target database from environment variables.

    Expected env vars: TGT_DB_HOST, TGT_DB_PORT (default 3306),
    TGT_DB_USER, TGT_DB_PASS, TGT_DB_NAME.

    Returns
    -------
    dict
        Keyword arguments for mysql.connector.connect().
    """
    return {
        "host": os.environ["TGT_DB_HOST"],
        "port": int(os.environ.get("TGT_DB_PORT", 3306)),
        "user": os.environ["TGT_DB_USER"],
        "password": os.environ["TGT_DB_PASS"],
        "database": os.environ["TGT_DB_NAME"],
    }


def ensure_table(cursor):
    """Drop and recreate the staging table to guarantee a clean schema.

    Uses DROP TABLE IF EXISTS followed by CREATE TABLE so that any prior
    schema drift (manual column alterations, type changes) is corrected.
    Both statements are DDL and trigger an implicit commit in MySQL — this
    is acceptable because the table is staging-only and fully rebuilt each
    sync cycle.

    Parameters
    ----------
    cursor : mysql.connector.cursor.MySQLCursor
        An open cursor on the target database connection.
    """
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
        create_ddl = f"""
            CREATE TABLE {TARGET_TABLE} (
                Assignee_Name_act    TEXT,
                Target_Name_act      TEXT,
                Source_Email_act     VARCHAR(255),
                Target_Email_act     VARCHAR(255),
                Activity_Type_act    VARCHAR(255),
                Subject_act          VARCHAR(255),
                Activity_Date_act    DATETIME,
                Activity_Status_act  INT,
                Activity_Details_act LONGTEXT
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
        cursor.execute(create_ddl)
    except mysql.connector.Error as e:
        print(f"[importer.ensure_table] DDL failed on target database")
        raise


def _build_insert_sql() -> str:
    """Build the parameterized INSERT statement from TARGET_TABLE and TARGET_COLS.

    Returns a SQL string with %s placeholders ready for cursor.executemany().
    Pure function — no side effects, no DB access.
    """
    placeholders = ", ".join(["%s"] * len(TARGET_COLS))
    col_names = ", ".join(TARGET_COLS)
    return f"INSERT INTO {TARGET_TABLE} ({col_names}) VALUES ({placeholders})"


def run(rows: list, cols: list):
    """Write the in-memory resultset into the target staging table.

    Connects to the target database, drops and recreates the staging table
    via ensure_table(), then batch-inserts all rows using executemany().

    Parameters
    ----------
    rows : list[tuple]
        The resultset from query_runner.run() — a list of 9-element tuples.
    cols : list[str]
        Column names from cursor.description (used for logging; INSERT
        uses TARGET_COLS to guarantee column-name alignment).

    Returns
    -------
    int
        Number of rows inserted.

    Raises
    ------
    ValueError
        If rows is empty (defense-in-depth; query_runner.run() also guards).
    mysql.connector.Error
        On any database error during INSERT; triggers rollback.
    """
    # Defense-in-depth: query_runner.run() already guards against empty resultsets
    if not rows:
        raise ValueError(
            "[importer.run] Received empty resultset — aborting to protect target data."
        )

    try:
        conn = mysql.connector.connect(**tgt_creds())
    except mysql.connector.Error as e:
        print(f"[importer.run] Failed to connect to target database")
        raise

    try:
        cursor = conn.cursor()

        # DDL phase: drop + recreate table (implicit commit in MySQL)
        ensure_table(cursor)

        # DML phase: batch insert with executemany
        insert_sql = _build_insert_sql()

        cursor.executemany(insert_sql, rows)
        conn.commit()

        inserted = cursor.rowcount
        print(f"[importer.run] Inserted {inserted} rows into {TARGET_TABLE}")
        return inserted

    except mysql.connector.Error as e:
        print(f"[importer.run] INSERT failed — rolling back")
        conn.rollback()
        raise
    finally:
        conn.close()
