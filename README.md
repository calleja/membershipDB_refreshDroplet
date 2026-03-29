# Run this project as a CLI:
- cd into the location of the package
> cd /opt/activity-pieline
- activate the venv:
> . venv/bin/activate
> activity-pipeline sync --start --end
> activity-pipeline sync --start 20250201 --end 20250228
# Full pipeline: query source CiviCRM → validate shape → INSERT into target DB
> activity-pipeline sync --start 20250201 --end 20250228

# install location
/opt/activity-pipeline/
- when copying new versions, ensure to only copy the contents of the refreshDroplet directory, and not the whole directory itself
> sudo cp -r /home/lcalleja/membership_db_git/membershipDB_refreshDroplet/. /opt/activity-pipeline
- if I need to remove previous versions, force it (this code MUST be double-checked):
> sudo rm -rf /opt/activity-pipeline/*

# Cron job in the future
# /etc/cron.d/activity-pipeline
0 3 1 * * appuser /opt/activity-pipeline/venv/bin/activity-pipeline sync \
    --start $(date -d "$(date +%Y-%m-01) -1 month" +%Y%m%d) \
    --end $(date -d "$(date +%Y-%m-01) -1 day" +%Y%m%d) \
    >> /var/log/activity-pipeline.log 2>&1


cli.main()
│
├─ 1. Parse args → cmd="sync", start="20220101", end="20221231"
│
├─ 2. query_runner.to_timestamp("20220101", True)  → "20220101000000"
│     query_runner.to_timestamp("20221231", False) → "20221231235959"
│
├─ 3. QUERY PHASE: query_runner.run("20220101000000", "20221231235959")
│     ├─ load_sql()  → reads sql/cte_select_activity_detail.sql
│     ├─ Interpolates %(start)s / %(end)s into the CTE query
│     ├─ Connects to SOURCE DB (SRC_DB_* env vars)
│     ├─ cursor.execute(formatted_sql)
│     ├─ cols = [column names from cursor.description]  → 9 strings
│     ├─ rows = cursor.fetchall()  → list of 9-element tuples
│     ├─ GUARD: if rows is empty → raise ValueError, abort
│     └─ Returns (cols, rows)
│
├─ 4. VALIDATE PHASE: validator.run(rows=rows, cols=cols)
│     ├─ Assert rows is a non-empty list
│     ├─ Assert every row is a tuple with exactly 9 elements
│     └─ Returns (no exception = passed)
│
├─ 5. IMPORT PHASE: importer.run(rows=rows, cols=cols)
│     │  (skipped if --dry-run)
│     ├─ GUARD: if rows is empty → raise ValueError
│     ├─ Connects to TARGET DB (TGT_DB_* env vars, database=membership_ard)
│     ├─ ensure_table(cursor)
│     │   ├─ DROP TABLE IF EXISTS selectActivityReport_temp
│     │   └─ CREATE TABLE selectActivityReport_temp (9 cols) ← implicit commit
│     ├─ cursor.executemany(INSERT INTO selectActivityReport_temp …, rows)
│     ├─ conn.commit()  ← on success
│     ├─ conn.rollback()  ← on exception
│     └─ Returns inserted row count
│
└─ 6. Print summary: "[sync] Done — N rows written to target"


activity-pipeline validate (CI smoke test)
cli.main()
├─ Parse args → cmd="validate", no arguments
└─ validator.run()  ← called with no args
   ├─ Connects to SOURCE DB
   ├─ Runs CTE query with hardcoded dates (Jan 1 2022)
   ├─ Assert rows > 0
   └─ Print "[validate] Smoke test passed"