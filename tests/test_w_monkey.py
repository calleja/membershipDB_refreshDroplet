
### What each file does

- **tests/sql/test_query.sql** — A simple SQL file you control (e.g., `SELECT 1`), used to override SQL_PATH.
- **`tests/test_query_runner.py`** — New test file with `monkeypatch` to override SQL_PATH, @pytest.mark.parametrize for multiple date ranges, and a custom marker.
- **`tests/conftest.py`** — Adds src to sys.path so from uv import query_runner resolves.
- **pytest.ini** — Registers custom markers to avoid warnings.

### Sketch of `test_query_runner.py` logic

```python
import os
import pytest
import mysql.connector
from uv import query_runner

CUSTOM_SQL = os.path.join(os.path.dirname(__file__), "sql", "test_query.sql")

@pytest.fixture(autouse=False)
def override_sql_path(monkeypatch):
    monkeypatch.setattr(query_runner, "SQL_PATH", CUSTOM_SQL)

@pytest.fixture(scope="module")
def src_conn():
    conn = mysql.connector.connect(...)  # use src_creds
    yield conn
    conn.close()

@pytest.mark.custom_sql
@pytest.mark.parametrize("start,end", [
    ("20220101000000", "20220101235959"),
    ("20230601000000", "20230601235959"),
])
def test_run_query_with_custom_sql(override_sql_path, src_conn, start, end):
    rows = query_runner.run_query(src_conn, start, end)
    assert isinstance(rows, list)