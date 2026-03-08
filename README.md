# membershipDB_refreshDroplet

Small Python package `uv` to run a parameterized activity query, validate results, and import into a target MySQL database.

## Usage

1. Copy `.env.example` to `.env` and fill in credentials.
2. Create a virtual environment and install:
   ```sh
   python3 -m venv venv
   . venv/bin/activate
   pip install -e .
   ```
3. Run query for a date range (YYYYMMDD) and write CSV:
   ```sh
   uv query --start 20240101 --end 20240131 --output results.csv
   ```
4. Validate with `uv validate --input results.csv` or simply `uv validate`.
5. Import with `uv import --input results.csv` (stubbed).

## Tests

```sh
pytest
```

## Notes

The SQL file `select_activity_details_parameterized.sql` is parameterized using
`%(start)s`/`%(end)s`.  The CLI accepts simple YYYYMMDD dates and expands them to
full timestamps.  The output path is configurable; pass `-` for stdout.


## To use VS Code to connect and test to the python package in the server:
How: VS Code's Remote - SSH extension lets you open the droplet's filesystem directly in VS Code as if it were local. You get the full IDE (editor, terminal, Test Explorer, debugger) running on the remote machine.

Install the Remote - SSH extension in VS Code.
Add your droplet: Cmd+Shift+P → Remote-SSH: Add New SSH Host → ssh user@<droplet-ip>.
Remote-SSH: Connect to Host → navigate to the project folder.
Install the Python extension on the remote (VS Code prompts you).
Create src/.env, then open the Testing panel (flask icon) — pytest tests will be discovered and runnable with a click. The debugger works too.
Challenge: Requires the droplet to have internet access to download VS Code Server on first connect (~100 MB). SSH key auth is strongly recommended over password auth.