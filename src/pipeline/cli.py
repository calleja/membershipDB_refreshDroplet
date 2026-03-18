"""Command-line interface for the activity-pipeline package.

Provides two subcommands:

  sync     — Full pipeline: query source DB → validate → insert into target DB.
             Accepts --start, --end (YYYYMMDD) and optional --dry-run flag.

  validate — CI smoke test: queries source DB with a hardcoded date range
             and asserts at least one row is returned.  No arguments needed.
"""

import argparse
from . import query_runner, importer, validator


def main():
    """Parse CLI arguments and dispatch to the appropriate pipeline stage."""
    parser = argparse.ArgumentParser(prog="activity-pipeline")
    sub = parser.add_subparsers(dest="cmd")

    # ── sync: full pipeline (query → validate → import) ───────────────
    s = sub.add_parser(
        "sync",
        help="query source DB, validate resultset, insert into target DB",
    )
    s.add_argument(
        "--start", required=True,
        help="start date YYYYMMDD",
    )
    s.add_argument(
        "--end", required=True,
        help="end date YYYYMMDD",
    )
    s.add_argument(
        "--dry-run", action="store_true", default=False,
        help="query and validate only — skip INSERT into target DB",
    )

    # ── validate: CI smoke test against source DB ─────────────────────
    sub.add_parser(
        "validate",
        help="smoke-test: query source DB with hardcoded dates, assert rows > 0",
    )

    args = parser.parse_args()

    if args.cmd == "sync":
        # Convert YYYYMMDD → YYYYMMDDhhmmss timestamps
        start_ts = query_runner.to_timestamp(args.start, is_start=True)
        end_ts = query_runner.to_timestamp(args.end, is_start=False)

        # Step 1: query source database
        print(f"[sync] Querying source DB for range {args.start}–{args.end} …")
        try:
            cols, rows = query_runner.run(start_ts, end_ts)
        except Exception as e:
            print(f"[sync] Query phase failed: {e}")
            raise SystemExit(1)

        print(f"[sync] Received {len(rows)} rows, {len(cols)} columns")

        # Step 2: validate resultset shape
        try:
            validator.run(rows=rows, cols=cols)
        except Exception as e:
            print(f"[sync] Validation failed: {e}")
            raise SystemExit(1)

        print("[sync] Validation passed")

        # Step 3: insert into target (skip if --dry-run)
        if args.dry_run:
            print("[sync] --dry-run: skipping INSERT into target DB")
        else:
            try:
                count = importer.run(rows=rows, cols=cols)
                print(f"[sync] Done — {count} rows written to target")
            except Exception as e:
                print(f"[sync] Import phase failed: {e}")
                raise SystemExit(1)

    elif args.cmd == "validate":
        # CI smoke test — no arguments, queries source with hardcoded dates
        try:
            validator.run()
            print("[validate] Smoke test passed")
        except Exception as e:
            print(f"[validate] Smoke test failed: {e}")
            raise SystemExit(1)

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
