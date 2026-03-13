"""Command-line interface for the `uv` package."""

import argparse
import os
from . import query_runner, importer, validator


def main():
    parser = argparse.ArgumentParser(prog="uv")
    sub = parser.add_subparsers(dest="cmd")

    # query command
    q = sub.add_parser("query", help="run parameterized SQL against source DB")
    q.add_argument("--start", required=True,
                   help="start date YYYYMMDD")
    q.add_argument("--end", required=True,
                   help="end date YYYYMMDD")
    q.add_argument("--output", default="-", 
                   help="output CSV file path (\"-\" for stdout)")

    # validate command (later might read existing CSV)
    v = sub.add_parser("validate", help="validate a resultset CSV or run tests")
    v.add_argument("--input", default=None,
                   help="path to CSV to validate (optional)")

    # import command placeholder
    im = sub.add_parser("import", help="import CSV into target staging table")
    im.add_argument("--input", required=True,
                    help="path to CSV to load into target DB")

    args = parser.parse_args()
    if args.cmd == "query":
        # normalize dates to full timestamp strings
        start_ts = query_runner.to_timestamp(args.start, is_start=True)
        end_ts = query_runner.to_timestamp(args.end, is_start=False)
        query_runner.run(start_ts, end_ts, output=args.output)
    elif args.cmd == "validate":
        validator.run(input_path=args.input)
    elif args.cmd == "import":
        importer.run(input_path=args.input)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
