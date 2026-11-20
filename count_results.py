#!/usr/bin/env python3
"""
Print the number of results in saved SPARQL result files.

Supported formats (by file extension):
  .srj / .json   SPARQL 1.1 Query Results JSON (application/sparql-results+json)
  .csv           SPARQL 1.1 Query Results CSV (header row + one row per result)

JSON files are read as a stream, so large files do not need much memory. An ASK
result counts as 1 row.

Usage:
  python3 count_results.py result.srj [more files ...]
"""
import argparse
import csv
import os
import sys

sys.dont_write_bytecode = True  # no __pycache__ in the repo from the import below
from update_query_catalogue import count_sparql_json  # noqa: E402  (streaming row counter)


def count_json(path):
    with open(path, "rb") as f:
        rows, _ = count_sparql_json(f, float("inf"))
    return rows


def count_csv(path):
    # csv handles quoted values that contain newlines; the first row is the header
    with open(path, newline="", encoding="utf-8") as f:
        rows = sum(1 for _ in csv.reader(f)) - 1
    return max(rows, 0)


COUNTERS = {".srj": count_json, ".json": count_json, ".csv": count_csv}


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("files", nargs="+", help=".srj, .json or .csv result files")
    args = p.parse_args()

    status = 0
    for path in args.files:
        ext = os.path.splitext(path)[1].lower()
        if ext not in COUNTERS:
            print(f"{path}: unsupported extension {ext!r} (use .srj, .json or .csv)", file=sys.stderr)
            status = 1
            continue
        try:
            rows = COUNTERS[ext](path)
        except (OSError, ValueError, csv.Error) as e:
            print(f"{path}: {e}", file=sys.stderr)
            status = 1
            continue
        print(f"{rows}\t{path}")
    sys.exit(status)


if __name__ == "__main__":
    main()
