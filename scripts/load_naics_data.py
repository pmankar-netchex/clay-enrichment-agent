"""
Script to load US Census NAICS code table into DuckDB.

Usage:
    python scripts/load_naics_data.py [--source /path/to/naics_codes.csv]

Download the NAICS 2022 dataset from:
  https://www.census.gov/naics/?58967?yearbck=2022

The CSV should have columns: naics_code, naics_title (at minimum).
"""

import argparse
import logging
import os
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Load NAICS codes into DuckDB")
    parser.add_argument(
        "--source",
        default=os.getenv("NAICS_DATA_PATH", "./data/naics_codes.csv"),
        help="Path to NAICS CSV file",
    )
    parser.add_argument(
        "--db",
        default=os.getenv("DUCKDB_PATH", "./data/static_data.duckdb"),
        help="Path to DuckDB database file",
    )
    args = parser.parse_args()

    source = Path(args.source)
    if not source.exists():
        logger.error("Source file not found: %s", source)
        sys.exit(1)

    import duckdb

    Path(args.db).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(args.db)

    conn.execute("DROP TABLE IF EXISTS naics_codes")
    conn.execute(f"CREATE TABLE naics_codes AS SELECT * FROM read_csv_auto('{source}')")

    count = conn.execute("SELECT COUNT(*) FROM naics_codes").fetchone()[0]
    logger.info("Loaded %d NAICS codes", count)
    conn.close()


if __name__ == "__main__":
    main()
