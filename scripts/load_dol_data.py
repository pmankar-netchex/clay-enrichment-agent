"""
Load a local DOL Form 5500 extract (Parquet or CSV) into DuckDB.

Usage:
    python scripts/load_dol_data.py [--source /path/to/dol.parquet]

The DOL EFAST2 Form 5500 dataset is published annually at:
  https://www.dol.gov/agencies/ebsa/researchers/analysis/form-5500-datasets

Download the latest year's Schedule A or full filing extract and
convert to Parquet (pandas: df.to_parquet('dol_form5500.parquet'))
before running this script.
"""

import argparse
import logging
import os
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Load DOL Form 5500 data into DuckDB")
    parser.add_argument(
        "--source",
        default=os.getenv("DOL_DATA_PATH", "./data/dol_form5500.parquet"),
        help="Path to DOL parquet or CSV file",
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
    from app.tools.dol import normalise_company_name

    Path(args.db).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(args.db)

    # Drop and recreate
    conn.execute("DROP TABLE IF EXISTS dol_form5500")
    ext = source.suffix.lower()
    if ext == ".parquet":
        conn.execute(f"CREATE TABLE dol_form5500 AS SELECT * FROM read_parquet('{source}')")
    else:
        conn.execute(f"CREATE TABLE dol_form5500 AS SELECT * FROM read_csv_auto('{source}')")

    count = conn.execute("SELECT COUNT(*) FROM dol_form5500").fetchone()[0]
    logger.info("Loaded %d rows into dol_form5500", count)

    # Add normalised_name column
    try:
        conn.execute("ALTER TABLE dol_form5500 ADD COLUMN normalised_name VARCHAR")
    except Exception:
        pass

    logger.info("Computing normalised_name for %d rows ...", count)
    rows = conn.execute("SELECT rowid, sponsor_dfe_name FROM dol_form5500").fetchall()
    batch = []
    for rowid, name in rows:
        normalised = normalise_company_name(name or "")
        batch.append((normalised, rowid))
        if len(batch) >= 5000:
            conn.executemany(
                "UPDATE dol_form5500 SET normalised_name = ? WHERE rowid = ?", batch
            )
            batch = []
    if batch:
        conn.executemany(
            "UPDATE dol_form5500 SET normalised_name = ? WHERE rowid = ?", batch
        )

    logger.info("Done. DuckDB saved to %s", args.db)
    conn.close()


if __name__ == "__main__":
    main()
