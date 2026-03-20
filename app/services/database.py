"""
DuckDB singleton — loaded once at startup, reused across requests.

The DOL Form 5500 table is expected to exist (or be created) as:
    dol_form5500   (from dol_form5500.parquet / CSV)
    naics_codes    (from naics_codes.csv)

If the parquet/CSV files are present, they are loaded on first startup.
"""

import logging
import os
from pathlib import Path

import duckdb

from app.tools.dol import normalise_company_name
from app.utils.db import fetchall_as_dicts  # re-exported for convenience

logger = logging.getLogger(__name__)

_conn: duckdb.DuckDBPyConnection | None = None


def get_conn() -> duckdb.DuckDBPyConnection:
    if _conn is None:
        raise RuntimeError("Database not initialised — call init_db() at startup")
    return _conn


def init_db() -> None:
    global _conn

    db_path = os.getenv("DUCKDB_PATH", "./data/static_data.duckdb")
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    logger.info("Opening DuckDB at %s", db_path)
    _conn = duckdb.connect(db_path)

    _ensure_dol_table()
    _ensure_naics_table()
    _ensure_batch_table()
    _ensure_review_rows_table()
    _ensure_enrichment_log_table()
    logger.info("DuckDB ready")


# ---------------------------------------------------------------------------
# Table bootstrap helpers
# ---------------------------------------------------------------------------

def _ensure_dol_table() -> None:
    conn = _conn
    tables = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
    if "dol_form5500" in tables:
        logger.info("dol_form5500 table exists")
        _ensure_normalised_column()
        return

    dol_path = os.getenv("DOL_DATA_PATH", "./data/dol_form5500.parquet")
    if Path(dol_path).exists():
        logger.info("Loading DOL data from %s", dol_path)
        ext = Path(dol_path).suffix.lower()
        if ext == ".parquet":
            conn.execute(f"CREATE TABLE dol_form5500 AS SELECT * FROM read_parquet('{dol_path}')")
        else:
            conn.execute(f"CREATE TABLE dol_form5500 AS SELECT * FROM read_csv_auto('{dol_path}')")
        _add_normalised_column()
    else:
        logger.warning(
            "DOL data file not found at %s — creating empty dol_form5500 table. "
            "Load data before running in production.",
            dol_path,
        )
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dol_form5500 (
                ack_id VARCHAR,
                ein VARCHAR,
                plan_name VARCHAR,
                sponsor_dfe_name VARCHAR,
                spons_dfe_pn VARCHAR,
                spons_state VARCHAR,
                business_code VARCHAR,
                broker_name VARCHAR,
                broker_ein VARCHAR,
                cpa_name VARCHAR,
                plan_admin_name VARCHAR,
                plan_admin_sign_name VARCHAR,
                tot_partcp_boy_cnt INTEGER,
                normalised_name VARCHAR
            )
        """)


def _ensure_normalised_column() -> None:
    """Add normalised_name column if missing (schema migration)."""
    conn = _conn
    # DESCRIBE returns rows where column_name is index 0
    cols = {r[0].lower() for r in conn.execute("DESCRIBE dol_form5500").fetchall()}
    if "normalised_name" not in cols:
        _add_normalised_column()


def _add_normalised_column() -> None:
    """Compute and store normalised_name for all DOL rows."""
    conn = _conn
    logger.info("Computing normalised_name for dol_form5500 ...")
    try:
        conn.execute("ALTER TABLE dol_form5500 ADD COLUMN IF NOT EXISTS normalised_name VARCHAR")
    except Exception:
        pass  # Already exists in some DuckDB versions

    rows = conn.execute("SELECT rowid, sponsor_dfe_name FROM dol_form5500").fetchall()
    for rowid, name in rows:
        normalised = normalise_company_name(name or "")
        conn.execute(
            "UPDATE dol_form5500 SET normalised_name = ? WHERE rowid = ?",
            [normalised, rowid],
        )
    logger.info("normalised_name populated for %d rows", len(rows))


def _ensure_naics_table() -> None:
    conn = _conn
    tables = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
    if "naics_codes" in tables:
        logger.info("naics_codes table exists")
        return

    naics_path = os.getenv("NAICS_DATA_PATH", "./data/naics_codes.csv")
    if Path(naics_path).exists():
        logger.info("Loading NAICS data from %s", naics_path)
        conn.execute(
            f"CREATE TABLE naics_codes AS SELECT * FROM read_csv_auto('{naics_path}')"
        )
    else:
        logger.warning(
            "NAICS CSV not found at %s — creating empty naics_codes table.",
            naics_path,
        )
        conn.execute("""
            CREATE TABLE IF NOT EXISTS naics_codes (
                naics_code VARCHAR,
                naics_title VARCHAR,
                naics_description VARCHAR
            )
        """)


def _ensure_batch_table() -> None:
    """Persistent review batch metadata store."""
    conn = _conn
    conn.execute("""
        CREATE TABLE IF NOT EXISTS review_batches (
            batch_id VARCHAR PRIMARY KEY,
            list_name VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_row_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            expires_at TIMESTAMP,
            total_rows INTEGER DEFAULT 0,
            auto_written INTEGER DEFAULT 0,
            review_count INTEGER DEFAULT 0,
            status VARCHAR DEFAULT 'collecting',
            email_sent BOOLEAN DEFAULT FALSE
        )
    """)


def _ensure_review_rows_table() -> None:
    """Pending review rows — one record per row routed to human review."""
    conn = _conn
    conn.execute("""
        CREATE TABLE IF NOT EXISTS review_rows (
            id VARCHAR PRIMARY KEY,
            batch_id VARCHAR NOT NULL,
            row_id VARCHAR,
            company_name VARCHAR,
            person_name VARCHAR,
            confidence_final DOUBLE,
            review_reason VARCHAR,
            enriched_fields JSON,
            suggested_action VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            decision VARCHAR DEFAULT 'pending'
        )
    """)


def _ensure_enrichment_log_table() -> None:
    """Append-only audit log (T4)."""
    conn = _conn
    conn.execute("""
        CREATE TABLE IF NOT EXISTS enrichment_log (
            id VARCHAR PRIMARY KEY,
            log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            source_table VARCHAR DEFAULT 'upload_processing',
            batch_id VARCHAR,
            list_name VARCHAR,
            archetype VARCHAR,
            phase_final VARCHAR,
            confidence_final DOUBLE,
            review_decision VARCHAR DEFAULT 'auto',
            static_source_used VARCHAR,
            sf_outcome VARCHAR
        )
    """)
