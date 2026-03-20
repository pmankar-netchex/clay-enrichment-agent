"""
Batch review collection and flush service.

Design
------
- Each list upload shares a batch_id (Clay sets it in the SynthesiseRequest).
- As rows complete synthesis, review_pending rows are stored in DuckDB (review_rows table).
- A background flush loop runs every FLUSH_CHECK_INTERVAL seconds.
  When a batch's last_row_at is older than BATCH_IDLE_SECONDS, the batch is
  considered complete and a single review email is sent.
- Auto-written rows increment the batch's auto_written counter but are not stored
  in review_rows.

This gives one email per list upload regardless of how many rows need review.
"""

import asyncio
import json
import logging
import os
import uuid
from datetime import datetime, timedelta
from typing import Optional

from app.services.database import get_conn
from app.utils.db import fetchall_as_dicts
from app.services.email_service import send_review_email

logger = logging.getLogger(__name__)

# How long after the last row lands before we flush the batch email
BATCH_IDLE_SECONDS = int(os.getenv("BATCH_IDLE_SECONDS", "60"))
# How often the flush loop checks for ready batches
FLUSH_CHECK_INTERVAL = int(os.getenv("FLUSH_CHECK_INTERVAL", "30"))
REVIEW_BATCH_EXPIRY_DAYS = int(os.getenv("REVIEW_BATCH_EXPIRY_DAYS", "7"))

_flush_task: Optional[asyncio.Task] = None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def ensure_batch(batch_id: str, list_name: str) -> None:
    """Create or touch a batch record."""
    conn = get_conn()
    now = datetime.now()
    expires = now + timedelta(days=REVIEW_BATCH_EXPIRY_DAYS)
    conn.execute(
        """
        INSERT INTO review_batches (batch_id, list_name, created_at, last_row_at, expires_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (batch_id) DO UPDATE SET last_row_at = excluded.last_row_at
        """,
        [batch_id, list_name, now, now, expires],
    )


def record_auto_written(batch_id: str) -> None:
    """Increment auto_written and total_rows counters for a batch."""
    conn = get_conn()
    conn.execute(
        """
        UPDATE review_batches
        SET auto_written = auto_written + 1,
            total_rows = total_rows + 1,
            last_row_at = ?
        WHERE batch_id = ?
        """,
        [datetime.now(), batch_id],
    )


def store_review_row(
    batch_id: str,
    row_id: str,
    company_name: str,
    person_name: str,
    confidence_final: float,
    review_reason: str,
    enriched_fields: dict,
    suggested_action: str,
) -> None:
    """Persist a review-pending row and update batch counters."""
    conn = get_conn()
    now = datetime.now()
    conn.execute(
        """
        INSERT INTO review_rows
            (id, batch_id, row_id, company_name, person_name,
             confidence_final, review_reason, enriched_fields, suggested_action, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            str(uuid.uuid4()),
            batch_id,
            row_id,
            company_name,
            person_name,
            confidence_final,
            review_reason,
            json.dumps(enriched_fields),
            suggested_action,
            now,
        ],
    )
    conn.execute(
        """
        UPDATE review_batches
        SET review_count = review_count + 1,
            total_rows = total_rows + 1,
            last_row_at = ?
        WHERE batch_id = ?
        """,
        [now, batch_id],
    )


def append_enrichment_log(
    batch_id: str,
    list_name: str,
    archetype: str,
    phase_final: str,
    confidence_final: float,
    review_decision: str = "auto",
    sf_outcome: str = "pending",
) -> None:
    """Write a record to the enrichment_log (T4 equivalent)."""
    try:
        conn = get_conn()
        conn.execute(
            """
            INSERT INTO enrichment_log
                (id, batch_id, list_name, archetype, phase_final,
                 confidence_final, review_decision, sf_outcome)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                str(uuid.uuid4()),
                batch_id,
                list_name,
                archetype,
                phase_final,
                confidence_final,
                review_decision,
                sf_outcome,
            ],
        )
    except Exception as exc:
        logger.warning("enrichment_log write error: %s", exc)


def get_batch_data(batch_id: str) -> Optional[dict]:
    """Return batch metadata + review rows, or None if not found / expired."""
    conn = get_conn()
    batches = fetchall_as_dicts(
        conn.execute("SELECT * FROM review_batches WHERE batch_id = ?", [batch_id])
    )
    if not batches:
        return None

    batch = batches[0]
    expires_at = batch.get("expires_at")
    if expires_at and datetime.now() > expires_at:
        return None

    raw_rows = fetchall_as_dicts(conn.execute(
        "SELECT * FROM review_rows WHERE batch_id = ? ORDER BY created_at",
        [batch_id],
    ))

    rows = []
    for r in raw_rows:
        try:
            r["enriched_fields"] = json.loads(r.get("enriched_fields") or "{}")
        except (json.JSONDecodeError, TypeError):
            r["enriched_fields"] = {}
        rows.append(r)

    return {
        "batch_id": batch_id,
        "list_name": batch.get("list_name", ""),
        "total_rows": int(batch.get("total_rows", 0)),
        "auto_written": int(batch.get("auto_written", 0)),
        "review_count": int(batch.get("review_count", 0)),
        "status": batch.get("status", "collecting"),
        "email_sent": bool(batch.get("email_sent", False)),
        "created_at": str(batch.get("created_at", "")),
        "rows": rows,
    }


# ---------------------------------------------------------------------------
# Background flush loop
# ---------------------------------------------------------------------------

async def flush_ready_batches() -> int:
    """
    Find batches that have been idle for BATCH_IDLE_SECONDS and haven't had
    an email sent yet. Send the review email and mark them flushed.

    Returns number of batches flushed.
    """
    conn = get_conn()
    cutoff = datetime.now() - timedelta(seconds=BATCH_IDLE_SECONDS)

    ready = fetchall_as_dicts(conn.execute(
        """
        SELECT batch_id FROM review_batches
        WHERE email_sent = FALSE
          AND review_count > 0
          AND last_row_at < ?
          AND status = 'collecting'
        """,
        [cutoff],
    ))

    flushed = 0
    for item in ready:
        batch_id = item["batch_id"]
        try:
            data = get_batch_data(batch_id)
            if not data:
                continue

            await send_review_email(data)

            conn.execute(
                "UPDATE review_batches SET email_sent = TRUE, status = 'emailed' WHERE batch_id = ?",
                [batch_id],
            )
            logger.info(
                "Flushed batch %s: %d review rows, %d auto-written",
                batch_id,
                data["review_count"],
                data["auto_written"],
            )
            flushed += 1
        except Exception as exc:
            logger.error("Flush error for batch %s: %s", batch_id, exc)

    return flushed


async def _flush_loop() -> None:
    """Background coroutine — periodically flush ready batches."""
    logger.info(
        "Batch flush loop started (idle_seconds=%d, check_interval=%d)",
        BATCH_IDLE_SECONDS,
        FLUSH_CHECK_INTERVAL,
    )
    while True:
        await asyncio.sleep(FLUSH_CHECK_INTERVAL)
        try:
            n = await flush_ready_batches()
            if n:
                logger.info("Flush loop: %d batch(es) emailed", n)
        except Exception as exc:
            logger.error("Flush loop error: %s", exc)


def start_flush_loop() -> None:
    """Start the background flush task (call from FastAPI lifespan)."""
    global _flush_task
    if _flush_task is None or _flush_task.done():
        _flush_task = asyncio.create_task(_flush_loop())
        logger.info("Batch flush loop task created")


def stop_flush_loop() -> None:
    """Cancel the flush task on shutdown."""
    global _flush_task
    if _flush_task and not _flush_task.done():
        _flush_task.cancel()
        logger.info("Batch flush loop stopped")
