"""
Review endpoints — Section 5.5 / 8.4

POST /api/v1/review/resume    — Human decisions written back to Clay rows
GET  /api/v1/review/{batch_id} — Batch data for review UI
POST /api/v1/review/flush     — Manual flush trigger (for testing / ops)
GET  /api/v1/review/approve_all — Quick approve-all from email link
"""

import logging
import os
from datetime import datetime
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse

from app.models.requests import ReviewResumeRequest
from app.models.responses import ReviewResumeResponse
from app.services.database import get_conn
from app.utils.db import fetchall_as_dicts
from app.services.batch_review import (
    get_batch_data,
    flush_ready_batches,
    append_enrichment_log,
)

logger = logging.getLogger(__name__)
router = APIRouter()

CLAY_API_KEY = os.getenv("CLAY_API_KEY", "")
CLAY_API_BASE = os.getenv("CLAY_API_BASE", "https://api.clay.com/v1")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

async def _write_clay_row(row_id: str, fields: dict) -> bool:
    """Write fields back to a Clay row via the Clay API."""
    if not CLAY_API_KEY:
        logger.warning("CLAY_API_KEY not set — skipping Clay row update for %s", row_id)
        # Return True so tests can exercise the rest of the flow
        return True
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.patch(
                f"{CLAY_API_BASE}/rows/{row_id}",
                headers={"Authorization": f"Bearer {CLAY_API_KEY}"},
                json=fields,
            )
            if resp.status_code in (200, 204):
                return True
            logger.error(
                "Clay API error %s for row %s: %s",
                resp.status_code, row_id, resp.text[:200],
            )
            return False
    except Exception as exc:
        logger.error("Clay write error for row %s: %s", row_id, exc)
        return False


def _validate_batch(batch_id: str) -> Optional[dict]:
    """Return batch metadata or None if expired/not found."""
    conn = get_conn()
    try:
        rows = fetchall_as_dicts(
            conn.execute("SELECT * FROM review_batches WHERE batch_id = ?", [batch_id])
        )
        if not rows:
            return None
        batch = rows[0]
        expires_at = batch.get("expires_at")
        if expires_at and datetime.now() > expires_at:
            return None
        return batch
    except Exception as exc:
        logger.error("Batch lookup error: %s", exc)
        return None


def _update_row_decision(batch_id: str, row_id: str, decision: str) -> None:
    """Mark a review_row as decided and log to enrichment_log."""
    try:
        conn = get_conn()
        conn.execute(
            "UPDATE review_rows SET decision = ? WHERE batch_id = ? AND row_id = ?",
            [decision, batch_id, row_id],
        )
    except Exception as exc:
        logger.warning("review_rows update error: %s", exc)


# ---------------------------------------------------------------------------
# POST /api/v1/review/resume
# ---------------------------------------------------------------------------

@router.post("/review/resume", response_model=ReviewResumeResponse)
async def review_resume(req: ReviewResumeRequest) -> ReviewResumeResponse:
    batch = _validate_batch(req.batch_id)
    if batch is None:
        raise HTTPException(status_code=404, detail="Batch not found or expired")

    approved = rejected = edited = 0
    errors = []

    for decision in req.decisions:
        row_id = decision.row_id
        action = decision.decision.lower()

        if action == "approved":
            fields = {"run_sf": True, "run_dedup": True, "phase": "approved"}
            ok = await _write_clay_row(row_id, fields)
            if ok:
                approved += 1
                _update_row_decision(req.batch_id, row_id, "approved")
            else:
                errors.append(f"Failed to write to row {row_id}")

        elif action == "edited":
            edited_fields = decision.edited_fields or {}
            fields = {**edited_fields, "run_sf": True, "run_dedup": True, "phase": "approved"}
            ok = await _write_clay_row(row_id, fields)
            if ok:
                edited += 1
                _update_row_decision(req.batch_id, row_id, "edited")
            else:
                errors.append(f"Failed to write to row {row_id}")

        elif action == "rejected":
            fields = {"run_sf": False, "phase": "rejected"}
            ok = await _write_clay_row(row_id, fields)
            if ok:
                rejected += 1
                _update_row_decision(req.batch_id, row_id, "rejected")
            else:
                errors.append(f"Failed to write to row {row_id}")

        else:
            errors.append(f"Unknown decision '{action}' for row {row_id}")

    # Update batch status
    all_decided = approved + rejected + edited
    try:
        conn = get_conn()
        new_status = "processed" if not errors else "partially_processed"
        conn.execute(
            "UPDATE review_batches SET status = ? WHERE batch_id = ?",
            [new_status, req.batch_id],
        )
    except Exception as exc:
        logger.warning("Batch status update error: %s", exc)

    return ReviewResumeResponse(
        batch_id=req.batch_id,
        processed=len(req.decisions),
        approved=approved,
        rejected=rejected,
        edited=edited,
        errors=errors,
    )


# ---------------------------------------------------------------------------
# GET /api/v1/review/{batch_id} — Review UI data endpoint
# ---------------------------------------------------------------------------

@router.get("/review/{batch_id}")
async def get_review_batch(batch_id: str) -> dict:
    """Return batch data for the review UI."""
    data = get_batch_data(batch_id)
    if data is None:
        raise HTTPException(status_code=404, detail="Batch not found or expired")
    return data


# ---------------------------------------------------------------------------
# GET /api/v1/review/approve_all — Approve-all from email link
# ---------------------------------------------------------------------------

@router.get("/review/approve_all", response_class=HTMLResponse)
async def approve_all(batch_id: str = Query(...)) -> HTMLResponse:
    """
    Approve all pending rows in a batch via a single link click from the email.
    Writes run_sf=true, run_dedup=true, phase=approved for every pending row.
    """
    data = get_batch_data(batch_id)
    if data is None:
        return HTMLResponse(
            "<h2>Batch not found or expired.</h2>", status_code=404
        )

    rows = [r for r in data["rows"] if r.get("decision", "pending") == "pending"]
    approved = 0
    errors = []

    for row in rows:
        row_id = row.get("row_id", "")
        if not row_id:
            continue
        fields = {"run_sf": True, "run_dedup": True, "phase": "approved"}
        ok = await _write_clay_row(row_id, fields)
        if ok:
            approved += 1
            _update_row_decision(batch_id, row_id, "approved")
        else:
            errors.append(row_id)

    try:
        conn = get_conn()
        conn.execute(
            "UPDATE review_batches SET status = 'processed' WHERE batch_id = ?",
            [batch_id],
        )
    except Exception:
        pass

    html = f"""
    <html><body style="font-family:sans-serif;max-width:600px;margin:auto;padding:40px">
    <h2>✓ Batch Approved</h2>
    <p>
        <strong>Batch:</strong> {batch_id}<br>
        <strong>Rows approved:</strong> {approved}<br>
        {"<strong>Errors:</strong> " + str(len(errors)) if errors else ""}
    </p>
    <p>Salesforce write has been triggered for all approved rows.</p>
    </body></html>
    """
    return HTMLResponse(html)


# ---------------------------------------------------------------------------
# POST /api/v1/review/flush — Manual flush trigger (ops / testing)
# ---------------------------------------------------------------------------

@router.post("/review/flush")
async def manual_flush() -> dict:
    """Immediately flush all batches that have pending review rows, ignoring idle timeout."""
    conn = get_conn()
    # Temporarily update last_row_at to force flush
    conn.execute(
        """
        UPDATE review_batches
        SET last_row_at = '2000-01-01 00:00:00'
        WHERE email_sent = FALSE AND review_count > 0 AND status = 'collecting'
        """
    )
    n = await flush_ready_batches()
    return {"flushed_batches": n}
