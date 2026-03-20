"""
Tests for /api/v1/review/resume and batch review decision handling.
"""

import pytest
import duckdb
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import app.services.database as db_module
import app.services.batch_review as batch_module
from app.models.requests import ReviewResumeRequest, ReviewDecision
from app.routers.review import review_resume, _validate_batch, _update_row_decision


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def in_memory_db():
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE review_batches (
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
    conn.execute("""
        CREATE TABLE review_rows (
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
    conn.execute("""
        CREATE TABLE enrichment_log (
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
    db_module._conn = conn
    yield conn
    db_module._conn = None


def _seed_batch(batch_id: str, expired: bool = False) -> None:
    conn = db_module.get_conn()
    now = datetime.now(timezone.utc)
    expires = now - timedelta(days=1) if expired else now + timedelta(days=7)
    conn.execute(
        """
        INSERT INTO review_batches (batch_id, list_name, expires_at, review_count, status)
        VALUES (?, ?, ?, 2, 'emailed')
        """,
        [batch_id, "test.csv", expires],
    )
    # Add two review rows
    for i, row_id in enumerate(["row-1", "row-2"]):
        conn.execute(
            """
            INSERT INTO review_rows (id, batch_id, row_id, company_name, decision)
            VALUES (?, ?, ?, ?, 'pending')
            """,
            [f"uuid-{i}", batch_id, row_id, f"Company {i}"],
        )


# ---------------------------------------------------------------------------
# _validate_batch
# ---------------------------------------------------------------------------

class TestValidateBatch:
    def test_valid_batch_returned(self):
        _seed_batch("batch-valid")
        result = _validate_batch("batch-valid")
        assert result is not None
        assert result["batch_id"] == "batch-valid"

    def test_missing_batch_returns_none(self):
        result = _validate_batch("does-not-exist")
        assert result is None

    def test_expired_batch_returns_none(self):
        _seed_batch("batch-expired", expired=True)
        result = _validate_batch("batch-expired")
        assert result is None


# ---------------------------------------------------------------------------
# _update_row_decision
# ---------------------------------------------------------------------------

class TestUpdateRowDecision:
    def test_marks_row_as_approved(self):
        _seed_batch("batch-upd")
        _update_row_decision("batch-upd", "row-1", "approved")
        conn = db_module.get_conn()
        dec = conn.execute(
            "SELECT decision FROM review_rows WHERE batch_id = 'batch-upd' AND row_id = 'row-1'"
        ).fetchone()[0]
        assert dec == "approved"


# ---------------------------------------------------------------------------
# review_resume endpoint
# ---------------------------------------------------------------------------

class TestReviewResume:
    @pytest.mark.asyncio
    async def test_approve_decision(self):
        _seed_batch("batch-resume")
        req = ReviewResumeRequest(
            batch_id="batch-resume",
            decisions=[ReviewDecision(row_id="row-1", decision="approved")],
        )
        with patch("app.routers.review._write_clay_row", new_callable=AsyncMock, return_value=True):
            result = await review_resume(req)

        assert result.approved == 1
        assert result.rejected == 0
        assert result.edited == 0
        assert result.errors == []

    @pytest.mark.asyncio
    async def test_reject_decision(self):
        _seed_batch("batch-reject")
        req = ReviewResumeRequest(
            batch_id="batch-reject",
            decisions=[ReviewDecision(row_id="row-1", decision="rejected")],
        )
        with patch("app.routers.review._write_clay_row", new_callable=AsyncMock, return_value=True):
            result = await review_resume(req)

        assert result.rejected == 1

    @pytest.mark.asyncio
    async def test_edited_decision_merges_fields(self):
        _seed_batch("batch-edit")
        req = ReviewResumeRequest(
            batch_id="batch-edit",
            decisions=[ReviewDecision(
                row_id="row-1",
                decision="edited",
                edited_fields={"enriched_company_name": "Corrected Corp"},
            )],
        )
        captured = {}

        async def mock_write(row_id, fields):
            captured.update(fields)
            return True

        with patch("app.routers.review._write_clay_row", side_effect=mock_write):
            result = await review_resume(req)

        assert result.edited == 1
        assert captured.get("enriched_company_name") == "Corrected Corp"
        assert captured.get("run_sf") is True
        assert captured.get("phase") == "approved"

    @pytest.mark.asyncio
    async def test_expired_batch_raises_404(self):
        from fastapi import HTTPException
        _seed_batch("batch-exp", expired=True)
        req = ReviewResumeRequest(
            batch_id="batch-exp",
            decisions=[ReviewDecision(row_id="row-1", decision="approved")],
        )
        with pytest.raises(HTTPException) as exc:
            await review_resume(req)
        assert exc.value.status_code == 404

    @pytest.mark.asyncio
    async def test_mixed_decisions(self):
        _seed_batch("batch-mixed")
        req = ReviewResumeRequest(
            batch_id="batch-mixed",
            decisions=[
                ReviewDecision(row_id="row-1", decision="approved"),
                ReviewDecision(row_id="row-2", decision="rejected"),
            ],
        )
        with patch("app.routers.review._write_clay_row", new_callable=AsyncMock, return_value=True):
            result = await review_resume(req)

        assert result.processed == 2
        assert result.approved == 1
        assert result.rejected == 1
        assert result.errors == []

    @pytest.mark.asyncio
    async def test_clay_write_failure_recorded_in_errors(self):
        _seed_batch("batch-fail")
        req = ReviewResumeRequest(
            batch_id="batch-fail",
            decisions=[ReviewDecision(row_id="row-1", decision="approved")],
        )
        with patch("app.routers.review._write_clay_row", new_callable=AsyncMock, return_value=False):
            result = await review_resume(req)

        assert result.approved == 0
        assert len(result.errors) == 1

    @pytest.mark.asyncio
    async def test_unknown_decision_recorded_in_errors(self):
        _seed_batch("batch-unk")
        req = ReviewResumeRequest(
            batch_id="batch-unk",
            decisions=[ReviewDecision(row_id="row-1", decision="maybe")],
        )
        with patch("app.routers.review._write_clay_row", new_callable=AsyncMock, return_value=True):
            result = await review_resume(req)

        assert len(result.errors) == 1
        assert "maybe" in result.errors[0]


# ---------------------------------------------------------------------------
# get_batch_data
# ---------------------------------------------------------------------------

class TestGetBatchData:
    def test_returns_rows(self):
        _seed_batch("batch-data")
        data = batch_module.get_batch_data("batch-data")
        assert data is not None
        assert len(data["rows"]) == 2

    def test_enriched_fields_parsed_as_dict(self):
        import json
        _seed_batch("batch-fields")
        conn = db_module.get_conn()
        conn.execute(
            "UPDATE review_rows SET enriched_fields = ? WHERE batch_id = 'batch-fields' AND row_id = 'row-1'",
            [json.dumps({"enriched_email": "a@b.com"})],
        )
        data = batch_module.get_batch_data("batch-fields")
        row1 = next(r for r in data["rows"] if r["row_id"] == "row-1")
        assert isinstance(row1["enriched_fields"], dict)
        assert row1["enriched_fields"]["enriched_email"] == "a@b.com"
