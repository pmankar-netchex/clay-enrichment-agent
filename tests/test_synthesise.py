"""
Integration tests for the /synthesise endpoint and batch review flow.
Uses an in-memory DuckDB so no file system state leaks between tests.
"""

import asyncio
import json
import pytest
import duckdb
from unittest.mock import AsyncMock, MagicMock, patch

import app.services.database as db_module
import app.services.batch_review as batch_module
from app.models.requests import SynthesiseRequest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def in_memory_db(tmp_path):
    """Replace the global DuckDB connection with an in-memory instance."""
    conn = duckdb.connect(":memory:")
    # Bootstrap all tables
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


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _make_req(**kwargs) -> SynthesiseRequest:
    defaults = {
        "archetype": "A",
        "dba_risk": "low",
        "batch_id": "test-batch-001",
        "list_name": "test_list.csv",
        "row_id": "row-001",
        "company_name": "Acme Corp",
        "enriched_company_name": "Acme Corp",
        "enriched_domain": "acme.com",
        "enriched_industry": "Manufacturing",
        "enriched_email": "ceo@acme.com",
        "enriched_first_name": "Jane",
        "enriched_last_name": "Doe",
        "enriched_state": "CA",
        "clay_company_confidence": 0.9,
        "dol_match_confidence": 0.95,
        "dol_spons_state": "CA",
    }
    defaults.update(kwargs)
    return SynthesiseRequest(**defaults)


# ---------------------------------------------------------------------------
# Batch grouping tests
# ---------------------------------------------------------------------------

class TestBatchGrouping:
    def test_shared_batch_id_groups_rows(self):
        """Two rows with the same batch_id share one batch record."""
        batch_module.ensure_batch("batch-abc", "test.csv")
        batch_module.ensure_batch("batch-abc", "test.csv")  # touch again

        conn = db_module.get_conn()
        count = conn.execute(
            "SELECT COUNT(*) FROM review_batches WHERE batch_id = 'batch-abc'"
        ).fetchone()[0]
        assert count == 1

    def test_auto_written_increments_counter(self):
        batch_module.ensure_batch("batch-abc", "test.csv")
        batch_module.record_auto_written("batch-abc")
        batch_module.record_auto_written("batch-abc")

        conn = db_module.get_conn()
        row = conn.execute(
            "SELECT auto_written, total_rows FROM review_batches WHERE batch_id = 'batch-abc'"
        ).fetchone()
        assert row[0] == 2  # auto_written
        assert row[1] == 2  # total_rows

    def test_review_row_stored(self):
        batch_module.ensure_batch("batch-abc", "test.csv")
        batch_module.store_review_row(
            batch_id="batch-abc",
            row_id="r1",
            company_name="Acme",
            person_name="Jane Doe",
            confidence_final=0.65,
            review_reason="Low confidence",
            enriched_fields={"enriched_email": "jane@acme.com"},
            suggested_action="approve",
        )

        conn = db_module.get_conn()
        count = conn.execute(
            "SELECT COUNT(*) FROM review_rows WHERE batch_id = 'batch-abc'"
        ).fetchone()[0]
        assert count == 1

        batch_count = conn.execute(
            "SELECT review_count, total_rows FROM review_batches WHERE batch_id = 'batch-abc'"
        ).fetchone()
        assert batch_count[0] == 1  # review_count
        assert batch_count[1] == 1  # total_rows

    def test_different_batches_stay_separate(self):
        batch_module.ensure_batch("batch-1", "list1.csv")
        batch_module.ensure_batch("batch-2", "list2.csv")
        batch_module.store_review_row("batch-1", "r1", "Acme", "", 0.6, "low", {}, "approve")
        batch_module.store_review_row("batch-2", "r2", "Beta", "", 0.5, "low", {}, "review")

        conn = db_module.get_conn()
        assert conn.execute(
            "SELECT COUNT(*) FROM review_rows WHERE batch_id = 'batch-1'"
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM review_rows WHERE batch_id = 'batch-2'"
        ).fetchone()[0] == 1


# ---------------------------------------------------------------------------
# Confidence routing tests
# ---------------------------------------------------------------------------

class TestConfidenceRouting:
    def test_high_confidence_sets_run_sf(self):
        """FEIN match + high Clay confidence → auto-write."""
        req = _make_req(
            clay_company_confidence=0.95,
            dol_match_confidence=0.98,
            dol_spons_state="CA",
            enriched_state="CA",
        )
        from app.services.confidence import compute_confidence, cross_source_agreement, record_completeness
        enriched = req.model_dump()
        signals = {
            "fein_match": 1.0,
            "clay_company_confidence": 0.95,
            "cross_source_agreement": cross_source_agreement(enriched, {
                "dol_spons_state": "CA", "dol_business_code": None, "dol_active_participants": None
            }),
            "completeness": record_completeness(enriched),
            "dol_fuzzy_confidence": 0.98,
        }
        score, _ = compute_confidence(signals)
        assert score >= 0.80, f"Expected auto-write threshold, got {score}"

    def test_low_confidence_goes_to_review(self):
        """No FEIN, low Clay confidence → review_pending."""
        from app.services.confidence import compute_confidence, cross_source_agreement, record_completeness
        req = _make_req(
            clay_company_confidence=0.4,
            dol_match_confidence=0.55,
        )
        enriched = req.model_dump()
        signals = {
            "fein_match": 0.0,
            "clay_company_confidence": 0.4,
            "cross_source_agreement": 0.5,
            "completeness": record_completeness(enriched),
            "dol_fuzzy_confidence": 0.55,
        }
        score, _ = compute_confidence(signals)
        assert score < 0.80, f"Expected review threshold, got {score}"


# ---------------------------------------------------------------------------
# Batch data retrieval
# ---------------------------------------------------------------------------

class TestGetBatchData:
    def test_returns_none_for_missing_batch(self):
        result = batch_module.get_batch_data("nonexistent")
        assert result is None

    def test_returns_batch_with_rows(self):
        batch_module.ensure_batch("batch-xyz", "my_list.csv")
        batch_module.store_review_row(
            "batch-xyz", "r1", "Hotel Alpha", "John Smith",
            0.62, "Low DOL confidence", {"enriched_email": "j@hotel.com"}, "approve"
        )

        data = batch_module.get_batch_data("batch-xyz")
        assert data is not None
        assert data["batch_id"] == "batch-xyz"
        assert data["review_count"] == 1
        assert len(data["rows"]) == 1
        assert data["rows"][0]["company_name"] == "Hotel Alpha"


# ---------------------------------------------------------------------------
# Enrichment log
# ---------------------------------------------------------------------------

class TestEnrichmentLog:
    def test_log_appended(self):
        batch_module.append_enrichment_log(
            batch_id="batch-log",
            list_name="test.csv",
            archetype="A",
            phase_final="scored",
            confidence_final=0.85,
            review_decision="auto",
            sf_outcome="queued",
        )

        conn = db_module.get_conn()
        count = conn.execute(
            "SELECT COUNT(*) FROM enrichment_log WHERE batch_id = 'batch-log'"
        ).fetchone()[0]
        assert count == 1

    def test_multiple_rows_same_batch(self):
        for i in range(3):
            batch_module.append_enrichment_log(
                batch_id="batch-multi",
                list_name="test.csv",
                archetype="A",
                phase_final="scored",
                confidence_final=0.9,
            )

        conn = db_module.get_conn()
        count = conn.execute(
            "SELECT COUNT(*) FROM enrichment_log WHERE batch_id = 'batch-multi'"
        ).fetchone()[0]
        assert count == 3


# ---------------------------------------------------------------------------
# Flush logic
# ---------------------------------------------------------------------------

class TestFlushLogic:
    @pytest.mark.asyncio
    async def test_flush_sends_email_for_idle_batch(self):
        batch_module.ensure_batch("batch-flush", "flush_test.csv")
        batch_module.store_review_row(
            "batch-flush", "r1", "Acme", "Jane", 0.65,
            "Low confidence", {}, "approve"
        )

        # Force idle by backdating last_row_at
        conn = db_module.get_conn()
        conn.execute(
            "UPDATE review_batches SET last_row_at = '2000-01-01' WHERE batch_id = 'batch-flush'"
        )

        with patch(
            "app.services.batch_review.send_review_email",
            new_callable=AsyncMock,
            return_value=True,
        ) as mock_email:
            n = await batch_module.flush_ready_batches()

        assert n == 1
        mock_email.assert_called_once()
        call_payload = mock_email.call_args[0][0]
        assert call_payload["batch_id"] == "batch-flush"
        assert call_payload["review_count"] == 1
        assert len(call_payload["rows"]) == 1

    @pytest.mark.asyncio
    async def test_flush_skips_already_emailed_batch(self):
        batch_module.ensure_batch("batch-done", "done.csv")
        batch_module.store_review_row(
            "batch-done", "r1", "Corp", "", 0.5, "reason", {}, "review"
        )
        conn = db_module.get_conn()
        conn.execute(
            "UPDATE review_batches SET email_sent = TRUE, last_row_at = '2000-01-01' WHERE batch_id = 'batch-done'"
        )

        with patch("app.services.batch_review.send_review_email", new_callable=AsyncMock) as mock_email:
            n = await batch_module.flush_ready_batches()

        assert n == 0
        mock_email.assert_not_called()

    @pytest.mark.asyncio
    async def test_flush_skips_batch_without_review_rows(self):
        """A batch with only auto-written rows (review_count=0) should not be flushed."""
        batch_module.ensure_batch("batch-auto", "auto.csv")
        batch_module.record_auto_written("batch-auto")
        conn = db_module.get_conn()
        conn.execute(
            "UPDATE review_batches SET last_row_at = '2000-01-01' WHERE batch_id = 'batch-auto'"
        )

        with patch("app.services.batch_review.send_review_email", new_callable=AsyncMock) as mock_email:
            n = await batch_module.flush_ready_batches()

        assert n == 0
        mock_email.assert_not_called()
