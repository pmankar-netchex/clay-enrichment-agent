"""
Phase 6 — Legal entity resolution tests.

Covers all three decision tree paths:
  1. high DBA + website     → claygent_pending (run_legal_entity=True flag)
  2. high DBA + no website  → SOS web search
  3. low/medium DBA risk    → assumed operating name
"""

import pytest
from unittest.mock import AsyncMock, patch

from app.tools.legal_entity import (
    build_sos_query,
    resolve_legal_entity,
    _extract_legal_name,
)


# ---------------------------------------------------------------------------
# build_sos_query
# ---------------------------------------------------------------------------

class TestBuildSosQuery:
    def test_includes_company_name(self):
        query = build_sos_query("Best Western Hotel", "TX")
        assert "Best Western Hotel" in query

    def test_includes_state(self):
        query = build_sos_query("Acme Corp", "CA")
        assert "CA" in query or "ca" in query.lower()

    def test_targets_sos_domain(self):
        query = build_sos_query("Acme Corp", "CA")
        assert "sos" in query.lower() or "secretary of state" in query.lower()


# ---------------------------------------------------------------------------
# _extract_legal_name regex
# ---------------------------------------------------------------------------

class TestExtractLegalName:
    def test_extracts_llc_name(self):
        text = "The registered entity is Best Western Hotels LLC operating in Texas."
        result = _extract_legal_name(text, "Best Western")
        assert result is not None
        assert "Best Western" in result
        assert "LLC" in result

    def test_extracts_inc_name(self):
        text = "Ford Motor Company Inc is the registered name."
        result = _extract_legal_name(text, "Ford")
        assert result is not None
        assert "Ford" in result

    def test_returns_none_when_no_match(self):
        text = "No legal entity found in this text."
        result = _extract_legal_name(text, "Acme")
        assert result is None

    def test_returns_none_for_empty_text(self):
        result = _extract_legal_name("", "Acme")
        assert result is None


# ---------------------------------------------------------------------------
# resolve_legal_entity — Path 1: claygent_pending
# ---------------------------------------------------------------------------

class TestResolveLegalEntityClaygentPending:
    @pytest.mark.asyncio
    async def test_high_dba_with_website_defers_to_claygent(self):
        result = await resolve_legal_entity(
            company_name="Best Western Hotel",
            state="TX",
            website="bestwestern.com",
            dba_risk="high",
        )
        assert result.source == "claygent_pending"
        assert result.matched is False
        assert result.fields.get("run_legal_entity") is True

    @pytest.mark.asyncio
    async def test_medium_dba_with_website_does_not_defer_to_claygent(self):
        """Medium DBA risk should not trigger Claygent — path 3 instead."""
        result = await resolve_legal_entity(
            company_name="Downtown Bowling",
            state="TX",
            website="bowling.com",
            dba_risk="medium",
        )
        assert result.source != "claygent_pending"


# ---------------------------------------------------------------------------
# resolve_legal_entity — Path 2: SOS web search (high DBA, no website)
# ---------------------------------------------------------------------------

class TestResolveLegalEntityWebSearch:
    @pytest.mark.asyncio
    async def test_high_dba_no_website_attempts_sos_search(self):
        with patch(
            "app.tools.legal_entity._web_search",
            new_callable=AsyncMock,
            return_value="Best Western Hotels LLC is registered in Texas.",
        ):
            result = await resolve_legal_entity(
                company_name="Best Western Hotel",
                state="TX",
                website=None,
                dba_risk="high",
            )

        assert result.source == "agent_web_search"

    @pytest.mark.asyncio
    async def test_successful_sos_search_returns_legal_name(self):
        with patch(
            "app.tools.legal_entity._web_search",
            new_callable=AsyncMock,
            return_value="The entity Best Western Hotels LLC is registered.",
        ):
            result = await resolve_legal_entity(
                company_name="Best Western Hotels",
                state="TX",
                website=None,
                dba_risk="high",
            )

        assert result.matched is True
        assert result.confidence > 0.0
        assert result.fields.get("legal_entity_name") is not None

    @pytest.mark.asyncio
    async def test_failed_sos_search_returns_zero_confidence(self):
        with patch(
            "app.tools.legal_entity._web_search",
            new_callable=AsyncMock,
            return_value=None,
        ):
            result = await resolve_legal_entity(
                company_name="Unknown Hotel",
                state="TX",
                website=None,
                dba_risk="high",
            )

        assert result.confidence == 0.0
        assert result.matched is False

    @pytest.mark.asyncio
    async def test_web_search_error_handled_gracefully(self):
        with patch(
            "app.tools.legal_entity._web_search",
            new_callable=AsyncMock,
            side_effect=Exception("Network error"),
        ):
            # Should not raise — error is caught internally
            result = await resolve_legal_entity(
                company_name="Broken Hotel",
                state="TX",
                website=None,
                dba_risk="high",
            )
        # Returns zero-confidence result, not an exception
        assert result.confidence == 0.0


# ---------------------------------------------------------------------------
# resolve_legal_entity — Path 3: assumed operating name
# ---------------------------------------------------------------------------

class TestResolveLegalEntityAssumed:
    @pytest.mark.asyncio
    async def test_low_dba_risk_returns_assumed_operating(self):
        result = await resolve_legal_entity(
            company_name="Acme Manufacturing",
            state="OH",
            website="acme.com",
            dba_risk="low",
        )
        assert result.source == "assumed_operating"
        assert result.matched is True
        assert result.confidence == 0.85
        assert result.fields.get("legal_entity_name") == "Acme Manufacturing"

    @pytest.mark.asyncio
    async def test_medium_dba_risk_returns_assumed_operating(self):
        result = await resolve_legal_entity(
            company_name="Downtown Gym",
            state="CA",
            website=None,
            dba_risk="medium",
        )
        assert result.source == "assumed_operating"
        assert result.fields.get("legal_entity_name") == "Downtown Gym"
        assert result.fields.get("legal_entity_source") == "assumed_operating"

    @pytest.mark.asyncio
    async def test_assumed_confidence_is_0_85(self):
        result = await resolve_legal_entity(
            company_name="Any Corp",
            state="NY",
            website=None,
            dba_risk="low",
        )
        assert result.confidence == pytest.approx(0.85)


# ---------------------------------------------------------------------------
# run_legal_entity flag wiring in classify
# ---------------------------------------------------------------------------

class TestRunLegalEntityFlag:
    def _req(self, **kwargs):
        from app.models.requests import ClassifyRequest
        defaults = {
            "has_company": True, "has_person": False, "has_fein": False,
            "is_dol_native": False, "has_website": True, "has_intent_signal": False,
            "list_intent": "enrich_only",
            "skip_company_size": False, "skip_industry": False, "skip_revenue": False,
        }
        defaults.update(kwargs)
        return ClassifyRequest(**defaults)

    def test_high_dba_no_legal_entity_sets_flag(self):
        from app.routers.classify import build_run_flags
        req = self._req(has_company=True)
        flags = build_run_flags(req, "A", "high", None)
        assert flags["run_legal_entity"] is True

    def test_legal_entity_resolved_clears_flag(self):
        from app.routers.classify import build_run_flags
        req = self._req(has_company=True)
        flags = build_run_flags(req, "A", "high", "Best Western Hotels LLC")
        assert flags["run_legal_entity"] is False

    def test_low_dba_risk_does_not_set_flag(self):
        from app.routers.classify import build_run_flags
        req = self._req(has_company=True)
        flags = build_run_flags(req, "A", "low", None)
        assert flags["run_legal_entity"] is False
