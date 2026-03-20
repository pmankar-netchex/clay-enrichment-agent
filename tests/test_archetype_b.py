"""
Phase 3 — Archetype B (person-only rows) tests.

Verifies that person-only rows are classified correctly, get phase=find_company,
and that static_lookup advances them to phase=enrich.
"""

import pytest
import duckdb
from unittest.mock import AsyncMock, patch, MagicMock

import app.services.database as db_module
from app.models.requests import ClassifyRequest, StaticLookupRequest
from app.routers.classify import classify_archetype, assign_phase, build_run_flags


# ---------------------------------------------------------------------------
# Archetype B classification
# ---------------------------------------------------------------------------

class TestArchetypeBClassification:
    def _req(self, **kwargs) -> ClassifyRequest:
        defaults = {
            "has_company": False, "has_person": True, "has_fein": False,
            "is_dol_native": False, "has_website": False, "has_intent_signal": False,
            "list_intent": "enrich_only",
            "skip_company_size": False, "skip_industry": False, "skip_revenue": False,
        }
        defaults.update(kwargs)
        return ClassifyRequest(**defaults)

    def test_person_only_is_archetype_b(self):
        req = self._req(has_person=True, has_company=False)
        assert classify_archetype(req) == "B"

    def test_archetype_b_gets_find_company_phase(self):
        assert assign_phase("B") == "find_company"

    def test_archetype_b_sets_run_person_lookup(self):
        req = self._req(has_person=True, has_company=False)
        flags = build_run_flags(req, "B", "low", None)
        assert flags["run_person_lookup"] is True

    def test_archetype_b_does_not_set_run_company_enrich(self):
        req = self._req(has_person=True, has_company=False)
        flags = build_run_flags(req, "B", "low", None)
        assert flags["run_company_enrich"] is False

    def test_person_plus_company_is_archetype_a_not_b(self):
        req = self._req(has_person=True, has_company=True)
        assert classify_archetype(req) != "B"

    def test_linkedin_only_without_company_is_b(self):
        req = ClassifyRequest(
            linkedin_person="https://linkedin.com/in/john-doe",
            has_company=False, has_person=True, has_fein=False,
            is_dol_native=False, has_website=False, has_intent_signal=False,
            list_intent="enrich_only",
            skip_company_size=False, skip_industry=False, skip_revenue=False,
        )
        assert classify_archetype(req) == "B"


# ---------------------------------------------------------------------------
# Static lookup response (Agent Call 2)
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def in_memory_db():
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE dol_form5500 (
            ack_id VARCHAR, ein VARCHAR, plan_name VARCHAR,
            sponsor_dfe_name VARCHAR, spons_dfe_pn VARCHAR, spons_state VARCHAR,
            business_code VARCHAR, broker_name VARCHAR, broker_ein VARCHAR,
            cpa_name VARCHAR, plan_admin_name VARCHAR, plan_admin_sign_name VARCHAR,
            tot_partcp_boy_cnt INTEGER, normalised_name VARCHAR
        )
    """)
    conn.execute("""
        CREATE TABLE naics_codes (
            naics_code VARCHAR, naics_title VARCHAR, naics_description VARCHAR
        )
    """)
    db_module._conn = conn
    yield conn
    db_module._conn = None


class TestStaticLookupAdvancesPhase:
    @pytest.mark.asyncio
    async def test_returns_phase_enrich(self):
        from app.routers.static_lookup import static_lookup
        req = StaticLookupRequest(
            company_name="Acme Hotel",
            state="CA",
            dba_risk="high",
            industry_detected="hotels_motels",
            archetype="B",
        )
        with patch("app.routers.static_lookup.find_in_dol", new_callable=AsyncMock,
                   return_value=MagicMock(matched=False, fields={})), \
             patch("app.routers.static_lookup.lookup_naics", new_callable=AsyncMock,
                   return_value=MagicMock(matched=False, fields={})), \
             patch("app.routers.static_lookup.resolve_legal_entity", new_callable=AsyncMock,
                   return_value=MagicMock(source="assumed_operating", matched=True,
                                         fields={"legal_entity_name": "Acme Hotel LLC",
                                                 "legal_entity_source": "assumed_operating"})):
            result = await static_lookup(req)

        assert result.phase == "enrich"
        assert result.static_done is True

    @pytest.mark.asyncio
    async def test_dol_results_populated_from_match(self):
        from app.routers.static_lookup import static_lookup
        from app.models.responses import StaticToolResult
        req = StaticLookupRequest(
            company_name="Best Western Hotel",
            state="TX",
            dba_risk="high",
            industry_detected="hotels_motels",
            archetype="B",
        )
        dol_mock = StaticToolResult(
            source="dol_form5500", match_key_used="name+state",
            confidence=0.82, matched=True,
            fields={
                "dol_sponsor_name": "Best Western Hotel TX",
                "dol_broker_name": "Broker LLC",
                "dol_broker_ein": None,
                "dol_cpa_name": None,
                "dol_plan_name": "Best Western 401k",
                "dol_plan_administrator": "John Smith",
                "dol_business_code": "7211",
                "dol_active_participants": 45,
                "dol_spons_state": "TX",
            },
        )
        with patch("app.routers.static_lookup.find_in_dol", new_callable=AsyncMock,
                   return_value=dol_mock), \
             patch("app.routers.static_lookup.lookup_naics", new_callable=AsyncMock,
                   return_value=MagicMock(matched=False, fields={})), \
             patch("app.routers.static_lookup.resolve_legal_entity", new_callable=AsyncMock,
                   return_value=MagicMock(source="assumed_operating", matched=True,
                                         fields={"legal_entity_name": None})):
            result = await static_lookup(req)

        assert result.dol_broker_name == "Broker LLC"
        assert result.dol_plan_administrator == "John Smith"
        assert result.dol_match_confidence == pytest.approx(0.82)
        assert result.phase == "enrich"

    @pytest.mark.asyncio
    async def test_timeout_returns_phase_enrich_with_nulls(self):
        """Even on tool timeout, phase must advance to 'enrich'."""
        import asyncio
        from app.routers.static_lookup import static_lookup
        req = StaticLookupRequest(
            company_name="Slow Company",
            dba_risk="low",
            industry_detected="unknown",
            archetype="B",
        )

        async def _slow(*args, **kwargs):
            await asyncio.sleep(10)  # will be cancelled by wait_for

        with patch("app.routers.static_lookup.asyncio.wait_for",
                   side_effect=asyncio.TimeoutError()):
            result = await static_lookup(req)

        assert result.phase == "enrich"
        assert result.static_done is True
        assert result.dol_broker_name is None
