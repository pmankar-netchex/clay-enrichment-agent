"""
Tests for Agent Call 1 — classify endpoint.
Runs without a real DuckDB (mocked) to validate archetype + flag logic.
"""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from app.models.requests import ClassifyRequest
from app.routers.classify import classify_archetype, build_run_flags, assign_phase


# ---------------------------------------------------------------------------
# Archetype classification
# ---------------------------------------------------------------------------

class TestArchetypeClassification:
    def _req(self, **kwargs) -> ClassifyRequest:
        defaults = {
            "has_company": False, "has_person": False, "has_fein": False,
            "is_dol_native": False, "has_website": False, "has_intent_signal": False,
            "list_intent": "enrich_only",
            "skip_company_size": False, "skip_industry": False, "skip_revenue": False,
        }
        defaults.update(kwargs)
        return ClassifyRequest(**defaults)

    def test_archetype_a_company_and_person(self):
        req = self._req(has_company=True, has_person=True)
        assert classify_archetype(req) == "A"

    def test_archetype_b_person_only(self):
        req = self._req(has_person=True, has_company=False)
        assert classify_archetype(req) == "B"

    def test_archetype_c_company_only(self):
        req = self._req(has_company=True, has_person=False)
        assert classify_archetype(req) == "C"

    def test_fein_list(self):
        req = self._req(has_fein=True, has_company=False, has_person=False)
        assert classify_archetype(req) == "fein_list"

    def test_dol_native(self):
        req = self._req(is_dol_native=True, has_company=True, has_person=True)
        assert classify_archetype(req) == "dol_native"

    def test_sparse(self):
        req = self._req()
        assert classify_archetype(req) == "sparse"


# ---------------------------------------------------------------------------
# Phase assignment
# ---------------------------------------------------------------------------

class TestPhaseAssignment:
    def test_archetype_a_gets_strategy_set(self):
        assert assign_phase("A") == "strategy_set"

    def test_archetype_b_gets_find_company(self):
        assert assign_phase("B") == "find_company"

    def test_archetype_c_gets_strategy_set(self):
        assert assign_phase("C") == "strategy_set"

    def test_sparse_gets_skipped(self):
        assert assign_phase("sparse") == "skipped"

    def test_dol_native_gets_strategy_set(self):
        assert assign_phase("dol_native") == "strategy_set"


# ---------------------------------------------------------------------------
# Run flag construction
# ---------------------------------------------------------------------------

class TestRunFlags:
    def _req(self, **kwargs) -> ClassifyRequest:
        defaults = {
            "has_company": False, "has_person": False, "has_fein": False,
            "is_dol_native": False, "has_website": False, "has_intent_signal": False,
            "list_intent": "enrich_only",
            "skip_company_size": False, "skip_industry": False, "skip_revenue": False,
        }
        defaults.update(kwargs)
        return ClassifyRequest(**defaults)

    def test_archetype_a_runs_company_enrich(self):
        req = self._req(has_company=True, has_person=True)
        flags = build_run_flags(req, "A", "low", None)
        assert flags["run_company_enrich"] is True

    def test_archetype_b_does_not_run_company_enrich(self):
        req = self._req(has_company=False, has_person=True)
        flags = build_run_flags(req, "B", "low", None)
        assert flags["run_company_enrich"] is False
        assert flags["run_person_lookup"] is True

    def test_company_only_enables_contacts_search(self):
        req = self._req(has_company=True, list_intent="enrich_only")
        flags = build_run_flags(req, "C", "low", None)
        assert flags["run_contacts_search"] is True

    def test_expand_intent_enables_people_expansion(self):
        req = self._req(has_company=True, has_person=True, list_intent="enrich_then_expand")
        flags = build_run_flags(req, "A", "low", None)
        assert flags["run_people_expansion"] is True

    def test_high_dba_risk_no_legal_entity_sets_flag(self):
        req = self._req(has_company=True)
        flags = build_run_flags(req, "A", "high", None)
        assert flags["run_legal_entity"] is True

    def test_legal_entity_already_resolved_skips_flag(self):
        req = self._req(has_company=True)
        flags = build_run_flags(req, "A", "high", "Some Legal Corp")
        assert flags["run_legal_entity"] is False

    def test_dol_native_skips_company_enrich(self):
        req = self._req(has_company=True, is_dol_native=True)
        flags = build_run_flags(req, "dol_native", "low", None)
        assert flags["run_company_enrich"] is False


# ---------------------------------------------------------------------------
# Null normalisation
# ---------------------------------------------------------------------------

class TestNullNormalisation:
    def test_undefined_becomes_none(self):
        req = ClassifyRequest(
            company_name="undefined",
            has_company=False, has_person=False, has_fein=False,
            is_dol_native=False, has_website=False, has_intent_signal=False,
            list_intent="enrich_only",
            skip_company_size=False, skip_industry=False, skip_revenue=False,
        )
        assert req.company_name is None

    def test_null_string_becomes_none(self):
        req = ClassifyRequest(
            company_name="null",
            has_company=False, has_person=False, has_fein=False,
            is_dol_native=False, has_website=False, has_intent_signal=False,
            list_intent="enrich_only",
            skip_company_size=False, skip_industry=False, skip_revenue=False,
        )
        assert req.company_name is None

    def test_real_value_passes_through(self):
        req = ClassifyRequest(
            company_name="Acme Corp",
            has_company=True, has_person=False, has_fein=False,
            is_dol_native=False, has_website=False, has_intent_signal=False,
            list_intent="enrich_only",
            skip_company_size=False, skip_industry=False, skip_revenue=False,
        )
        assert req.company_name == "Acme Corp"
