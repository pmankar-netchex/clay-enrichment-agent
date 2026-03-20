"""
Phase 5 — Contact expansion tests.

Covers:
- run_people_expansion flag set correctly by list_intent
- persona_filter_json populated from industry detection
- DOL plan administrator → T2 free contact (spec 9.3)
- Clay API T2 write client (parse_admin_name, write_plan_admin_to_t2)
"""

import pytest
from unittest.mock import AsyncMock, patch

from app.models.requests import ClassifyRequest
from app.routers.classify import classify_archetype, build_run_flags
from app.services.clay_api import parse_admin_name, write_plan_admin_to_t2, insert_t2_contact


# ---------------------------------------------------------------------------
# run_people_expansion flag
# ---------------------------------------------------------------------------

class TestRunPeopleExpansion:
    def _req(self, **kwargs) -> ClassifyRequest:
        defaults = {
            "has_company": True, "has_person": True, "has_fein": False,
            "is_dol_native": False, "has_website": False, "has_intent_signal": False,
            "skip_company_size": False, "skip_industry": False, "skip_revenue": False,
        }
        defaults.update(kwargs)
        return ClassifyRequest(**defaults)

    def test_enrich_only_does_not_expand(self):
        req = self._req(list_intent="enrich_only")
        flags = build_run_flags(req, "A", "low", None)
        assert flags["run_people_expansion"] is False

    def test_enrich_then_expand_sets_flag(self):
        req = self._req(list_intent="enrich_then_expand")
        flags = build_run_flags(req, "A", "low", None)
        assert flags["run_people_expansion"] is True

    def test_expand_then_enrich_sets_flag(self):
        req = self._req(list_intent="expand_then_enrich")
        flags = build_run_flags(req, "A", "low", None)
        assert flags["run_people_expansion"] is True

    def test_company_only_always_searches_contacts(self):
        """Archetype C (company-only list) always wants contacts regardless of intent."""
        req = self._req(has_person=False, list_intent="enrich_only")
        flags = build_run_flags(req, "C", "low", None)
        assert flags["run_contacts_search"] is True

    def test_enrich_then_expand_also_searches_contacts(self):
        req = self._req(list_intent="enrich_then_expand")
        flags = build_run_flags(req, "A", "low", None)
        assert flags["run_contacts_search"] is True


# ---------------------------------------------------------------------------
# DOL plan admin name parsing
# ---------------------------------------------------------------------------

class TestParseAdminName:
    def test_full_name_splits_correctly(self):
        first, last = parse_admin_name("John Smith")
        assert first == "John"
        assert last == "Smith"

    def test_three_word_name_first_is_first(self):
        first, last = parse_admin_name("John Michael Smith")
        assert first == "John"
        assert last == "Michael Smith"

    def test_single_name(self):
        first, last = parse_admin_name("Madonna")
        assert first == "Madonna"
        assert last == ""

    def test_strips_extra_whitespace(self):
        first, last = parse_admin_name("  Jane   Doe  ")
        assert first == "Jane"
        assert last == "Doe"

    def test_empty_string(self):
        first, last = parse_admin_name("")
        assert first == ""
        assert last == ""


# ---------------------------------------------------------------------------
# write_plan_admin_to_t2 — free contact path (spec 9.3)
# ---------------------------------------------------------------------------

class TestWritePlanAdminToT2:
    @pytest.mark.asyncio
    async def test_calls_insert_t2_contact(self):
        with patch(
            "app.services.clay_api.insert_t2_contact",
            new_callable=AsyncMock,
            return_value=True,
        ) as mock_insert:
            result = await write_plan_admin_to_t2(
                plan_admin_name="Jane Smith",
                plan_admin_title="Benefits Manager",
                company_name="Best Western Hotel",
                company_website="bestwestern.com",
                naics_code="7211",
                dol_broker_name="Broker LLC",
                sf_account_id="001abc123",
            )

        assert result is True
        mock_insert.assert_called_once()
        contact = mock_insert.call_args[0][0]
        assert contact["first_name"] == "Jane"
        assert contact["last_name"] == "Smith"
        assert contact["job_title"] == "Benefits Manager"
        assert contact["company_name"] == "Best Western Hotel"
        assert contact["sf_account_id"] == "001abc123"
        assert contact["source"] == "dol_form5500_free"
        assert contact["archetype"] == "expansion_contact"

    @pytest.mark.asyncio
    async def test_defaults_title_when_missing(self):
        with patch(
            "app.services.clay_api.insert_t2_contact",
            new_callable=AsyncMock,
            return_value=True,
        ) as mock_insert:
            await write_plan_admin_to_t2(
                plan_admin_name="Bob Jones",
                plan_admin_title="",
                company_name="Corp",
                company_website="",
                naics_code="",
                dol_broker_name="",
                sf_account_id="",
            )

        contact = mock_insert.call_args[0][0]
        assert contact["job_title"] == "Plan Administrator"


# ---------------------------------------------------------------------------
# insert_t2_contact — no-op when API key not set
# ---------------------------------------------------------------------------

class TestInsertT2ContactNoKey:
    @pytest.mark.asyncio
    async def test_returns_true_when_no_api_key(self):
        """Without credentials, T2 write is non-fatal and returns True."""
        with patch.dict("os.environ", {"CLAY_API_KEY": "", "CLAY_T2_TABLE_ID": ""}):
            result = await insert_t2_contact({
                "first_name": "Jane", "last_name": "Doe", "company_name": "Corp",
            })
        assert result is True

    @pytest.mark.asyncio
    async def test_returns_false_on_http_error(self):
        """When Clay returns a non-201 status, insert_t2_contact returns False."""
        from unittest.mock import MagicMock
        contact = {
            "first_name": "Jane", "last_name": "Doe",
            "company_name": "Corp", "archetype": "expansion_contact",
        }
        mock_resp = AsyncMock()
        mock_resp.status_code = 422
        mock_resp.text = "Unprocessable Entity"

        mock_http = MagicMock()
        mock_http.post = AsyncMock(return_value=mock_resp)

        with patch("app.services.clay_api.CLAY_API_KEY", "test-key"), \
             patch("app.services.clay_api.CLAY_T2_TABLE_ID", "table-xyz"), \
             patch("app.services.clay_api.httpx.AsyncClient") as mock_cls:
            mock_cls.return_value.__aenter__ = AsyncMock(return_value=mock_http)
            mock_cls.return_value.__aexit__ = AsyncMock(return_value=False)
            result = await insert_t2_contact(contact)

        assert result is False
