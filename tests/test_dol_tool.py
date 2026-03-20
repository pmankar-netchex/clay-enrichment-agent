"""
Tests for the DOL Form 5500 tool — name normalisation and scoring.
"""

import pytest
from app.tools.dol import normalise_company_name


class TestNormaliseCompanyName:
    def test_strips_llc(self):
        assert "acme" == normalise_company_name("Acme LLC")

    def test_strips_inc(self):
        assert "acme" == normalise_company_name("Acme, Inc.")

    def test_strips_plan_suffix(self):
        result = normalise_company_name("Acme Corp 401k Plan")
        assert "plan" not in result
        assert "acme" in result

    def test_expands_mfg(self):
        result = normalise_company_name("Acme Mfg LLC")
        assert "manufacturing" in result

    def test_expands_svcs(self):
        result = normalise_company_name("Best Svcs Inc")
        assert "services" in result

    def test_lowercase(self):
        result = normalise_company_name("BEST WESTERN HOTEL")
        assert result == result.lower()

    def test_strips_punctuation(self):
        result = normalise_company_name("Smith & Jones, Co.")
        assert "&" not in result
        assert "," not in result

    def test_collapses_spaces(self):
        result = normalise_company_name("Big   Corp")
        assert "  " not in result

    def test_empty_string(self):
        assert normalise_company_name("") == ""
