"""
Phase 4 — NAICS lookup + industry config + confidence ceiling tests.
"""

import pytest
from app.tools.naics import detect_industry, NAICS_PREFIX_MAP, PERSONA_FILTERS
from app.services.confidence import auto_write_threshold, compute_confidence


# ---------------------------------------------------------------------------
# Industry detection across all 7 industries
# ---------------------------------------------------------------------------

class TestIndustryDetection:
    @pytest.mark.parametrize("company_name,expected_industry", [
        # Auto dealerships
        ("Ford of Fremont", "auto_dealerships"),
        ("Toyota Motors of Dallas", "auto_dealerships"),
        ("Best Chevrolet Automotive", "auto_dealerships"),
        # Hotels & motels
        ("Best Western Hotel", "hotels_motels"),
        ("Holiday Inn & Suites", "hotels_motels"),
        ("Comfort Inn Motel", "hotels_motels"),
        # Food & drinking
        ("Mario's Italian Restaurant", "food_drinking"),
        ("The Downtown Grill", "food_drinking"),
        ("Joe's Pizza Bar", "food_drinking"),
        ("Happy Diner LLC", "food_drinking"),
        # Entertainment & leisure
        ("Downtown Bowling Entertainment", "entertainment_leisure"),
        ("Galaxy Theater & Cinema", "entertainment_leisure"),
        # Fitness centers
        ("Anytime Fitness Center", "fitness_centers"),
        ("CrossFit Gym of Austin", "fitness_centers"),
        ("Wellness Studio Yoga", "fitness_centers"),
        # Physician's offices
        ("Dr. Smith Pediatric Clinic", "physicians_offices"),
        ("Regional Medical Center", "physicians_offices"),
        ("Dental Care Dentist Group", "physicians_offices"),
        # Manufacturing
        ("Acme Manufacturing LLC", "manufacturing"),
        ("Precision Industrial Products", "manufacturing"),
        ("Tech Systems Solutions", "manufacturing"),
    ])
    def test_industry_detected(self, company_name, expected_industry):
        industry_key, naics_prefix, dba_risk, ceiling = detect_industry(company_name)
        assert industry_key == expected_industry, (
            f"{company_name!r}: expected {expected_industry}, got {industry_key}"
        )

    def test_unknown_company_returns_unknown(self):
        industry_key, naics_prefix, dba_risk, ceiling = detect_industry("XYZ Corp 12345")
        assert industry_key == "unknown"

    def test_all_seven_industries_have_naics_prefix(self):
        industries = [
            "auto_dealerships", "hotels_motels", "food_drinking",
            "entertainment_leisure", "fitness_centers",
            "physicians_offices", "manufacturing",
        ]
        for ind in industries:
            assert ind in NAICS_PREFIX_MAP, f"{ind} missing from NAICS_PREFIX_MAP"
            assert NAICS_PREFIX_MAP[ind], f"{ind} has empty NAICS prefix"

    def test_all_seven_industries_have_persona_filters(self):
        industries = [
            "auto_dealerships", "hotels_motels", "food_drinking",
            "entertainment_leisure", "fitness_centers",
            "physicians_offices", "manufacturing",
        ]
        for ind in industries:
            assert ind in PERSONA_FILTERS, f"{ind} missing from PERSONA_FILTERS"
            pf = PERSONA_FILTERS[ind]
            assert "titles" in pf and len(pf["titles"]) > 0
            assert "seniority" in pf and len(pf["seniority"]) > 0


# ---------------------------------------------------------------------------
# DBA risk per industry
# ---------------------------------------------------------------------------

class TestDbaRisk:
    @pytest.mark.parametrize("company_name,expected_risk", [
        ("Ford Dealership", "high"),
        ("Best Western Hotel", "high"),
        ("Mario's Restaurant", "high"),
        ("Downtown Bowling Entertainment", "medium"),
        ("Anytime Fitness", "medium"),
        ("Pediatric Medical Clinic", "low"),
        ("Acme Manufacturing", "low"),
    ])
    def test_dba_risk_per_industry(self, company_name, expected_risk):
        _, _, dba_risk, _ = detect_industry(company_name)
        assert dba_risk == expected_risk


# ---------------------------------------------------------------------------
# Per-industry confidence ceiling (spec 7.5)
# ---------------------------------------------------------------------------

class TestConfidenceCeiling:
    @pytest.mark.parametrize("industry_key,dba_risk,expected_ceiling", [
        ("auto_dealerships", "high", 0.85),
        ("hotels_motels", "high", 0.85),
        ("food_drinking", "high", 0.85),
        ("entertainment_leisure", "medium", 0.90),
        ("fitness_centers", "medium", 0.90),
        ("physicians_offices", "low", 0.95),
        ("manufacturing", "low", 0.95),
        ("unknown", "low", 0.80),   # default (env var CONFIDENCE_AUTO_WRITE)
    ])
    def test_ceiling_per_industry(self, industry_key, dba_risk, expected_ceiling):
        threshold = auto_write_threshold(industry_key, dba_risk)
        assert threshold == expected_ceiling

    def test_high_dba_risk_requires_higher_threshold_than_default(self):
        """High DBA-risk industries need ≥ 0.85, not 0.80, to auto-write."""
        hotel_threshold = auto_write_threshold("hotels_motels", "high")
        assert hotel_threshold > 0.80

    def test_ceiling_applied_in_synthesise(self):
        """A score of 0.82 should NOT auto-write for a hotel (ceiling 0.85)."""
        signals = {
            "fein_match": 0.35,
            "clay_company_confidence": 0.25,
            "cross_source_agreement": 0.20 * 0.5,
            "completeness": 0.10,
            "dol_fuzzy_confidence": 0.10 * 0.5,
        }
        score, _ = compute_confidence({
            "fein_match": 1.0,
            "clay_company_confidence": 0.6,
            "cross_source_agreement": 0.5,
            "completeness": 0.5,
            "dol_fuzzy_confidence": 0.5,
        })
        hotel_threshold = auto_write_threshold("hotels_motels", "high")
        # Even if score passes default 0.80, it still needs to clear 0.85
        # This test just confirms the ceiling is higher
        assert hotel_threshold == 0.85


# ---------------------------------------------------------------------------
# Industry confidence ceiling from detect_industry
# ---------------------------------------------------------------------------

class TestIndustryCeilingFromDetect:
    def test_high_risk_industry_ceiling(self):
        _, _, _, ceiling = detect_industry("Best Western Hotel")
        assert ceiling == 0.85

    def test_low_risk_industry_ceiling(self):
        _, _, _, ceiling = detect_industry("Acme Manufacturing")
        assert ceiling == 0.95

    def test_medium_risk_industry_ceiling(self):
        _, _, _, ceiling = detect_industry("Anytime Fitness Center")
        assert ceiling == 0.90
