"""
Tests for confidence scoring model (Section 7).
"""

import pytest
from app.services.confidence import (
    compute_confidence,
    cross_source_agreement,
    record_completeness,
    build_review_reason,
)


class TestComputeConfidence:
    def test_full_score(self):
        signals = {
            "fein_match": 1.0,
            "clay_company_confidence": 1.0,
            "cross_source_agreement": 1.0,
            "completeness": 1.0,
            "dol_fuzzy_confidence": 1.0,
        }
        score, breakdown = compute_confidence(signals)
        assert abs(score - 1.0) < 0.01
        assert sum(breakdown.values()) == pytest.approx(score, abs=0.001)

    def test_zero_score(self):
        score, breakdown = compute_confidence({})
        assert score == 0.0

    def test_partial_score(self):
        signals = {"fein_match": 1.0}  # 0.35 weight
        score, _ = compute_confidence(signals)
        assert abs(score - 0.35) < 0.01

    def test_weights_sum_to_one(self):
        from app.services.confidence import WEIGHTS
        assert abs(sum(WEIGHTS.values()) - 1.0) < 0.001


class TestCrossSourceAgreement:
    def test_matching_state(self):
        clay = {"enriched_state": "CA"}
        dol = {"dol_spons_state": "CA"}
        result = cross_source_agreement(clay, dol)
        assert result == 1.0

    def test_mismatched_state(self):
        clay = {"enriched_state": "CA"}
        dol = {"dol_spons_state": "TX"}
        result = cross_source_agreement(clay, dol)
        assert result == 0.0

    def test_no_overlap_defaults_to_half(self):
        result = cross_source_agreement({}, {})
        assert result == 0.5


class TestRecordCompleteness:
    def test_all_present(self):
        enriched = {
            "enriched_company_name": "Acme",
            "enriched_domain": "acme.com",
            "enriched_industry": "Manufacturing",
            "enriched_email": "ceo@acme.com",
            "enriched_first_name": "Jane",
            "enriched_last_name": "Doe",
        }
        assert record_completeness(enriched) == 1.0

    def test_none_present(self):
        assert record_completeness({}) == 0.0

    def test_partial(self):
        enriched = {"enriched_company_name": "Acme", "enriched_domain": "acme.com"}
        score = record_completeness(enriched)
        assert 0 < score < 1.0


class TestBuildReviewReason:
    def test_no_fein(self):
        signals = {"fein_match": 0.0, "dol_fuzzy_confidence": 0.0, "cross_source_agreement": 0.5, "completeness": 0.8}
        reason = build_review_reason(signals, {})
        assert "FEIN" in reason

    def test_low_dol_confidence(self):
        signals = {"fein_match": 0.0, "dol_fuzzy_confidence": 0.65, "cross_source_agreement": 0.5, "completeness": 0.8}
        reason = build_review_reason(signals, {})
        assert "DOL match confidence" in reason
