"""
Composite confidence scoring model (Section 7).
"""

import re
from typing import Optional


WEIGHTS = {
    "fein_match": 0.35,
    "clay_company_confidence": 0.25,
    "cross_source_agreement": 0.20,
    "completeness": 0.10,
    "dol_fuzzy_confidence": 0.10,
}

# Required Salesforce fields used for completeness scoring
_REQUIRED_SF_FIELDS = [
    "enriched_company_name",
    "enriched_domain",
    "enriched_industry",
    "enriched_email",
    "enriched_first_name",
    "enriched_last_name",
]

# Industry-specific auto-write ceilings
_INDUSTRY_CEILINGS = {
    "auto_dealerships": 0.85,
    "hotels_motels": 0.85,
    "food_drinking": 0.85,
    "entertainment_leisure": 0.90,
    "fitness_centers": 0.90,
    "physicians_offices": 0.95,
    "manufacturing": 0.95,
}


def compute_confidence(signals: dict) -> tuple[float, dict]:
    """
    Compute weighted confidence score.

    Returns (composite_score, breakdown_dict).
    """
    breakdown = {}
    for key, weight in WEIGHTS.items():
        value = float(signals.get(key, 0.0) or 0.0)
        breakdown[key] = round(value * weight, 3)

    composite = round(sum(breakdown.values()), 3)
    return composite, breakdown


def cross_source_agreement(clay_fields: dict, dol_fields: dict) -> float:
    """
    Measure agreement between Clay-enriched fields and DOL data.
    Returns 0.0–1.0 (default 0.5 when no checks possible).
    """
    checks = []

    clay_industry = clay_fields.get("enriched_industry") or ""
    dol_biz_code = str(dol_fields.get("dol_business_code") or "")
    if clay_industry and dol_biz_code:
        clay_naics = _industry_to_naics_prefix(clay_industry)
        if clay_naics and dol_biz_code:
            checks.append(1.0 if clay_naics[:3] == dol_biz_code[:3] else 0.0)

    clay_count_raw = clay_fields.get("enriched_employee_count")
    dol_count_raw = dol_fields.get("dol_active_participants")
    if clay_count_raw and dol_count_raw:
        clay_count = _parse_count_band(str(clay_count_raw))
        try:
            dol_count = int(dol_count_raw)
            if clay_count > 0:
                ratio = abs(clay_count - dol_count) / max(clay_count, 1)
                checks.append(1.0 if ratio < 0.5 else 0.3)
        except (ValueError, TypeError):
            pass

    clay_state = clay_fields.get("enriched_state") or ""
    dol_state = str(dol_fields.get("dol_spons_state") or "")
    if clay_state and dol_state:
        checks.append(1.0 if clay_state.upper() == dol_state.upper() else 0.0)

    return round(sum(checks) / len(checks), 3) if checks else 0.5


def record_completeness(enriched: dict) -> float:
    """Return fraction of required SF fields that are populated."""
    populated = sum(
        1 for f in _REQUIRED_SF_FIELDS
        if enriched.get(f) not in (None, "", "undefined", "null")
    )
    return round(populated / len(_REQUIRED_SF_FIELDS), 3)


def auto_write_threshold(industry_key: str, dba_risk: str) -> float:
    """Return the confidence threshold for auto-write for this industry."""
    return _INDUSTRY_CEILINGS.get(industry_key, 0.80)


def build_review_reason(signals: dict, breakdown: dict) -> str:
    reasons = []

    if signals.get("fein_match", 0) == 0:
        reasons.append("No FEIN available for exact DOL match")

    dol_conf = signals.get("dol_fuzzy_confidence", 0)
    if 0 < dol_conf < 0.75:
        reasons.append(
            f"DOL match confidence {dol_conf:.2f} — company may be DBA or franchise operator"
        )

    cross = signals.get("cross_source_agreement", 0)
    if cross < 0.5:
        reasons.append("Low cross-source agreement between Clay enrichment and DOL data")

    completeness = signals.get("completeness", 0)
    if completeness < 0.5:
        reasons.append("Missing required Salesforce fields after enrichment")

    return ". ".join(reasons) if reasons else "Confidence below auto-write threshold"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _industry_to_naics_prefix(industry: str) -> str:
    industry_lower = industry.lower()
    mappings = {
        "auto": "4411", "car": "4411", "dealer": "4411",
        "hotel": "7211", "motel": "7211", "inn": "7211",
        "restaurant": "722", "food": "722", "bar": "722",
        "entertainment": "7131", "leisure": "7131",
        "fitness": "7139", "gym": "7139",
        "medical": "621", "health": "621", "physician": "621",
        "manufacturing": "31", "industrial": "31",
    }
    for kw, prefix in mappings.items():
        if kw in industry_lower:
            return prefix
    return ""


def _parse_count_band(band: str) -> int:
    """Parse employee count band like '50-100' or '500+' to a midpoint integer."""
    band = band.strip().replace(",", "")
    if "+" in band:
        try:
            return int(re.sub(r"\D", "", band.split("+")[0]))
        except ValueError:
            return 0
    if "-" in band:
        parts = band.split("-")
        try:
            return (int(parts[0]) + int(parts[1])) // 2
        except (ValueError, IndexError):
            return 0
    try:
        return int(re.sub(r"\D", "", band))
    except ValueError:
        return 0
