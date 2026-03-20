"""
NAICS lookup tool.

Matches a company name against industry keyword patterns and
the naics_codes DuckDB table.
"""

import logging
from typing import Optional

import duckdb

from app.models.responses import StaticToolResult
from app.utils.db import fetchall_as_dicts

logger = logging.getLogger(__name__)

# Industry keyword patterns → (naics_prefix, dba_risk, confidence_ceiling)
INDUSTRY_PATTERNS: list[tuple[str, list[str], str, float]] = [
    (
        "auto_dealerships",
        ["ford", "chevy", "toyota", "honda", "nissan", "mazda", "auto", "motors",
         "motor co", "automotive", "dealership", "chevrolet"],
        "high", 0.85,
    ),
    (
        "hotels_motels",
        ["hotel", "inn", "motel", "suites", "hospitality", "lodging", "resort",
         "marriott", "hilton", "hyatt", "sheraton"],
        "high", 0.85,
    ),
    (
        "food_drinking",
        ["restaurant", "grill", "kitchen", "bistro", "cafe", "diner", "bar",
         "eatery", "pizza", "burger", "bbq", "sushi", "taco", "thai", "chinese",
         "italian", "steakhouse", "brewery", "pub"],
        "high", 0.85,
    ),
    (
        "entertainment_leisure",
        ["entertainment", "theatre", "theater", "cinema", "arcade", "bowling",
         "golf", "amusement", "recreation", "fun"],
        "medium", 0.90,
    ),
    (
        "fitness_centers",
        ["fitness", "gym", "wellness", "yoga", "crossfit", "sport", "athletic",
         "health club", "anytime", "planet fitness"],
        "medium", 0.90,
    ),
    (
        "physicians_offices",
        ["medical", "healthcare", "health care", "clinic", "physician", "doctor",
         "pediatric", "surgery", "dental", "dentist", "ortho", "cardio",
         "oncology", "dermatology", "ophthalmology", "urolog"],
        "low", 0.95,
    ),
    (
        "manufacturing",
        ["manufacturing", "fabrication", "industries", "industrial", "products",
         "systems", "solutions", "technologies", "equipment", "components",
         "supply", "metals", "plastics", "chemicals"],
        "low", 0.95,
    ),
]

# NAICS prefix per industry key
NAICS_PREFIX_MAP = {
    "auto_dealerships": "4411",
    "hotels_motels": "7211",
    "food_drinking": "722",
    "entertainment_leisure": "7131",
    "fitness_centers": "7139",
    "physicians_offices": "621",
    "manufacturing": "31",
}

PERSONA_FILTERS: dict = {
    "auto_dealerships": {
        "titles": ["General Manager", "Finance Director", "HR Director",
                   "HR Manager", "Controller"],
        "seniority": ["director", "manager", "vp"],
    },
    "hotels_motels": {
        "titles": ["General Manager", "HR Manager", "Director of Finance",
                   "Controller", "Benefits Manager"],
        "seniority": ["director", "manager", "vp"],
    },
    "food_drinking": {
        "titles": ["Owner", "General Manager", "HR Manager", "Operations Manager"],
        "seniority": ["owner", "director", "manager"],
    },
    "entertainment_leisure": {
        "titles": ["General Manager", "HR Director", "CFO", "Controller"],
        "seniority": ["director", "manager", "vp", "c-suite"],
    },
    "fitness_centers": {
        "titles": ["Owner", "General Manager", "HR Manager", "Studio Manager"],
        "seniority": ["owner", "director", "manager"],
    },
    "physicians_offices": {
        "titles": ["Practice Manager", "Office Manager", "Administrator",
                   "HR Manager"],
        "seniority": ["director", "manager"],
    },
    "manufacturing": {
        "titles": ["HR Director", "HR Manager", "Benefits Manager", "VP HR",
                   "Controller"],
        "seniority": ["director", "manager", "vp"],
    },
}


def detect_industry(company_name: str) -> tuple[str, str, float, float]:
    """
    Detect industry from company name keywords.

    Returns: (industry_key, naics_prefix, dba_risk_str, confidence_ceiling)
    """
    name_lower = company_name.lower()
    for industry_key, keywords, dba_risk, ceiling in INDUSTRY_PATTERNS:
        for kw in keywords:
            if kw in name_lower:
                return (
                    industry_key,
                    NAICS_PREFIX_MAP[industry_key],
                    dba_risk,
                    ceiling,
                )
    return ("unknown", "", "low", 0.95)


async def lookup_naics(
    conn: duckdb.DuckDBPyConnection,
    company_name: str,
    industry_hint: Optional[str] = None,
) -> StaticToolResult:
    """
    Return NAICS code and sector for a company.

    Uses keyword detection first; falls back to DuckDB naics_codes table
    if an industry hint or NAICS prefix is available.
    """
    industry_key, naics_prefix, dba_risk, ceiling = detect_industry(company_name)

    if industry_key == "unknown" and not industry_hint:
        return StaticToolResult(
            source="naics_keyword",
            match_key_used="none",
            confidence=0.0,
            matched=False,
            fields={},
        )

    effective_prefix = naics_prefix or (industry_hint or "")[:4]

    try:
        rows = fetchall_as_dicts(conn.execute(
            "SELECT * FROM naics_codes WHERE CAST(naics_code AS VARCHAR) LIKE ? LIMIT 1",
            [f"{effective_prefix}%"],
        ))

        sector = ""
        naics_code = effective_prefix
        if rows:
            row = rows[0]
            naics_code = str(row.get("naics_code", effective_prefix))
            sector = str(row.get("naics_title", ""))

        return StaticToolResult(
            source="naics_keyword",
            match_key_used="keyword_match",
            confidence=0.80 if industry_key != "unknown" else 0.50,
            matched=True,
            fields={
                "naics_code": naics_code,
                "naics_sector": sector,
                "industry_key": industry_key,
                "dba_risk": dba_risk,
                "confidence_ceiling": ceiling,
                "persona_filter": PERSONA_FILTERS.get(industry_key),
            },
        )
    except Exception as exc:
        logger.warning("lookup_naics error: %s", exc)
        return StaticToolResult(
            source="naics_keyword",
            match_key_used="keyword_only",
            confidence=0.60 if industry_key != "unknown" else 0.0,
            matched=industry_key != "unknown",
            fields={
                "naics_code": naics_prefix,
                "naics_sector": "",
                "industry_key": industry_key,
                "dba_risk": dba_risk,
                "confidence_ceiling": ceiling,
                "persona_filter": PERSONA_FILTERS.get(industry_key),
            },
        )
