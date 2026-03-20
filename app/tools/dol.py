"""
DOL Form 5500 static lookup tool.

Queries the DuckDB dol_form5500 table using FEIN exact match first,
then fuzzy name + state blocking match.
"""

import re
import logging
from typing import Optional

import duckdb
import jellyfish

from app.models.responses import StaticToolResult
from app.utils.db import fetchall_as_dicts

logger = logging.getLogger(__name__)

# Legal / plan suffix tokens stripped during normalisation
_LEGAL_SUFFIXES = {
    "llc", "inc", "corp", "ltd", "lp", "llp", "co", "incorporated", "limited",
}
_PLAN_SUFFIXES = {
    "pension", "plan", "401k", "retirement", "health", "welfare",
    "group", "insurance", "profit", "sharing",
}
_ABBREVIATIONS = {
    "mfg": "manufacturing",
    "svcs": "services",
    "assoc": "associates",
    "mgmt": "management",
    "hosp": "hospitality",
}

_PUNCT_RE = re.compile(r"[^\w\s]")
_SPACE_RE = re.compile(r"\s+")


def normalise_company_name(name: str) -> str:
    """Normalise a company name for fuzzy matching."""
    s = name.lower()
    # Remove punctuation first so suffix tokens are clean
    s = _PUNCT_RE.sub("", s)
    # Collapse spaces
    s = _SPACE_RE.sub(" ", s).strip()
    # Expand abbreviations
    tokens = s.split()
    tokens = [_ABBREVIATIONS.get(t, t) for t in tokens]
    s = " ".join(tokens)
    # Strip legal suffixes
    tokens = [t for t in s.split() if t not in _LEGAL_SUFFIXES]
    s = " ".join(tokens)
    # Strip plan suffixes
    tokens = [t for t in s.split() if t not in _PLAN_SUFFIXES]
    s = " ".join(tokens)
    # Final collapse
    s = _SPACE_RE.sub(" ", s).strip()
    return s


def _score_candidate(
    row: dict,
    normalised_input: str,
    state: Optional[str],
    naics_hint: Optional[str],
) -> float:
    candidate_name = row.get("normalised_name", "") or ""
    name_score = jellyfish.jaro_winkler_similarity(normalised_input, candidate_name)
    state_score = 1.0 if (state and row.get("spons_state") == state) else 0.0
    naics_score = 0.0
    if naics_hint and row.get("business_code"):
        if str(row["business_code"]).startswith(str(naics_hint)[:3]):
            naics_score = 1.0
    addr_score = 0.0
    addr = (row.get("spons_dfe_pn") or "").lower()
    if addr:
        for tok in normalised_input.split():
            if len(tok) > 3 and tok in addr:
                addr_score = 1.0
                break
    return (
        name_score * 0.50
        + state_score * 0.20
        + naics_score * 0.15
        + addr_score * 0.15
    )


async def find_in_dol(
    conn: duckdb.DuckDBPyConnection,
    name: str,
    state: Optional[str] = None,
    city: Optional[str] = None,
    naics_hint: Optional[str] = None,
    fein: Optional[str] = None,
) -> StaticToolResult:
    """
    Look up a company in the DOL Form 5500 dataset.

    Returns a StaticToolResult with confidence 0.0–1.0.
    """
    empty = StaticToolResult(
        source="dol_form5500",
        match_key_used="none",
        confidence=0.0,
        matched=False,
        fields={},
    )

    try:
        # Path 1: FEIN exact match
        if fein:
            rows = fetchall_as_dicts(
                conn.execute("SELECT * FROM dol_form5500 WHERE ein = ? LIMIT 1", [fein])
            )
            if rows:
                row = rows[0]
                return StaticToolResult(
                    source="dol_form5500",
                    match_key_used="fein_exact",
                    confidence=0.98,
                    matched=True,
                    matched_record_id=str(row.get("ack_id", "")),
                    fields=_extract_fields(row),
                )

        # Path 2: Fuzzy name match with state blocking
        if not name:
            return empty

        normalised_input = normalise_company_name(name)
        if not normalised_input:
            return empty

        tokens = normalised_input.split()
        if not tokens:
            return empty

        prefix_token = tokens[0]

        query_args: list = [normalised_input, f"{prefix_token}%"]
        state_clause = ""
        if state:
            state_clause = "AND spons_state = ?"
            query_args.append(state)

        sql = f"""
            SELECT *,
                   jaro_winkler_similarity(normalised_name, ?) AS name_score
            FROM dol_form5500
            WHERE normalised_name LIKE ?
              {state_clause}
            ORDER BY name_score DESC
            LIMIT 200
        """

        candidates = fetchall_as_dicts(conn.execute(sql, query_args))

        if not candidates and state:
            # Retry without state filter
            sql_no_state = """
                SELECT *,
                       jaro_winkler_similarity(normalised_name, ?) AS name_score
                FROM dol_form5500
                WHERE normalised_name LIKE ?
                ORDER BY name_score DESC
                LIMIT 200
            """
            candidates = fetchall_as_dicts(
                conn.execute(sql_no_state, [normalised_input, f"{prefix_token}%"])
            )

        if not candidates:
            return empty

        best_score = 0.0
        best_row = None
        for row in candidates:
            score = _score_candidate(row, normalised_input, state, naics_hint)
            if score > best_score:
                best_score = score
                best_row = row

        if best_row is None or best_score < 0.60:
            return empty

        return StaticToolResult(
            source="dol_form5500",
            match_key_used="name+state" if state else "name",
            confidence=round(best_score, 4),
            matched=True,
            matched_record_id=str(best_row.get("ack_id", "")),
            fields=_extract_fields(best_row),
        )

    except Exception as exc:
        logger.warning("find_in_dol error: %s", exc)
        return empty


def _extract_fields(row: dict) -> dict:
    return {
        "dol_sponsor_name": row.get("sponsor_dfe_name"),
        "dol_broker_name": row.get("broker_name"),
        "dol_broker_ein": row.get("broker_ein"),
        "dol_cpa_name": row.get("cpa_name"),
        "dol_plan_name": row.get("plan_name"),
        "dol_plan_administrator": _build_admin_name(row),
        "dol_business_code": row.get("business_code"),
        "dol_active_participants": row.get("tot_partcp_boy_cnt"),
        "dol_spons_state": row.get("spons_state"),
    }


def _build_admin_name(row: dict) -> Optional[str]:
    """Construct plan administrator name from available fields."""
    admin = row.get("plan_admin_name") or row.get("plan_admin_sign_name")
    return admin if admin else None
