"""
POST /api/v1/classify — Agent Call 1

Classifies a row by archetype, runs static lookups in parallel,
and returns run flags + static data results to Clay.
"""

import asyncio
import logging
from typing import Optional

from fastapi import APIRouter, HTTPException

from app.models.requests import ClassifyRequest
from app.models.responses import ClassifyResponse
from app.services.database import get_conn
from app.tools.dol import find_in_dol
from app.tools.naics import lookup_naics, detect_industry, PERSONA_FILTERS
from app.tools.legal_entity import resolve_legal_entity

logger = logging.getLogger(__name__)
router = APIRouter()


# ---------------------------------------------------------------------------
# Archetype classification
# ---------------------------------------------------------------------------

def classify_archetype(req: ClassifyRequest) -> str:
    if req.is_dol_native:
        return "dol_native"
    if req.has_fein:
        return "fein_list"
    if req.has_company and req.has_person:
        return "A"
    if req.has_person and not req.has_company:
        return "B"
    if req.has_company and not req.has_person:
        return "C"
    if req.linkedin_person and req.has_intent_signal:
        return "intent_contact"
    # No usable signal
    return "sparse"


# ---------------------------------------------------------------------------
# Flag construction
# ---------------------------------------------------------------------------

def build_run_flags(
    req: ClassifyRequest,
    archetype: str,
    dba_risk: str,
    legal_entity_name: Optional[str],
) -> dict:
    run_company_enrich = (
        req.has_company
        and archetype in ("A", "C", "fein_list")
        and not req.is_dol_native
    )
    run_person_lookup = req.has_person and not req.has_company  # Archetype B
    run_legal_entity = (
        req.has_company
        and dba_risk == "high"
        and legal_entity_name is None
    )
    run_contacts_search = (
        req.list_intent in ("enrich_then_expand", "expand_then_enrich")
        or archetype == "C"
    )
    run_people_expansion = req.list_intent in ("enrich_then_expand", "expand_then_enrich")

    return {
        "run_company_enrich": run_company_enrich,
        "run_person_lookup": run_person_lookup,
        "run_legal_entity": run_legal_entity,
        "run_contacts_search": run_contacts_search,
        "run_people_expansion": run_people_expansion,
    }


# ---------------------------------------------------------------------------
# Phase assignment
# ---------------------------------------------------------------------------

def assign_phase(archetype: str) -> str:
    if archetype == "sparse":
        return "skipped"
    if archetype == "B":
        return "find_company"
    return "strategy_set"


# ---------------------------------------------------------------------------
# Main handler
# ---------------------------------------------------------------------------

@router.post("/classify", response_model=ClassifyResponse)
async def classify(req: ClassifyRequest) -> ClassifyResponse:
    # Idempotency guard
    if req.static_done:
        logger.info("static_done=True — returning no-op for idempotency")
        return ClassifyResponse(
            phase="strategy_set",
            archetype="A",
            dba_risk="low",
            industry_detected="unknown",
            run_company_enrich=False,
            run_person_lookup=False,
            run_legal_entity=False,
            run_contacts_search=False,
            run_people_expansion=False,
            static_done=True,
            confidence_pre=0.0,
        )

    archetype = classify_archetype(req)
    logger.info("Archetype: %s | company=%s | person=%s", archetype, req.company_name, req.first_name)

    # ------------------------------------------------------------------
    # Static lookups (parallel) — only if we have a name or FEIN
    # ------------------------------------------------------------------
    dol_result = None
    naics_result = None
    legal_result = None

    can_run_static = (req.has_company or req.has_fein) and not req.is_dol_native
    if can_run_static and archetype not in ("B", "sparse"):
        conn = get_conn()

        async def _dol():
            return await find_in_dol(
                conn,
                name=req.company_name or "",
                state=req.state,
                naics_hint=None,
                fein=req.fein,
            )

        async def _naics():
            return await lookup_naics(conn, company_name=req.company_name or "")

        async def _legal():
            return await resolve_legal_entity(
                company_name=req.company_name or "",
                state=req.state,
                website=req.website,
                dba_risk="low",  # placeholder — updated below
            )

        try:
            dol_result, naics_result, legal_result = await asyncio.wait_for(
                asyncio.gather(_dol(), _naics(), _legal()),
                timeout=2.5,
            )
        except asyncio.TimeoutError:
            logger.warning("Static tool calls timed out for %s", req.company_name)

    # ------------------------------------------------------------------
    # Industry / DBA risk
    # ------------------------------------------------------------------
    industry_key = "unknown"
    dba_risk = "low"
    naics_code = None
    naics_sector = None
    confidence_ceiling = 0.95
    persona_filter = None

    if naics_result and naics_result.matched:
        f = naics_result.fields
        industry_key = f.get("industry_key", "unknown")
        dba_risk = f.get("dba_risk", "low")
        naics_code = f.get("naics_code")
        naics_sector = f.get("naics_sector")
        confidence_ceiling = f.get("confidence_ceiling", 0.95)
        persona_filter = f.get("persona_filter")

    # Re-run legal entity with correct dba_risk if needed
    if can_run_static and dba_risk != "low" and req.has_company and archetype not in ("B", "sparse"):
        legal_result = await resolve_legal_entity(
            company_name=req.company_name or "",
            state=req.state,
            website=req.website,
            dba_risk=dba_risk,
        )

    # ------------------------------------------------------------------
    # DOL fields
    # ------------------------------------------------------------------
    dol_fields: dict = {}
    dol_confidence = None
    if dol_result and dol_result.matched:
        dol_fields = dol_result.fields
        dol_confidence = dol_result.confidence

    legal_entity_name = None
    legal_entity_source = None
    run_legal_entity_flag = False

    if legal_result:
        f = legal_result.fields
        if legal_result.source == "claygent_pending":
            run_legal_entity_flag = True
        else:
            legal_entity_name = f.get("legal_entity_name")
            legal_entity_source = f.get("legal_entity_source")

    # ------------------------------------------------------------------
    # Run flags
    # ------------------------------------------------------------------
    flags = build_run_flags(req, archetype, dba_risk, legal_entity_name)
    if run_legal_entity_flag:
        flags["run_legal_entity"] = True

    # ------------------------------------------------------------------
    # Pre-enrichment confidence estimate
    # ------------------------------------------------------------------
    confidence_pre = 0.0
    if dol_result and dol_result.matched:
        confidence_pre = round(dol_result.confidence * 0.5, 3)  # rough pre-score
    if req.has_fein and dol_result and dol_result.match_key_used == "fein_exact":
        confidence_pre = 0.60  # FEIN match gives good pre-score

    phase = assign_phase(archetype)
    static_done = can_run_static and archetype not in ("B", "sparse")

    return ClassifyResponse(
        phase=phase,
        archetype=archetype,
        dba_risk=dba_risk,
        industry_detected=industry_key,
        run_company_enrich=flags["run_company_enrich"],
        run_person_lookup=flags["run_person_lookup"],
        run_legal_entity=flags["run_legal_entity"],
        run_contacts_search=flags["run_contacts_search"],
        run_people_expansion=flags["run_people_expansion"],
        static_done=static_done,
        confidence_pre=confidence_pre,
        persona_filter_json=persona_filter,
        dol_sponsor_name=dol_fields.get("dol_sponsor_name"),
        dol_broker_name=dol_fields.get("dol_broker_name"),
        dol_broker_ein=dol_fields.get("dol_broker_ein"),
        dol_cpa_name=dol_fields.get("dol_cpa_name"),
        dol_plan_name=dol_fields.get("dol_plan_name"),
        dol_plan_administrator=dol_fields.get("dol_plan_administrator"),
        dol_match_confidence=dol_confidence,
        naics_code=naics_code,
        naics_sector=naics_sector,
        legal_entity_name=legal_entity_name,
        legal_entity_source=legal_entity_source,
    )
