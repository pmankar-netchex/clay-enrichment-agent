"""
POST /api/v1/static_lookup — Agent Call 2

Fires for Archetype B rows when Clay has resolved the company name.
Runs DOL + NAICS lookups and advances phase to "enrich".
"""

import asyncio
import logging

from fastapi import APIRouter

from app.models.requests import StaticLookupRequest
from app.models.responses import StaticLookupResponse
from app.services.database import get_conn
from app.tools.dol import find_in_dol
from app.tools.naics import lookup_naics
from app.tools.legal_entity import resolve_legal_entity

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/static_lookup", response_model=StaticLookupResponse)
async def static_lookup(req: StaticLookupRequest) -> StaticLookupResponse:
    conn = get_conn()

    async def _dol():
        return await find_in_dol(
            conn,
            name=req.company_name,
            state=req.state,
        )

    async def _naics():
        return await lookup_naics(conn, company_name=req.company_name)

    async def _legal():
        return await resolve_legal_entity(
            company_name=req.company_name,
            state=req.state,
            website=req.website,
            dba_risk=req.dba_risk,
        )

    try:
        dol_result, naics_result, legal_result = await asyncio.wait_for(
            asyncio.gather(_dol(), _naics(), _legal()),
            timeout=2.5,
        )
    except asyncio.TimeoutError:
        logger.warning("Static lookup timed out for %s", req.company_name)
        dol_result = naics_result = legal_result = None

    dol_fields = dol_result.fields if (dol_result and dol_result.matched) else {}
    dol_confidence = dol_result.confidence if (dol_result and dol_result.matched) else None

    naics_code = None
    naics_sector = None
    if naics_result and naics_result.matched:
        naics_code = naics_result.fields.get("naics_code")
        naics_sector = naics_result.fields.get("naics_sector")

    legal_entity_name = None
    legal_entity_source = None
    if legal_result and legal_result.source != "claygent_pending" and legal_result.matched:
        legal_entity_name = legal_result.fields.get("legal_entity_name")
        legal_entity_source = legal_result.fields.get("legal_entity_source")

    return StaticLookupResponse(
        phase="enrich",
        static_done=True,
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
