"""
POST /api/v1/synthesise — Agent Call 3

Final column in Clay. Computes composite confidence, sets run_dedup / run_sf,
and queues low-confidence rows for batch human review.

Batch grouping: Clay passes batch_id and list_name consistently for all rows
in a single list upload. The background flush loop sends one email per batch
after BATCH_IDLE_SECONDS of inactivity.
"""

import logging
import os
import uuid
from typing import Optional

from fastapi import APIRouter, BackgroundTasks

from app.models.requests import SynthesiseRequest
from app.models.responses import SynthesiseResponse
from app.services.confidence import (
    compute_confidence,
    cross_source_agreement,
    record_completeness,
    build_review_reason,
    auto_write_threshold,
)
from app.services.batch_review import (
    ensure_batch,
    record_auto_written,
    store_review_row,
    append_enrichment_log,
)
from app.services.clay_api import write_plan_admin_to_t2

logger = logging.getLogger(__name__)
router = APIRouter()

CONFIDENCE_AUTO_WRITE = float(os.getenv("CONFIDENCE_AUTO_WRITE", "0.80"))
CONFIDENCE_REVIEW_LOW = float(os.getenv("CONFIDENCE_REVIEW_LOW", "0.60"))


@router.post("/synthesise", response_model=SynthesiseResponse)
async def synthesise(req: SynthesiseRequest, background_tasks: BackgroundTasks) -> SynthesiseResponse:
    # Resolve batch identity — use Clay-provided batch_id or generate one
    batch_id = req.batch_id or str(uuid.uuid4())
    list_name = req.list_name or "enrichment_batch"

    # Ensure batch record exists / update last_row_at
    ensure_batch(batch_id, list_name)

    # ------------------------------------------------------------------
    # Confidence scoring
    # ------------------------------------------------------------------
    enriched = req.model_dump()

    fein_match = 1.0 if (req.dol_match_confidence and req.dol_match_confidence >= 0.95) else 0.0
    clay_co_conf = float(req.clay_company_confidence or 0.0)
    dol_fuzzy_conf = float(req.dol_match_confidence or 0.0)

    dol_fields = {
        "dol_business_code": req.dol_business_code,
        "dol_active_participants": req.dol_active_participants,
        "dol_spons_state": req.dol_spons_state,
    }
    cross_agree = cross_source_agreement(enriched, dol_fields)
    completeness = record_completeness(enriched)

    signals = {
        "fein_match": fein_match,
        "clay_company_confidence": clay_co_conf,
        "cross_source_agreement": cross_agree,
        "completeness": completeness,
        "dol_fuzzy_confidence": dol_fuzzy_conf,
    }

    confidence_final, breakdown = compute_confidence(signals)

    # ------------------------------------------------------------------
    # Per-industry confidence ceiling (spec 7.5)
    # Higher threshold required for high DBA-risk industries.
    # High DBA-risk rows also require legal entity resolution before auto-write.
    # ------------------------------------------------------------------
    industry_key = req.industry_detected or "unknown"
    threshold = auto_write_threshold(industry_key, req.dba_risk)

    legal_entity_gate_failed = (
        req.dba_risk == "high"
        and not req.legal_entity_name
        and req.archetype not in ("dol_native", "fein_list")
    )

    # ------------------------------------------------------------------
    # Outcome routing
    # ------------------------------------------------------------------
    run_dedup = False
    run_sf = False
    review_reason: Optional[str] = None

    if confidence_final >= threshold and not legal_entity_gate_failed:
        run_dedup = True
        run_sf = True
        phase = "scored"
        background_tasks.add_task(record_auto_written, batch_id)
    else:
        phase = "review_pending"
        review_reason = build_review_reason(signals, breakdown)
        if legal_entity_gate_failed:
            review_reason = "Legal entity not resolved for high DBA-risk company. " + (review_reason or "")

        company = req.company_name or req.enriched_company_name or ""
        person = req.person_name or _build_name(req)
        row_id = req.row_id or str(uuid.uuid4())
        enriched_fields = {
            k: v for k, v in enriched.items()
            if k.startswith("enriched_") and v is not None
        }
        suggested = "approve" if confidence_final >= CONFIDENCE_REVIEW_LOW else "review"

        background_tasks.add_task(
            store_review_row,
            batch_id=batch_id,
            row_id=row_id,
            company_name=company,
            person_name=person,
            confidence_final=confidence_final,
            review_reason=review_reason,
            enriched_fields=enriched_fields,
            suggested_action=suggested,
        )

    # ------------------------------------------------------------------
    # DOL plan administrator → T2 free contact (spec 9.3)
    # Fire when plan admin is present and list intent includes expansion
    # ------------------------------------------------------------------
    if (
        req.dol_plan_administrator
        and req.list_intent in ("enrich_then_expand", "expand_then_enrich")
    ):
        background_tasks.add_task(
            write_plan_admin_to_t2,
            plan_admin_name=req.dol_plan_administrator,
            plan_admin_title=req.dol_plan_administrator_title or "Plan Administrator",
            company_name=req.enriched_company_name or req.company_name or "",
            company_website=req.enriched_domain or "",
            naics_code=req.dol_business_code or "",
            dol_broker_name="",
            sf_account_id=req.sf_account_id or "",
        )

    # ------------------------------------------------------------------
    # Audit log (T4)
    # ------------------------------------------------------------------
    sf_outcome = "pending" if not run_sf else "queued"
    background_tasks.add_task(
        append_enrichment_log,
        batch_id=batch_id,
        list_name=list_name,
        archetype=req.archetype,
        phase_final=phase,
        confidence_final=confidence_final,
        review_decision="auto" if run_sf else "pending_review",
        sf_outcome=sf_outcome,
    )

    return SynthesiseResponse(
        confidence_final=confidence_final,
        confidence_breakdown=breakdown,
        review_reason=review_reason,
        run_dedup=run_dedup,
        run_sf=run_sf,
        phase=phase,
        batch_id=batch_id,
    )


def _build_name(req: SynthesiseRequest) -> str:
    parts = [req.enriched_first_name, req.enriched_last_name]
    return " ".join(p for p in parts if p).strip()
