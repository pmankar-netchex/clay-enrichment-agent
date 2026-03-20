from typing import Optional, Dict, Any
from pydantic import BaseModel
from dataclasses import dataclass, field


@dataclass
class StaticToolResult:
    source: str
    match_key_used: str
    confidence: float
    matched: bool
    fields: dict = field(default_factory=dict)
    matched_record_id: Optional[str] = None


class ClassifyResponse(BaseModel):
    phase: str
    archetype: str
    dba_risk: str
    industry_detected: str
    run_company_enrich: bool
    run_person_lookup: bool
    run_legal_entity: bool
    run_contacts_search: bool
    run_people_expansion: bool
    static_done: bool
    confidence_pre: float
    persona_filter_json: Optional[Dict[str, Any]] = None
    # Static data results
    dol_sponsor_name: Optional[str] = None
    dol_broker_name: Optional[str] = None
    dol_broker_ein: Optional[str] = None
    dol_cpa_name: Optional[str] = None
    dol_plan_name: Optional[str] = None
    dol_plan_administrator: Optional[str] = None
    dol_match_confidence: Optional[float] = None
    naics_code: Optional[str] = None
    naics_sector: Optional[str] = None
    legal_entity_name: Optional[str] = None
    legal_entity_source: Optional[str] = None


class StaticLookupResponse(BaseModel):
    phase: str  # Always "enrich"
    static_done: bool  # Always True
    dol_sponsor_name: Optional[str] = None
    dol_broker_name: Optional[str] = None
    dol_broker_ein: Optional[str] = None
    dol_cpa_name: Optional[str] = None
    dol_plan_name: Optional[str] = None
    dol_plan_administrator: Optional[str] = None
    dol_match_confidence: Optional[float] = None
    naics_code: Optional[str] = None
    naics_sector: Optional[str] = None
    legal_entity_name: Optional[str] = None
    legal_entity_source: Optional[str] = None


class SynthesiseResponse(BaseModel):
    confidence_final: float
    confidence_breakdown: Dict[str, float]
    review_reason: Optional[str] = None
    run_dedup: bool
    run_sf: bool
    phase: str  # "scored" or "review_pending"
    batch_id: str


class ReviewResumeResponse(BaseModel):
    batch_id: str
    processed: int
    approved: int
    rejected: int
    edited: int
    errors: list = []
