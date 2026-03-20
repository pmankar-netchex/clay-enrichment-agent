from typing import Optional, List
from pydantic import BaseModel, field_validator


def _null_normalise(v: Optional[str]) -> Optional[str]:
    """Treat common sentinel strings as None."""
    if v is None:
        return None
    if v.strip().lower() in {"undefined", "null", "none", "n/a", ""}:
        return None
    return v


class ClassifyRequest(BaseModel):
    company_name: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[str] = None
    website: Optional[str] = None
    fein: Optional[str] = None
    state: Optional[str] = None
    linkedin_person: Optional[str] = None
    has_company: bool = False
    has_person: bool = False
    has_fein: bool = False
    is_dol_native: bool = False
    has_website: bool = False
    has_intent_signal: bool = False
    list_intent: str = "enrich_only"
    skip_company_size: bool = False
    skip_industry: bool = False
    skip_revenue: bool = False
    # Idempotency guard — if true, return existing results unchanged
    static_done: bool = False

    @field_validator(
        "company_name", "first_name", "last_name", "email",
        "website", "fein", "state", "linkedin_person",
        mode="before",
    )
    @classmethod
    def normalise_nulls(cls, v: Optional[str]) -> Optional[str]:
        return _null_normalise(v)


class StaticLookupRequest(BaseModel):
    company_name: str
    state: Optional[str] = None
    website: Optional[str] = None
    dba_risk: str = "low"
    industry_detected: str = "unknown"
    archetype: str = "B"

    @field_validator("state", "website", mode="before")
    @classmethod
    def normalise_nulls(cls, v: Optional[str]) -> Optional[str]:
        return _null_normalise(v)


class SynthesiseRequest(BaseModel):
    archetype: str
    dba_risk: str
    confidence_pre: float = 0.0
    dol_match_confidence: Optional[float] = None
    static_done: bool = False
    list_intent: str = "enrich_only"
    # Clay-enriched fields (passed through from Clay columns)
    enriched_company_name: Optional[str] = None
    enriched_domain: Optional[str] = None
    enriched_industry: Optional[str] = None
    enriched_employee_count: Optional[str] = None
    enriched_first_name: Optional[str] = None
    enriched_last_name: Optional[str] = None
    enriched_email: Optional[str] = None
    enriched_title: Optional[str] = None
    clay_company_confidence: Optional[float] = None
    clay_person_confidence: Optional[float] = None
    enriched_state: Optional[str] = None
    # DOL fields for cross-source agreement
    dol_business_code: Optional[str] = None
    dol_active_participants: Optional[int] = None
    dol_spons_state: Optional[str] = None
    # Row identity (used for batch review)
    row_id: Optional[str] = None
    company_name: Optional[str] = None
    person_name: Optional[str] = None
    review_reason: Optional[str] = None
    # Industry context (passed from classify flags)
    industry_detected: Optional[str] = None
    legal_entity_name: Optional[str] = None
    # DOL plan administrator — triggers free T2 contact write (spec 9.3)
    dol_plan_administrator: Optional[str] = None
    dol_plan_administrator_title: Optional[str] = None
    # Salesforce account ID (already written by T1 SF write) — links T2 contact
    sf_account_id: Optional[str] = None
    # Batch grouping — Clay sets these consistently for all rows in a list upload
    batch_id: Optional[str] = None
    list_name: Optional[str] = None


class ReviewDecision(BaseModel):
    row_id: str
    decision: str  # "approved" | "edited" | "rejected"
    edited_fields: Optional[dict] = None


class ReviewResumeRequest(BaseModel):
    batch_id: str
    decisions: List[ReviewDecision]
