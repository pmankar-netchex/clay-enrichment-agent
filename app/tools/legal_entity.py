"""
Legal entity resolution tool.

Decision tree:
  - website + high DBA risk  → defer to Claygent (return run_legal_entity=True)
  - no website + high risk   → SOS web search
  - low/medium risk          → assume operating name IS legal name
"""

import logging
import re
from typing import Optional

import httpx

from app.models.responses import StaticToolResult

logger = logging.getLogger(__name__)

_SOS_SEARCH_URL = "https://api.duckduckgo.com/"


def build_sos_query(company_name: str, state: str) -> str:
    """Build a Secretary of State search query."""
    return f'"{company_name}" site:sos.{state.lower()}.gov OR "{company_name}" "{state}" secretary of state business entity'


async def _web_search(query: str) -> Optional[str]:
    """
    Minimal DuckDuckGo instant-answer search.
    Returns raw AbstractText or None.
    """
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(
                _SOS_SEARCH_URL,
                params={"q": query, "format": "json", "no_html": "1"},
            )
            data = resp.json()
            return data.get("AbstractText") or data.get("Answer") or None
    except Exception as exc:
        logger.warning("web_search error: %s", exc)
        return None


def _extract_legal_name(text: str, company_name: str) -> Optional[str]:
    """
    Attempt to extract a legal entity name from SOS search result text.
    Looks for patterns like 'Company Name LLC' or 'Company Name, Inc.'
    """
    if not text:
        return None
    # Pattern: company name fragment followed by legal suffix
    base = re.escape(company_name.split()[0])
    pattern = rf"({base}[^.,()\n]{{0,60}}(?:LLC|Inc|Corp|Ltd|LP|LLP|Co\.))"
    match = re.search(pattern, text, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return None


async def resolve_legal_entity(
    company_name: str,
    state: Optional[str],
    website: Optional[str],
    dba_risk: str,
) -> StaticToolResult:
    """
    Resolve legal entity name.

    Returns a StaticToolResult indicating next action or resolved name.
    """
    if dba_risk == "high" and website:
        # Claygent column in Clay will handle this — return a pending marker
        return StaticToolResult(
            source="claygent_pending",
            match_key_used="website",
            confidence=0.0,
            matched=False,
            fields={"run_legal_entity": True},
        )

    if dba_risk == "high" and not website and state:
        query = build_sos_query(company_name, state)
        try:
            text = await _web_search(query)
        except Exception as exc:
            logger.warning("SOS web search failed for %s: %s", company_name, exc)
            text = None
        legal_name = _extract_legal_name(text or "", company_name)
        return StaticToolResult(
            source="agent_web_search",
            match_key_used="sos_search",
            confidence=0.70 if legal_name else 0.0,
            matched=bool(legal_name),
            fields={"legal_entity_name": legal_name, "legal_entity_source": "agent_web_search"},
        )

    # Low / medium risk — assume operating name is legal name
    return StaticToolResult(
        source="assumed_operating",
        match_key_used="assumed",
        confidence=0.85,
        matched=True,
        fields={
            "legal_entity_name": company_name,
            "legal_entity_source": "assumed_operating",
        },
    )
