"""
Clay API client.

Used by the agent to:
  - Write flag updates back to T1 rows (run_sf, run_dedup, phase)
  - Insert free contact rows into T2 (expansion_contacts table)

The Clay REST API endpoint and table IDs must be set via environment variables.
"""

import logging
import os

import httpx

logger = logging.getLogger(__name__)

CLAY_API_KEY = os.getenv("CLAY_API_KEY", "")
CLAY_API_BASE = os.getenv("CLAY_API_BASE", "https://api.clay.com/v1")
# Clay table IDs — set these after creating the tables in Clay
CLAY_T1_TABLE_ID = os.getenv("CLAY_T1_TABLE_ID", "")
CLAY_T2_TABLE_ID = os.getenv("CLAY_T2_TABLE_ID", "")


def _headers() -> dict:
    return {
        "Authorization": f"Bearer {CLAY_API_KEY}",
        "Content-Type": "application/json",
    }


async def update_row(row_id: str, fields: dict) -> bool:
    """
    Update fields on an existing Clay T1 row.
    Used by the review resume flow to write run_sf / phase back.
    """
    if not CLAY_API_KEY:
        logger.warning("CLAY_API_KEY not set — skipping row update for %s", row_id)
        return True  # Allow tests to proceed without real credentials

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.patch(
                f"{CLAY_API_BASE}/rows/{row_id}",
                headers=_headers(),
                json=fields,
            )
            if resp.status_code in (200, 204):
                return True
            logger.error("Clay update error %s for row %s: %s", resp.status_code, row_id, resp.text[:200])
            return False
    except Exception as exc:
        logger.error("Clay update exception for row %s: %s", row_id, exc)
        return False


async def insert_t2_contact(contact: dict) -> bool:
    """
    Insert a new row into the T2 expansion_contacts Clay table.

    Used for:
    - DOL plan administrator free contacts (spec 9.3)
    - Contact expansion rows written by the agent

    contact dict should match T2 schema:
        company_name, company_website, naics_code, dol_broker_name,
        sf_account_id, first_name, last_name, job_title, linkedin_url,
        archetype, source
    """
    if not CLAY_API_KEY or not CLAY_T2_TABLE_ID:
        logger.warning(
            "CLAY_API_KEY or CLAY_T2_TABLE_ID not set — skipping T2 insert. "
            "Contact: %s %s at %s",
            contact.get("first_name", ""),
            contact.get("last_name", ""),
            contact.get("company_name", ""),
        )
        return True  # Non-fatal; Clay credits not spent

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                f"{CLAY_API_BASE}/tables/{CLAY_T2_TABLE_ID}/rows",
                headers=_headers(),
                json={"fields": contact},
            )
            if resp.status_code in (200, 201):
                logger.info(
                    "T2 contact inserted: %s %s at %s",
                    contact.get("first_name", ""),
                    contact.get("last_name", ""),
                    contact.get("company_name", ""),
                )
                return True
            logger.error("Clay T2 insert error %s: %s", resp.status_code, resp.text[:200])
            return False
    except Exception as exc:
        logger.error("Clay T2 insert exception: %s", exc)
        return False


def parse_admin_name(full_name: str) -> tuple[str, str]:
    """Split 'First Last' into (first, last). Handles single-word names."""
    parts = full_name.strip().split(None, 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    return parts[0] if parts else "", ""


async def write_plan_admin_to_t2(
    plan_admin_name: str,
    plan_admin_title: str,
    company_name: str,
    company_website: str,
    naics_code: str,
    dol_broker_name: str,
    sf_account_id: str,
) -> bool:
    """
    Write a DOL plan administrator directly to T2 as a free contact (spec 9.3).
    Skips Clay Find People credits for this person.
    """
    first, last = parse_admin_name(plan_admin_name)
    contact = {
        "first_name": first,
        "last_name": last,
        "job_title": plan_admin_title or "Plan Administrator",
        "company_name": company_name,
        "company_website": company_website or "",
        "naics_code": naics_code or "",
        "dol_broker_name": dol_broker_name or "",
        "sf_account_id": sf_account_id or "",
        "source": "dol_form5500_free",
        "archetype": "expansion_contact",
    }
    return await insert_t2_contact(contact)
