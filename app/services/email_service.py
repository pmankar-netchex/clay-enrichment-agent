"""
Batch review email sender — Mailjet REST API.

Uses MAILEJET_API_KEY + MAILEJET_API_SECRET (HTTP Basic auth).
Sends one email per batch containing all rows that need human review.
"""

import json
import logging
import os

import httpx

logger = logging.getLogger(__name__)

MAILJET_API_URL = "https://api.mailjet.com/v3.1/send"


async def send_review_email(batch_payload: dict) -> bool:
    """
    Send batch review email via Mailjet REST API.

    Returns True if sent successfully.
    """
    api_key = os.getenv("MAILEJET_API_KEY", "")
    api_secret = os.getenv("MAILEJET_API_SECRET", "")
    if not api_key or not api_secret:
        logger.warning("MAILEJET_API_KEY / MAILEJET_API_SECRET not set — skipping review email")
        return False

    from_email = os.getenv("REVIEW_EMAIL_FROM", "no-reply@example.com")
    from_name = os.getenv("REVIEW_EMAIL_FROM_NAME", "GTM Enrichment Agent")
    to_email = os.getenv("REVIEW_EMAIL_TO", "")
    if not to_email:
        logger.warning("REVIEW_EMAIL_TO not set — skipping review email")
        return False

    batch_id = batch_payload.get("batch_id", "")
    review_count = batch_payload.get("review_count", 0)
    list_name = batch_payload.get("list_name", "unknown")
    agent_base = os.getenv("AGENT_BASE_URL", "https://your-agent-endpoint.example.com")
    review_ui = os.getenv("REVIEW_UI_BASE_URL", "https://your-review-ui.example.com")

    html = _build_email_html(batch_payload, agent_base, review_ui)
    subject = f"[Review Required] {review_count} rows need approval — {list_name}"

    payload = {
        "Messages": [
            {
                "From": {"Email": from_email, "Name": from_name},
                "To": [{"Email": to_email}],
                "Subject": subject,
                "HTMLPart": html,
                "CustomID": batch_id,
            }
        ]
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                MAILJET_API_URL,
                auth=(api_key, api_secret),
                headers={"Content-Type": "application/json"},
                content=json.dumps(payload),
            )
            if resp.status_code == 200:
                data = resp.json()
                status = data.get("Messages", [{}])[0].get("Status", "")
                if status == "success":
                    logger.info("Review email sent via Mailjet for batch %s", batch_id)
                    return True
                logger.error("Mailjet message status: %s — %s", status, data)
                return False
            logger.error("Mailjet error %s: %s", resp.status_code, resp.text[:300])
            return False
    except Exception as exc:
        logger.error("Email send error: %s", exc)
        return False


def _build_email_html(payload: dict, agent_base: str, review_ui: str) -> str:
    batch_id = payload.get("batch_id", "")
    rows = payload.get("rows", [])
    total_rows = payload.get("total_rows", 0)
    auto_written = payload.get("auto_written", 0)
    review_count = payload.get("review_count", len(rows))
    list_name = payload.get("list_name", "")

    approve_all_url = f"{agent_base}/api/v1/review/approve_all?batch_id={batch_id}"
    review_url = f"{review_ui}/review/{batch_id}"

    rows_html = ""
    for row in rows:
        confidence = row.get("confidence_final", 0)
        conf_color = "#e74c3c" if confidence < 0.60 else "#f39c12"
        rows_html += f"""
        <tr>
            <td style="padding:8px;border-bottom:1px solid #eee">{row.get('company_name', '')}</td>
            <td style="padding:8px;border-bottom:1px solid #eee">{row.get('person_name', '')}</td>
            <td style="padding:8px;border-bottom:1px solid #eee;color:{conf_color}">{confidence:.2f}</td>
            <td style="padding:8px;border-bottom:1px solid #eee;font-size:12px;color:#666">{row.get('review_reason', '')}</td>
        </tr>"""

    return f"""
    <html><body style="font-family:sans-serif;max-width:900px;margin:auto;padding:24px">
    <h2 style="color:#2c3e50">GTM Enrichment — Batch Review Required</h2>
    <p>
        <strong>List:</strong> {list_name}<br>
        <strong>Batch ID:</strong> {batch_id}<br>
        <strong>Total rows processed:</strong> {total_rows}<br>
        <strong>Auto-written to Salesforce:</strong> {auto_written}<br>
        <strong>Rows requiring review:</strong> {review_count}
    </p>
    <p>
        <a href="{approve_all_url}"
           style="background:#27ae60;color:white;padding:10px 20px;
                  text-decoration:none;border-radius:4px;display:inline-block;margin-right:8px">
            Approve All ({review_count} rows)
        </a>
        <a href="{review_url}"
           style="background:#2980b9;color:white;padding:10px 20px;
                  text-decoration:none;border-radius:4px;display:inline-block">
            Review Individually
        </a>
    </p>
    <h3>Rows for Review</h3>
    <table style="width:100%;border-collapse:collapse">
        <thead>
            <tr style="background:#f4f4f4">
                <th style="padding:8px;text-align:left">Company</th>
                <th style="padding:8px;text-align:left">Person</th>
                <th style="padding:8px;text-align:left">Confidence</th>
                <th style="padding:8px;text-align:left">Reason</th>
            </tr>
        </thead>
        <tbody>{rows_html}</tbody>
    </table>
    <p style="color:#999;font-size:12px;margin-top:24px">
        This batch expires in 7 days. Rows not reviewed will be logged as expired.
    </p>
    </body></html>
    """
