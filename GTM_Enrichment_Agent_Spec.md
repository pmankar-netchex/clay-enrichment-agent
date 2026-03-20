# GTM Enrichment Orchestration — System Specification

**Version:** 1.0  
**Status:** Draft  
**Scope:** Pre-intelligence agent, Clay table design, static data layer, Salesforce write flow, contact expansion

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Data Flow](#2-data-flow)
3. [Clay Tables](#3-clay-tables)
4. [Clay Sculptor Prompts](#4-clay-sculptor-prompts)
5. [Agent Specifications](#5-agent-specifications)
6. [Static Data Layer](#6-static-data-layer)
7. [Confidence Scoring Model](#7-confidence-scoring-model)
8. [Human Review Flow](#8-human-review-flow)
9. [Contact Expansion](#9-contact-expansion)
10. [Industry Configuration](#10-industry-configuration)
11. [Connections and Network Diagram](#11-connections-and-network-diagram)
12. [Error Handling](#12-error-handling)
13. [Build Order](#13-build-order)

---

## 1. System Overview

### Purpose

Automate GTM list enrichment across seven target industries by replacing manual Clay flow configuration with an intelligent pre-processing layer. The agent determines the correct enrichment strategy per row, pulls from free static data sources first, then gates expensive Clay enrichment to only what is needed.

### What this system does

- Accepts any uploaded list regardless of schema or column structure
- Classifies each row by archetype and enrichment need
- Queries pre-built static data (DOL Form 5500, NAICS, others) before spending Clay credits
- Outputs structured flags that drive Clay's conditional enrichment columns
- Scores confidence after enrichment and routes low-confidence rows to batch human review
- Triggers contact expansion when requested
- Writes approved records to Salesforce via existing Clay functions

### What this system does NOT do

- Does not replace Clay enrichment functions — it instructs them
- Does not replace the existing dedup Clay function
- Does not replace the existing Salesforce write Clay function
- Does not manage outreach sequences

### Target industries

| Industry | NAICS prefix | DBA risk |
|---|---|---|
| Auto Dealerships | 4411 | High |
| Hotels & Motels (excl. casinos) | 7211 | High |
| Food & Drinking Places | 722 | High |
| Entertainment & Leisure | 7131–7139 | Medium |
| Fitness & Recreational Sports Centers | 7139 | Medium |
| Physician's Offices & Outpatient Centers | 621 | Low |
| Production & Manufacturing | 31–33 | Low |

---

## 2. Data Flow

### 2.1 High-level flow

```
User uploads list to Clay (any schema)
        │
        ▼
[Clay] Webhook fires per row → Agent Call 1
        │
        ├─ Agent normalises nulls, classifies archetype, calls static tools
        │  Static tools: DOL Form 5500, NAICS lookup, others (parallel)
        │
        ▼
[Clay] Flag columns written to row
        │
        ▼
[Clay] Enrichment Phase 1 (only_run_if = phase flags)
   Company resolve │ Person resolve
        │
        ├─ Archetype B only: company now known → Webhook → Agent Call 2
        │  Agent runs static tools with resolved company name
        │  Writes static results + updates phase to "enrich"
        │
        ▼
[Clay] Enrichment Phase 2 (only_run_if = phase = "enrich")
   Contacts │ Email │ Claygent legal entity │ Detail
        │
        ▼
[Clay] HTTP API push → Agent Call 3 (synthesis)
        │
        ├─ Confidence ≥ threshold → run_dedup = true, run_sf = true
        │       │
        │       ▼
        │  [Clay] Dedup function → SF write function
        │
        └─ Confidence < threshold → batch_review_pending = true
                │
                ▼
           [Email] Batch review sent to user
                │
                ▼
           [User] Approves / edits / rejects
                │
                ▼
           [FastAPI] Resume endpoint called
                │
                ▼
           [Clay] run_sf = true → SF write function
```

### 2.2 List intent modes

Set by uploader as a column value at upload time. Defaults to `enrich_only`.

| Mode | Meaning |
|---|---|
| `enrich_only` | Enrich records in the list. No new contact discovery. |
| `enrich_then_expand` | Enrich first, then use enriched company data to find additional contacts |
| `expand_then_enrich` | Find contacts at each company first, then enrich all found contacts |

### 2.3 Archetype classification

Agent Call 1 assigns every row one of the following archetypes based on which signals are present after null normalisation:

| Archetype | Signals present | Static lookup timing |
|---|---|---|
| `A` — company + person | company_name + person fields | Agent Call 1 (immediate) |
| `B` — person only | person fields, no company | Agent Call 2 (after Clay resolves company) |
| `C` — company only | company_name, no person | Agent Call 1 (immediate) |
| `fein_list` | FEIN / EIN present | Agent Call 1 exact match |
| `dol_native` | DOL schema detected (ein + plan_name + business_code) | Skip DOL lookup — already source data |
| `intent_contact` | linkedin_url + intent_source present | Agent Call 1 if company present, else Call 2 |
| `sparse` | Insufficient signal for classification | Route to human review immediately |

### 2.4 Phase state machine

The `phase` column on each Clay row tracks execution state. Agent calls read and write this column.

```
null
  → strategy_set     (after Agent Call 1 writes flags)
  → find_company     (Archetype B: Clay needs to resolve company first)
  → enrich           (ready for Clay Phase 2 enrichment)
  → scored           (after Agent Call 3 completes)
  → review_pending   (confidence below threshold)
  → approved         (human approved via review email)
  → done             (SF write completed)
  → rejected         (human rejected in review)
  → skipped          (sparse archetype, insufficient signal)
```

---

## 3. Clay Tables

### 3.1 Table inventory

| # | Table name | Purpose | Webhook source | Writes to |
|---|---|---|---|---|
| T1 | `upload_processing` | Main enrichment pipeline. Receives uploaded lists. Passthrough mode. | Upload trigger | T4 (log), SF via function |
| T2 | `expansion_contacts` | Contacts found via Find People from T1. Separate enrichment flow. | Write from T1 | T4 (log), SF via function |
| T3 | `review_queue` | Snapshot of rows pending human review. Read-only view for audit. | Written from agent | — |
| T4 | `enrichment_log` | Append-only audit trail. Every row processed writes a log record. | Written from T1 and T2 | — |

---

### 3.2 T1 — upload_processing

**Mode:** Passthrough (ingest via webhook, enrich, push to SF, delete rows)  
**Webhook fires:** Agent Call 1 on every new row  
**HTTP API column fires:** Agent Call 2 (Archetype B, mid-flow), Agent Call 3 (final)

#### Raw input columns (from upload — variable per list)

These are whatever the user uploads. The agent reads them as-is after null normalisation. No fixed schema required.

#### Agent flag columns (written by agents)

| Column | Type | Written by | Values / format |
|---|---|---|---|
| `phase` | Text | Agent 1, 2, 3 | See phase state machine above |
| `archetype` | Text | Agent 1 | `A`, `B`, `C`, `fein_list`, `dol_native`, `intent_contact`, `sparse` |
| `list_intent` | Text | Uploader (set at upload) | `enrich_only`, `enrich_then_expand`, `expand_then_enrich` |
| `dba_risk` | Text | Agent 1 | `high`, `medium`, `low` |
| `industry_detected` | Text | Agent 1 | NAICS label or `unknown` |
| `static_done` | Boolean | Agent 1 or 2 | TRUE / FALSE |
| `confidence_pre` | Number | Agent 1 or 2 | 0.0–1.0 |
| `confidence_final` | Number | Agent 3 | 0.0–1.0 |
| `confidence_breakdown` | JSON text | Agent 3 | `{"fein_match":1,"clay_score":0.8,...}` |
| `review_reason` | Text | Agent 3 | Plain text explanation if low confidence |
| `batch_id` | Text | Agent 3 | UUID stamped on every write batch |

#### Run control columns (read by Clay column conditions)

| Column | Type | Written by | Purpose |
|---|---|---|---|
| `run_company_enrich` | Boolean | Agent 1 | Gates company enrichment columns |
| `run_person_lookup` | Boolean | Agent 1 | Gates person/contact enrichment columns |
| `run_legal_entity` | Boolean | Agent 1 | Gates Claygent legal entity column |
| `run_contacts_search` | Boolean | Agent 1 | Gates Find People column |
| `run_people_expansion` | Boolean | Agent 1 | Gates expansion trigger column |
| `run_dedup` | Boolean | Agent 3 | Gates dedup Clay function |
| `run_sf` | Boolean | Agent 3 | Gates SF write Clay function |
| `skip_company_size` | Boolean | Agent 1 | Prevents re-enriching already-present headcount |
| `skip_industry` | Boolean | Agent 1 | Prevents re-enriching already-present industry |
| `skip_revenue` | Boolean | Agent 1 | Prevents re-enriching already-present revenue band |

#### Static data result columns (written by agents)

| Column | Type | Source |
|---|---|---|
| `dol_sponsor_name` | Text | DOL Form 5500 |
| `dol_broker_name` | Text | DOL Form 5500 |
| `dol_broker_ein` | Text | DOL Form 5500 |
| `dol_cpa_name` | Text | DOL Form 5500 |
| `dol_plan_name` | Text | DOL Form 5500 |
| `dol_match_confidence` | Number | DOL Form 5500 |
| `dol_plan_administrator` | Text | DOL Form 5500 (free contact) |
| `naics_code` | Text | NAICS lookup |
| `naics_sector` | Text | NAICS lookup |
| `legal_entity_name` | Text | Legal entity resolution |
| `legal_entity_source` | Text | `claygent`, `agent_web_search`, `fein_exact` |

#### Clay enrichment columns (configured by you, gated by run flags)

| Column | Enrichment source | Run condition |
|---|---|---|
| Company enrichment | Apollo / Clearbit / etc. | `run_company_enrich = true` |
| Person lookup | LinkedIn / Apollo | `run_person_lookup = true AND phase = "find_company"` |
| Legal entity | Claygent | `run_legal_entity = true AND website IS NOT NULL` |
| Find People | Apollo Find People | `run_contacts_search = true AND phase = "enrich"` |
| Email find | Hunter / Apollo | `phase = "enrich"` |
| Agent Call 2 trigger | HTTP API → agent | `phase = "find_company" AND company_name IS NOT NULL` |
| Agent Call 3 trigger | HTTP API → agent | `phase = "enrich" AND all enrichment columns settled` |
| Dedup | `fn_dedup_person` / `fn_dedup_company` | `run_dedup = true` |
| SF write | SF write function | `run_sf = true AND is_duplicate = false` |

> **Column ordering is execution order.** Place agent trigger columns (HTTP API) as the last column in each phase group. Clay executes left to right.

---

### 3.3 T2 — expansion_contacts

Receives rows written from T1 via Clay's "write to other table" feature when `run_people_expansion = true`.

| Column | Source | Notes |
|---|---|---|
| `company_name` | Inherited from T1 | Already confirmed |
| `company_website` | Inherited from T1 | Already confirmed |
| `naics_code` | Inherited from T1 | Already enriched |
| `dol_broker_name` | Inherited from T1 | Already enriched |
| `sf_account_id` | Inherited from T1 | Links contact to correct SF account |
| `first_name` | Found by Find People | |
| `last_name` | Found by Find People | |
| `job_title` | Found by Find People | |
| `linkedin_url` | Found by Find People | |
| `phase` | Agent | Same state machine as T1 |
| `run_dedup` | Agent | Gates dedup before SF write |
| `run_sf` | Agent | Gates SF write |
| `archetype` | Set to `expansion_contact` | Fixed — no classification needed |

**Enrichment columns in T2:**

| Column | Run condition |
|---|---|
| Email find | Always (this is why we're expanding) |
| Enrich person | Always |
| Mobile waterfall (`fn_waterfall_contact`) | After email confirmed |
| Dedup (`fn_dedup_person`) | Before SF write |
| SF write contact | `run_sf = true AND is_duplicate = false` |

---

### 3.4 T4 — enrichment_log

Append-only. Written via HTTP API from T1 and T2 after each row completes.

| Column | Description |
|---|---|
| `log_timestamp` | ISO 8601 UTC |
| `source_table` | `upload_processing` or `expansion_contacts` |
| `batch_id` | Links to T1 batch |
| `list_name` | Name of uploaded list |
| `archetype` | Row archetype |
| `phase_final` | Final phase value |
| `confidence_final` | Final confidence score |
| `review_decision` | `auto`, `approved`, `edited`, `rejected` |
| `static_source_used` | Which static tools returned results |
| `clay_enrichments_run` | Which enrichment columns fired |
| `sf_outcome` | `created`, `updated`, `skipped_duplicate`, `skipped_review` |

---

## 4. Clay Sculptor Prompts

The following Sculptor prompts build T1 from scratch. Run one block at a time. Wait for confirmation before sending the next block.

---

### 4.1 T1 Block 1 — Clean input fields

```
You are a Clay table schema designer. Add the following columns to the table.
For every column, the source field is explicitly named — use ONLY that field,
do not substitute or infer alternatives.

The table already has these raw input columns (names will vary per uploaded list —
these are the normalised versions to produce for downstream use):

---

Create column: cleaned_company_name
- Type: Formula
- Source column: whichever column contains the company or organisation name
  (not the person name column, not any address column)
- Formula: Trim whitespace. Title Case. If value is exactly "undefined", "null",
  "none", "n/a", or empty string, return null.

Create column: cleaned_first_name
- Type: Formula
- Source column: whichever column contains the contact's first name
  (not the last name column, not the company name column)
- Formula: Trim whitespace. Title Case. If value is "undefined" or empty, return null.

Create column: cleaned_last_name
- Type: Formula
- Source column: whichever column contains the contact's last name
  (not the first name column, not the company name column)
- Formula: Trim whitespace. Title Case. If value is "undefined" or empty, return null.

Create column: cleaned_email
- Type: Formula
- Source column: whichever column contains an email address
  (not the phone column, not any LinkedIn column)
- Formula: Lowercase. Trim whitespace. If value is "undefined" or empty, return null.

Create column: cleaned_website
- Type: Formula
- Source column: whichever column contains a company website or domain
  (not the LinkedIn URL column, not the email column)
- Formula: Strip protocol (https://, http://), strip www., strip any path after
  the root domain, strip trailing slash. Lowercase. If "undefined" or empty, return null.

Create column: cleaned_linkedin_person
- Type: Formula
- Source column: whichever column contains a person LinkedIn URL
  (must contain /in/ — not a company LinkedIn with /company/)
- Formula: Lowercase. Trim. Remove trailing slash.
  If value does not contain "/in/", return null.
  If "undefined" or empty, return null.

Create column: cleaned_fein
- Type: Formula
- Source column: whichever column is named ein, fein, federal_id, federal_id_clean,
  or similar (not the company name column, not any phone column)
- Formula: Strip all non-numeric characters. If result is 9 digits, format as
  XX-XXXXXXX (insert dash after position 2). If not 9 digits after stripping, return null.
  If column does not exist in this table, return null.

Create column: cleaned_state
- Type: Formula
- Source column: whichever column contains a US state abbreviation or name
  (not the city column, not the zip column)
- Formula: Trim. Uppercase. If value is a full state name, convert to 2-letter abbreviation.
  If "undefined" or empty, return null.
```

---

### 4.2 T1 Block 2 — Routing flags

```
You are a Clay table schema designer. Add the following columns to the table.
For every column, the source field is explicitly named — use ONLY that field,
do not substitute or infer alternatives.

The table already has these columns (use these names verbatim):
cleaned_company_name, cleaned_first_name, cleaned_last_name, cleaned_email,
cleaned_website, cleaned_linkedin_person, cleaned_fein, cleaned_state

---

Create column: has_company
- Type: Formula
- Source column: cleaned_company_name (not any raw column, not cleaned_first_name)
- Formula: Return TRUE if cleaned_company_name is not null and not empty. Otherwise FALSE.

Create column: has_person
- Type: Formula
- Source columns: cleaned_first_name, cleaned_last_name, cleaned_linkedin_person
  (no other columns)
- Formula: Return TRUE if cleaned_first_name is not null OR cleaned_last_name is not null
  OR cleaned_linkedin_person is not null. Otherwise FALSE.

Create column: has_fein
- Type: Formula
- Source column: cleaned_fein (not any other column)
- Formula: Return TRUE if cleaned_fein is not null. Otherwise FALSE.

Create column: is_dol_native
- Type: Formula
- Source columns: check whether columns named ein, plan_name, business_code all exist
  in this table (not any cleaned column)
- Formula: Return TRUE if all three of those raw column names are present and non-null
  for this row. Otherwise FALSE.

Create column: has_website
- Type: Formula
- Source column: cleaned_website (not cleaned_linkedin_person, not cleaned_email)
- Formula: Return TRUE if cleaned_website is not null. Otherwise FALSE.

Create column: has_intent_signal
- Type: Formula
- Source column: whichever column is named intent_source, source, or similar
  (not the email column, not any name column)
- Formula: Return TRUE if that column is not null and not "undefined". Otherwise FALSE.

Create column: list_intent
- Type: Formula
- Source column: whichever column is named list_intent or intent_mode
  (not any person or company field)
- Formula: If value is "expand_then_enrich" return "expand_then_enrich".
  If value is "enrich_then_expand" return "enrich_then_expand".
  Otherwise return "enrich_only".
```

---

### 4.3 T1 Block 3 — Skip flags (pre-enrichment guards)

```
You are a Clay table schema designer. Add the following columns to the table.
For every column, the source field is explicitly named — use ONLY that field,
do not substitute or infer alternatives.

The table already has these columns (use these names verbatim):
has_company, has_person, has_fein, is_dol_native, has_website, list_intent

---

Create column: skip_company_size
- Type: Formula
- Source column: whichever column contains employee count, headcount, or employee_count_band
  (not revenue, not any name column)
- Formula: Return TRUE if that column is not null, not "undefined", not empty.
  Otherwise FALSE. (If column does not exist, return FALSE.)

Create column: skip_industry
- Type: Formula
- Source column: whichever column contains industry or sector information
  (not NAICS code column, not company name)
- Formula: Return TRUE if that column is not null, not "undefined", not empty.
  Otherwise FALSE. (If column does not exist, return FALSE.)

Create column: skip_revenue
- Type: Formula
- Source column: whichever column contains revenue or revenue_band
  (not employee count, not any name column)
- Formula: Return TRUE if that column is not null, not "undefined", not empty.
  Otherwise FALSE. (If column does not exist, return FALSE.)

Create column: should_process
- Type: Formula
- Source columns: has_company, has_person, has_fein, is_dol_native (no other columns)
- Formula: Return TRUE if any of the following is TRUE:
  has_company is TRUE, has_person is TRUE, has_fein is TRUE.
  Return FALSE only if all of has_company, has_person, has_fein are FALSE
  AND is_dol_native is FALSE.
```

---

### 4.4 T1 Block 4 — Agent Call 1 trigger (HTTP API)

```
You are a Clay table schema designer. Add the following columns to the table.
For every column, the source field is explicitly named — use ONLY that field,
do not substitute or infer alternatives.

The table already has these columns (use these names verbatim):
should_process, cleaned_company_name, cleaned_first_name, cleaned_last_name,
cleaned_email, cleaned_website, cleaned_fein, cleaned_state, cleaned_linkedin_person,
has_company, has_person, has_fein, is_dol_native, has_website, has_intent_signal,
list_intent, skip_company_size, skip_industry, skip_revenue

---

Create column: agent_call_1_result
- Type: HTTP API (POST)
- Run this column only if: should_process is TRUE
- Endpoint: https://[YOUR_AGENT_ENDPOINT]/api/v1/classify
- Method: POST
- Body (JSON): Build the request body using exactly these column names as values:
    - "company_name" → cleaned_company_name
    - "first_name" → cleaned_first_name
    - "last_name" → cleaned_last_name
    - "email" → cleaned_email
    - "website" → cleaned_website
    - "fein" → cleaned_fein
    - "state" → cleaned_state
    - "linkedin_person" → cleaned_linkedin_person
    - "has_company" → has_company
    - "has_person" → has_person
    - "has_fein" → has_fein
    - "is_dol_native" → is_dol_native
    - "has_website" → has_website
    - "has_intent_signal" → has_intent_signal
    - "list_intent" → list_intent
    - "skip_company_size" → skip_company_size
    - "skip_industry" → skip_industry
    - "skip_revenue" → skip_revenue
- Expected output: JSON object. Map the following response fields back to Clay columns:
    - response.phase → phase
    - response.archetype → archetype
    - response.dba_risk → dba_risk
    - response.industry_detected → industry_detected
    - response.run_company_enrich → run_company_enrich
    - response.run_person_lookup → run_person_lookup
    - response.run_legal_entity → run_legal_entity
    - response.run_contacts_search → run_contacts_search
    - response.run_people_expansion → run_people_expansion
    - response.dol_sponsor_name → dol_sponsor_name
    - response.dol_broker_name → dol_broker_name
    - response.dol_cpa_name → dol_cpa_name
    - response.dol_plan_administrator → dol_plan_administrator
    - response.naics_code → naics_code
    - response.legal_entity_name → legal_entity_name
    - response.confidence_pre → confidence_pre
    - response.static_done → static_done
```

---

### 4.5 T1 Block 5 — Agent Call 2 trigger (Archetype B mid-flow)

```
You are a Clay table schema designer. Add the following columns to the table.
For every column, the source field is explicitly named — use ONLY that field,
do not substitute or infer alternatives.

The table already has these columns (use these names verbatim):
phase, archetype, should_process, cleaned_company_name, cleaned_state,
cleaned_website, dba_risk, industry_detected

This column must appear AFTER all phase-1 enrichment columns (company resolve,
person lookup). It fires only when Clay has resolved the company name for
Archetype B rows.

---

Create column: agent_call_2_result
- Type: HTTP API (POST)
- Run this column only if: phase is exactly "find_company"
  AND cleaned_company_name is not null
- Endpoint: https://[YOUR_AGENT_ENDPOINT]/api/v1/static_lookup
- Method: POST
- Body (JSON):
    - "company_name" → cleaned_company_name
    - "state" → cleaned_state
    - "website" → cleaned_website
    - "dba_risk" → dba_risk
    - "industry_detected" → industry_detected
    - "archetype" → archetype
- Expected output: Map response fields back:
    - response.phase → phase
    - response.dol_sponsor_name → dol_sponsor_name
    - response.dol_broker_name → dol_broker_name
    - response.dol_cpa_name → dol_cpa_name
    - response.dol_plan_administrator → dol_plan_administrator
    - response.naics_code → naics_code
    - response.legal_entity_name → legal_entity_name
    - response.dol_match_confidence → dol_match_confidence
    - response.static_done → static_done
```

---

### 4.6 T1 Block 6 — Agent Call 3 trigger (synthesis, final column)

```
You are a Clay table schema designer. Add the following columns to the table.
For every column, the source field is explicitly named — use ONLY that field,
do not substitute or infer alternatives.

The table already has these columns (use these names verbatim):
phase, archetype, static_done, dba_risk, dol_broker_name, dol_cpa_name,
dol_match_confidence, naics_code, confidence_pre, list_intent

This column must be the LAST column in the table. It fires only after all
enrichment columns have completed.

---

Create column: agent_call_3_result
- Type: HTTP API (POST)
- Run this column only if: phase is exactly "enrich"
  AND static_done is TRUE
- Endpoint: https://[YOUR_AGENT_ENDPOINT]/api/v1/synthesise
- Method: POST
- Body (JSON): Include the fully enriched row. Map ALL enriched columns
  plus these specific fields:
    - "archetype" → archetype
    - "dba_risk" → dba_risk
    - "confidence_pre" → confidence_pre
    - "dol_match_confidence" → dol_match_confidence
    - "static_done" → static_done
    - "list_intent" → list_intent
    Include all Clay-enriched person and company fields from phase 2 columns.
- Expected output:
    - response.confidence_final → confidence_final
    - response.confidence_breakdown → confidence_breakdown
    - response.review_reason → review_reason
    - response.run_dedup → run_dedup
    - response.run_sf → run_sf
    - response.phase → phase
    - response.batch_id → batch_id
```

---

## 5. Agent Specifications

### 5.1 Runtime

- **Language:** Python 3.11+
- **Framework:** FastAPI
- **Hosting:** Any server with HTTPS endpoint reachable by Clay webhooks
- **Static data:** DuckDB database file loaded at startup
- **Async:** `asyncio.gather()` for parallel static tool calls

### 5.2 Agent Call 1 — Schema Classification and Strategy

**Endpoint:** `POST /api/v1/classify`  
**Trigger:** Clay T1 webhook, every row where `should_process = true`  
**Expected response time:** < 3 seconds (Clay webhook timeout)

#### Input schema

```python
class ClassifyRequest(BaseModel):
    company_name: Optional[str]
    first_name: Optional[str]
    last_name: Optional[str]
    email: Optional[str]
    website: Optional[str]
    fein: Optional[str]
    state: Optional[str]
    linkedin_person: Optional[str]
    has_company: bool
    has_person: bool
    has_fein: bool
    is_dol_native: bool
    has_website: bool
    has_intent_signal: bool
    list_intent: str  # "enrich_only" | "enrich_then_expand" | "expand_then_enrich"
    skip_company_size: bool
    skip_industry: bool
    skip_revenue: bool
```

#### Processing steps (in order)

1. **Null normalisation** — already done by Clay Block 1 formulas. No-op here; validate inputs.
2. **Source type detection** — detect `dol_native` from `is_dol_native` flag.
3. **Archetype classification** — apply decision tree (see section 2.3).
4. **Industry inference** — match company name patterns to industry config table (section 10).
5. **DBA risk assignment** — look up `dba_risk` from industry config.
6. **Static tool calls** — call all applicable tools in parallel via `asyncio.gather()`.
   - Only call if `has_company = true` OR `has_fein = true`
   - Skip if `is_dol_native = true`
7. **Flag construction** — build run flags based on archetype and static results.
8. **Phase assignment** — set `phase`:
   - Archetype A/C/fein_list/dol_native: `"strategy_set"` if static run, else `"enrich"`
   - Archetype B: `"find_company"`
   - Archetype sparse: `"skipped"`
9. **Return** — write all flags and static results to response.

#### Output schema

```python
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
    # Static data results (null if not found)
    dol_sponsor_name: Optional[str]
    dol_broker_name: Optional[str]
    dol_cpa_name: Optional[str]
    dol_plan_administrator: Optional[str]
    dol_match_confidence: Optional[float]
    naics_code: Optional[str]
    naics_sector: Optional[str]
    legal_entity_name: Optional[str]
    legal_entity_source: Optional[str]
```

#### Flag construction rules

```python
# run_company_enrich
run_company_enrich = (
    has_company
    and archetype in ["A", "C", "fein_list"]
    and not is_dol_native
)

# run_person_lookup
run_person_lookup = (
    has_person
    and not has_company  # Archetype B needs company resolved first
)

# run_legal_entity
run_legal_entity = (
    has_company
    and dba_risk == "high"
    and legal_entity_name is None  # DOL didn't already resolve it
)

# run_contacts_search
run_contacts_search = (
    list_intent in ["enrich_then_expand", "expand_then_enrich"]
    or (archetype == "C")  # Company-only list always wants contacts
)

# run_people_expansion
run_people_expansion = list_intent in ["enrich_then_expand", "expand_then_enrich"]
```

#### Idempotency guard

Check if `static_done` is already `true` in the incoming request. If so, return existing
static results unchanged without re-querying DuckDB.

---

### 5.3 Agent Call 2 — Static Lookup (Archetype B)

**Endpoint:** `POST /api/v1/static_lookup`  
**Trigger:** Clay T1 HTTP API column, fires when `phase = "find_company"` AND `company_name` is resolved  
**Expected response time:** < 3 seconds

#### Input schema

```python
class StaticLookupRequest(BaseModel):
    company_name: str        # Now resolved by Clay — not null
    state: Optional[str]
    website: Optional[str]
    dba_risk: str
    industry_detected: str
    archetype: str
```

#### Processing steps

1. Call all static tools in parallel with resolved `company_name`.
2. If `dba_risk = "high"` and `website` is present, flag `run_legal_entity = true`
   (Claygent column will handle it).
3. Set `phase = "enrich"` and `static_done = true`.
4. Return static results.

#### Output schema

Same shape as static fields in ClassifyResponse, plus:

```python
phase: str  # Always "enrich"
static_done: bool  # Always True
```

---

### 5.4 Agent Call 3 — Synthesis and Confidence Scoring

**Endpoint:** `POST /api/v1/synthesise`  
**Trigger:** Clay T1 HTTP API column (last column), fires when `phase = "enrich"`  
**Expected response time:** < 2 seconds (no external calls — pure computation)

#### Input schema

```python
class SynthesiseRequest(BaseModel):
    archetype: str
    dba_risk: str
    confidence_pre: float
    dol_match_confidence: Optional[float]
    static_done: bool
    list_intent: str
    # All Clay-enriched fields passed through
    enriched_company_name: Optional[str]
    enriched_domain: Optional[str]
    enriched_industry: Optional[str]
    enriched_employee_count: Optional[str]
    enriched_first_name: Optional[str]
    enriched_last_name: Optional[str]
    enriched_email: Optional[str]
    enriched_title: Optional[str]
    clay_company_confidence: Optional[float]
    clay_person_confidence: Optional[float]
    # ... all other enriched columns
```

#### Processing steps

1. Compute composite confidence score (see section 7).
2. Check against threshold config.
3. Build `run_dedup` and `run_sf` flags.
4. If low confidence, build `review_reason` text.
5. Generate `batch_id` (UUID4).
6. Set `phase` to `"scored"` (high confidence) or `"review_pending"` (low confidence).
7. If `phase = "review_pending"`, add row to batch review queue.
8. Write to log table (async, non-blocking).

#### Output schema

```python
class SynthesiseResponse(BaseModel):
    confidence_final: float
    confidence_breakdown: dict
    review_reason: Optional[str]
    run_dedup: bool
    run_sf: bool
    phase: str  # "scored" or "review_pending"
    batch_id: str
```

---

### 5.5 Review Resume Endpoint

**Endpoint:** `POST /api/v1/review/resume`  
**Trigger:** User clicks approve/edit link in review email  
**Purpose:** Write human decisions back to Clay row flags, enabling dedup and SF write to run

#### Input schema

```python
class ReviewResumeRequest(BaseModel):
    batch_id: str
    decisions: List[ReviewDecision]

class ReviewDecision(BaseModel):
    row_id: str
    decision: str  # "approved" | "edited" | "rejected"
    edited_fields: Optional[dict]  # Only present if decision = "edited"
```

#### Processing steps

1. Validate `batch_id` against stored batch (prevent replay).
2. For each decision:
   - `approved` → write `run_sf = true`, `run_dedup = true`, `phase = "approved"` back to Clay row
   - `edited` → re-run confidence scoring on edited fields, then set flags
   - `rejected` → write `phase = "rejected"`, `run_sf = false`
3. Write decisions to T4 log.

---

## 6. Static Data Layer

### 6.1 Architecture

All static sources live in a single DuckDB database file (`static_data.duckdb`) loaded at agent startup. Each source is a separate table. Each source has a dedicated Python tool function. All tools return the same envelope schema.

```python
@dataclass
class StaticToolResult:
    source: str              # e.g. "dol_form5500"
    match_key_used: str      # e.g. "name+state"
    confidence: float        # 0.0–1.0
    matched: bool
    fields: dict             # Source-specific fields
    matched_record_id: Optional[str]
```

### 6.2 Tool: find_in_dol

**Source:** DOL EFAST2 Form 5500 dataset (annual download)  
**DuckDB table:** `dol_form5500`  
**Key fields:** `sponsor_name`, `spons_state`, `spons_zip`, `ein`, `plan_name`,
`plan_admin_name`, `plan_admin_title`, `broker_name`, `broker_ein`, `cpa_name`,
`business_code`, `welfare_codes`, `pension_codes`

#### Preprocessing (run once on data load)

```python
# Applied to sponsor_name when loading into DuckDB
def normalise_company_name(name: str) -> str:
    # 1. Lowercase
    # 2. Strip legal suffixes: llc, inc, corp, ltd, lp, llp, co, incorporated, limited
    # 3. Strip plan suffixes: pension plan, 401k plan, retirement plan,
    #    health & welfare plan, group insurance plan, profit sharing plan
    # 4. Expand abbreviations: mfg→manufacturing, svcs→services,
    #    assoc→associates, mgmt→management, hosp→hospitality
    # 5. Remove punctuation
    # 6. Collapse multiple spaces
    return normalised
```

#### Query logic

```python
async def find_in_dol(
    name: str,
    state: Optional[str] = None,
    city: Optional[str] = None,
    naics_hint: Optional[str] = None,
    fein: Optional[str] = None
) -> StaticToolResult:

    # Path 1: FEIN exact match (highest confidence)
    if fein:
        result = duckdb.execute(
            "SELECT * FROM dol_form5500 WHERE ein = ? LIMIT 1", [fein]
        ).fetchone()
        if result:
            return StaticToolResult(confidence=0.98, match_key_used="fein_exact", ...)

    # Path 2: Fuzzy name match with blocking
    normalised_input = normalise_company_name(name)
    
    # Blocking: restrict candidates by state and first 2 tokens
    tokens = normalised_input.split()[:2]
    blocking_prefix = " ".join(tokens)
    
    candidates = duckdb.execute("""
        SELECT *, jaro_winkler_similarity(normalised_name, ?) as name_score
        FROM dol_form5500
        WHERE (spons_state = ? OR ? IS NULL)
          AND normalised_name LIKE ?
        ORDER BY name_score DESC
        LIMIT 200
    """, [normalised_input, state, state, f"{tokens[0]}%"]).fetchall()

    # Score each candidate
    best = score_candidates(candidates, normalised_input, state, city, naics_hint)

    if best.score >= 0.75:
        return StaticToolResult(confidence=best.score, matched=True, ...)
    elif best.score >= 0.60:
        return StaticToolResult(confidence=best.score, matched=True,
                               fields=best.fields)
        # Note: confidence_pre will be low — routes to review
    else:
        return StaticToolResult(confidence=0.0, matched=False, fields={})
```

#### Scoring formula

```
composite_score =
    jaro_winkler(normalised_name, candidate_name) * 0.50
  + (1.0 if state matches else 0.0) * 0.20
  + (naics_prefix_match ? 0.15 : 0.0)
  + (address_token_match ? 0.15 : 0.0)
```

### 6.3 Tool: lookup_naics

**Source:** US Census NAICS code table (static file, infrequent updates)  
**DuckDB table:** `naics_codes`

```python
async def lookup_naics(
    company_name: str,
    industry_hint: Optional[str] = None
) -> StaticToolResult:
    # Match against industry keyword patterns per section 10
    # Return NAICS 6-digit code, sector, subsector, and dba_risk level
```

### 6.4 Tool: resolve_legal_entity

Not a DuckDB tool. Decision tree:

```python
async def resolve_legal_entity(
    company_name: str,
    state: str,
    website: Optional[str],
    dba_risk: str
) -> StaticToolResult:

    if website and dba_risk == "high":
        # Return instruction for Claygent column
        # Agent sets run_legal_entity = True
        # Claygent column fires in Clay with website as input
        return StaticToolResult(
            source="claygent_pending",
            confidence=0.0,  # Will be updated after Claygent runs
            matched=False
        )

    elif not website and dba_risk == "high":
        # Construct targeted SOS web search query
        query = build_sos_query(company_name, state)
        result = await web_search(query)
        legal_name = extract_legal_name(result)
        return StaticToolResult(
            source="agent_web_search",
            confidence=0.70 if legal_name else 0.0,
            fields={"legal_entity_name": legal_name}
        )

    else:
        # Low DBA risk — operating name likely IS legal name
        return StaticToolResult(
            source="assumed_operating",
            confidence=0.85,
            fields={"legal_entity_name": company_name}
        )
```

### 6.5 Adding a new static source

To add a new static data source (e.g. SEC EDGAR, state SOS registry):

1. Download dataset and convert to Parquet.
2. Load into DuckDB: `CREATE TABLE new_source AS SELECT * FROM 'source.parquet'`.
3. Write a Python tool function matching the `StaticToolResult` return shape.
4. Add to the parallel tool list in Agent Call 1 and Agent Call 2.
5. Add output fields to Agent Call 1 and 2 response schemas.
6. Add corresponding Clay columns to T1.

No changes to the synthesiser or confidence model needed.

---

## 7. Confidence Scoring Model

### 7.1 Input signals

| Signal | Weight | How measured |
|---|---|---|
| FEIN exact match | 0.35 | `1.0` if FEIN matched DOL, else `0.0` |
| Clay company confidence | 0.25 | Score returned by Clay's company enrichment provider |
| Cross-source agreement | 0.20 | Agreement between DOL fields and Clay-enriched fields |
| Record completeness | 0.10 | Fraction of required SF fields populated |
| DOL fuzzy match confidence | 0.10 | Score from DOL fuzzy matching |

### 7.2 Composite formula

```python
def compute_confidence(signals: dict) -> tuple[float, dict]:
    weights = {
        "fein_match": 0.35,
        "clay_company_confidence": 0.25,
        "cross_source_agreement": 0.20,
        "completeness": 0.10,
        "dol_fuzzy_confidence": 0.10,
    }

    breakdown = {}
    for key, weight in weights.items():
        value = signals.get(key, 0.0)
        breakdown[key] = round(value * weight, 3)

    composite = sum(breakdown.values())
    return round(composite, 3), breakdown
```

### 7.3 Cross-source agreement calculation

```python
def cross_source_agreement(clay_fields: dict, dol_fields: dict) -> float:
    checks = []
    
    # Industry/NAICS agreement
    if clay_fields.get("industry") and dol_fields.get("business_code"):
        clay_naics = map_industry_to_naics(clay_fields["industry"])
        checks.append(1.0 if clay_naics[:3] == dol_fields["business_code"][:3] else 0.0)
    
    # Employee count vs plan participant count (within plausible range)
    if clay_fields.get("employee_count") and dol_fields.get("active_participants"):
        clay_count = parse_count_band(clay_fields["employee_count"])
        dol_count = int(dol_fields["active_participants"])
        checks.append(1.0 if abs(clay_count - dol_count) / max(clay_count, 1) < 0.5 else 0.3)
    
    # State match
    if clay_fields.get("state") and dol_fields.get("spons_state"):
        checks.append(1.0 if clay_fields["state"] == dol_fields["spons_state"] else 0.0)
    
    return sum(checks) / len(checks) if checks else 0.5  # Default 0.5 if no checks
```

### 7.4 Thresholds

| Score range | Action |
|---|---|
| ≥ 0.80 | Auto-write: `run_dedup = true`, `run_sf = true` |
| 0.60–0.79 | Human review: `phase = "review_pending"` |
| < 0.60 | Human review with `review_reason` explanation |

### 7.5 Industry confidence ceiling

High DBA-risk industries have a lower auto-write ceiling because the company name may not
be the legal entity:

| Industry | Confidence ceiling (auto-write) | Requires legal entity resolution first |
|---|---|---|
| Auto Dealerships | 0.85 | Yes |
| Hotels & Motels | 0.85 | Yes |
| Food & Drinking Places | 0.85 | Yes |
| Entertainment & Leisure | 0.90 | No |
| Fitness Centers | 0.90 | No |
| Physician's Offices | 0.95 | No |
| Manufacturing | 0.95 | No |

---

## 8. Human Review Flow

### 8.1 Batch collection

After Agent Call 3, all `phase = "review_pending"` rows within a processing batch
(same `batch_id`) are collected into a review payload. The review email is sent once
per batch, not per row.

### 8.2 Review email payload

```json
{
  "batch_id": "uuid-here",
  "list_name": "hospitality_dec_2025.csv",
  "total_rows": 847,
  "auto_written": 612,
  "review_count": 235,
  "rows": [
    {
      "row_id": "clay_row_id",
      "company_name": "Americas Best Value Inn",
      "person_name": "Bhupendra Bivek",
      "confidence_final": 0.63,
      "review_reason": "DOL match confidence 0.61 — company may be franchise operator under different legal name. Clay confirmed 'Americas Best Value Inn' but no FEIN available for verification.",
      "enriched_fields": { ... },
      "suggested_action": "approve"
    }
  ],
  "approve_all_url": "https://[YOUR_ENDPOINT]/api/v1/review/resume?batch_id=uuid&action=approve_all",
  "review_url": "https://[YOUR_REVIEW_UI]/review/uuid"
}
```

### 8.3 Review actions

| Action | Effect |
|---|---|
| Approve | `run_sf = true`, `run_dedup = true`, `phase = "approved"` |
| Edit | Human corrects field values → re-score → then approve |
| Reject | `phase = "rejected"`, `run_sf = false`, logged |
| Approve all | All rows in batch get approved without individual review |

### 8.4 Resume endpoint

`POST /api/v1/review/resume` accepts `batch_id` and array of per-row decisions.
For each decision, it writes the appropriate flags back to the Clay row via Clay's API.
This triggers the dedup and SF write columns which have been waiting on `run_dedup = false`.

---

## 9. Contact Expansion

### 9.1 When expansion runs

Expansion is triggered by `run_people_expansion = true`, set by Agent Call 1 based on `list_intent`.

- `expand_then_enrich`: Clay Find People column fires immediately after phase 1 enrichment confirms the company. Results written to T2 before T1 SF write.
- `enrich_then_expand`: T1 completes fully (including SF write for the account). Then Find People fires and writes to T2 with `sf_account_id` already populated.
- `enrich_only`: Expansion never triggers.

### 9.2 Persona configuration

Find People filters are set per industry. The agent writes `persona_filter_json` to T1,
which the Find People column reads as its title/seniority filter input.

See section 10 for industry-specific persona definitions.

### 9.3 DOL plan administrator as free contact

When `dol_plan_administrator` is populated, write it directly to T2 as a free contact row
without spending Clay credits on Find People for that person:

```python
if dol_plan_administrator:
    write_to_t2({
        "first_name": extract_first(dol_plan_administrator),
        "last_name": extract_last(dol_plan_administrator),
        "job_title": dol_plan_administrator_title,
        "company_name": company_name,
        "sf_account_id": sf_account_id,
        "source": "dol_form5500_free",
        "archetype": "expansion_contact"
    })
```

### 9.4 T2 enrichment flow

T2 runs a simplified version of the T1 flow:

1. Dedup check (`fn_dedup_person`) — prevent creating contacts that already exist in SF
2. Email find — primary enrichment goal
3. Enrich person — LinkedIn confirmation, title verification
4. Mobile waterfall (`fn_waterfall_contact`) — after email confirmed
5. SF write — creates contact with `AccountId` linked to parent account

---

## 10. Industry Configuration

### 10.1 Industry detection patterns

Agent Call 1 infers industry from company name keywords and any provided NAICS hint.

| Industry | NAICS prefix | Company name signals | DBA risk | Confidence ceiling |
|---|---|---|---|---|
| Auto Dealerships | 4411 | ford, chevy, toyota, honda, nissan, mazda, auto, motors, motor co, automotive | high | 0.85 |
| Hotels & Motels | 7211 | hotel, inn, motel, suites, hospitality, lodging, resort | high | 0.85 |
| Food & Drinking Places | 722 | restaurant, grill, kitchen, bistro, cafe, diner, bar, eatery, pizza, burger | high | 0.85 |
| Entertainment & Leisure | 7131–7139 | entertainment, theatre, theater, cinema, arcade, bowling, golf | medium | 0.90 |
| Fitness Centers | 7139 | fitness, gym, wellness, yoga, crossfit, sport, athletic | medium | 0.90 |
| Physician's Offices | 621 | medical, healthcare, health care, clinic, physician, doctor, pediatric, surgery, dental | low | 0.95 |
| Manufacturing | 31–33 | manufacturing, fabrication, industries, industrial, products, systems, solutions, technologies | low | 0.95 |

### 10.2 Persona filter by industry

Written to T1 as `persona_filter_json`. Read by the Clay Find People column.

```json
{
  "auto_dealerships": {
    "titles": ["General Manager", "Finance Director", "HR Director", "HR Manager", "Controller"],
    "seniority": ["director", "manager", "vp"]
  },
  "hotels_motels": {
    "titles": ["General Manager", "HR Manager", "Director of Finance", "Controller", "Benefits Manager"],
    "seniority": ["director", "manager", "vp"]
  },
  "food_drinking": {
    "titles": ["Owner", "General Manager", "HR Manager", "Operations Manager"],
    "seniority": ["owner", "director", "manager"]
  },
  "entertainment_leisure": {
    "titles": ["General Manager", "HR Director", "CFO", "Controller"],
    "seniority": ["director", "manager", "vp", "c-suite"]
  },
  "fitness_centers": {
    "titles": ["Owner", "General Manager", "HR Manager", "Studio Manager"],
    "seniority": ["owner", "director", "manager"]
  },
  "physicians_offices": {
    "titles": ["Practice Manager", "Office Manager", "Administrator", "HR Manager"],
    "seniority": ["director", "manager"]
  },
  "manufacturing": {
    "titles": ["HR Director", "HR Manager", "Benefits Manager", "VP HR", "Controller"],
    "seniority": ["director", "manager", "vp"]
  }
}
```

---

## 11. Connections and Network Diagram

### 11.1 System components

| ID | Component | Type | Description |
|---|---|---|---|
| C1 | User | Human | Uploads lists, receives emails, approves review batches |
| C2 | T1 `upload_processing` | Clay table | Main pipeline table. Passthrough mode. |
| C3 | T2 `expansion_contacts` | Clay table | Contact expansion output table |
| C4 | T4 `enrichment_log` | Clay table | Append-only audit log |
| C5 | Agent (FastAPI) | Python service | Hosts all agent endpoints |
| C6 | DuckDB | File-based DB | Static data: DOL, NAICS, future sources |
| C7 | Salesforce | CRM | Target system for enriched records |
| C8 | Email service | SMTP / SendGrid | Sends review email and notifications |
| C9 | Clay Claygent | Clay AI agent | Legal entity resolution from website |

### 11.2 Connection map

```
C1 (User)
  │── uploads CSV ──────────────────────────────────► C2 (T1)
  │── receives review email ◄──────────────────────── C8 (Email)
  └── clicks approve/edit/reject ──────────────────► C5 (Agent /review/resume)

C2 (T1)
  │── webhook (new row) ──────────────────────────► C5 (Agent /classify)
  │── HTTP API (phase=find_company) ──────────────► C5 (Agent /static_lookup)
  │── HTTP API (phase=enrich, last col) ──────────► C5 (Agent /synthesise)
  │── Claygent column (run_legal_entity=true) ───► C9 (Claygent)
  │── fn_dedup_person (run_dedup=true) [internal]
  │── fn_dedup_company (run_dedup=true) [internal]
  │── SF write function (run_sf=true) ─────────────► C7 (Salesforce)
  │── write to other table (run_people_expansion) ► C3 (T2)
  └── HTTP API (done) ──────────────────────────────► C4 (T4 log)

C3 (T2)
  │── fn_dedup_person [internal]
  │── SF write contact function ────────────────────► C7 (Salesforce)
  └── HTTP API (done) ──────────────────────────────► C4 (T4 log)

C5 (Agent)
  │── queries DuckDB ───────────────────────────────► C6 (DuckDB / DOL, NAICS)
  │── sends review email ──────────────────────────► C8 (Email service)
  │── web search (legal entity, no website) [external]
  └── writes flags back to Clay rows ─────────────► C2 (T1) via Clay API

C6 (DuckDB)
  └── static data files: dol_form5500.parquet, naics_codes.parquet, ...
```

### 11.3 Data payloads per connection

| Connection | Direction | Payload |
|---|---|---|
| C1 → C2 | Upload | Raw CSV rows. `list_intent` column set by uploader. |
| C2 → C5 /classify | POST | Cleaned fields + routing booleans (Block 4 payload) |
| C5 → C2 | Response | Flag columns + static results (ClassifyResponse) |
| C2 → C5 /static_lookup | POST | Resolved company name + state + website + context |
| C5 → C2 | Response | DOL/NAICS results + phase update |
| C2 → C5 /synthesise | POST | Fully enriched row + all flag columns |
| C5 → C2 | Response | confidence_final + run_dedup + run_sf + phase |
| C5 → C8 | Send email | Batch review payload (JSON → HTML email) |
| C1 → C5 /review/resume | POST | batch_id + per-row decisions |
| C5 → C2 | Clay API write | run_sf, run_dedup, phase update per approved row |
| C2 → C7 | SF function | Upsert account + contact with batch_id stamp |
| C2 → C3 | Write to table | company fields + sf_account_id + found person fields |
| C2 → C4 | HTTP API POST | Log record with outcome fields |
| C3 → C7 | SF function | Create contact linked to AccountId |

---

## 12. Error Handling

### 12.1 Webhook retry idempotency

Clay retries failed webhook calls. Agent Call 1 must be idempotent:

```python
# At start of /classify handler
if request.static_done:
    # Already processed — return existing results without re-querying DuckDB
    return existing_response
```

### 12.2 DOL no-match

If DOL fuzzy match returns no result above 0.60:

- Set `dol_match_confidence = 0.0`, `dol_broker_name = null`
- Do not fail the row — confidence scoring will reflect the gap
- Row may route to human review if overall confidence drops below threshold

### 12.3 Clay response timeout

Agent must respond within 3 seconds. If DuckDB queries take longer:

- Run static tools with a 2.5-second timeout per tool
- If a tool times out, return `null` for its fields (non-blocking)
- Log the timeout to T4

### 12.4 "undefined" string in source data

Handled by Clay Block 1 formula columns before any data reaches the agent.
Agent should nonetheless validate: if any incoming string equals `"undefined"`, `"null"`,
`"none"`, or `"n/a"` (case-insensitive), treat as `None`.

### 12.5 FEIN normalisation

```python
def normalise_fein(raw: str) -> Optional[str]:
    digits = re.sub(r'\D', '', raw)
    if len(digits) != 9:
        return None
    return f"{digits[:2]}-{digits[2:]}"
```

### 12.6 Review batch expiry

Review batches expire after 7 days. After expiry, clicking the approve link returns a
"batch expired" error. Expired rows are logged with `review_decision = "expired"`.

---

## 13. Build Order

Build and test each phase end-to-end before starting the next.

### Phase 1 — Core pipeline, Archetype A only

1. Set up FastAPI project structure
2. Load DOL Form 5500 data into DuckDB
3. Implement `normalise_company_name()` and DOL preprocessing
4. Implement `find_in_dol()` tool
5. Implement Agent Call 1 for Archetype A (company + person) only
6. Run T1 Block 1–4 Sculptor prompts, build Clay columns
7. Configure Clay phase 1 enrichment columns (company enrich)
8. Test with Sample 1 (hospitality contact list) — verify DOL match, flags written correctly
9. Implement Agent Call 3 (synthesis) with simple confidence model
10. Test SF write via existing function

**Exit criteria:** A list of 10 hospitality rows processes end-to-end. DOL results appear in Clay. High-confidence rows write to Salesforce sandbox.

### Phase 2 — Full confidence model + human review

1. Implement full cross-source agreement scoring
2. Implement batch review collection in Agent Call 3
3. Implement `/api/v1/review/resume` endpoint
4. Set up email service integration
5. Build review email template
6. Test with mixed-confidence list — verify batch email fires, approval resumes SF write

**Exit criteria:** Low-confidence rows hold for review. Batch email sends. Approvals write to SF.

### Phase 3 — Archetype B (person-only rows)

1. Implement Archetype B classification in Agent Call 1
2. Add `find_company` phase logic
3. Run T1 Block 5 Sculptor prompt (Agent Call 2 trigger)
4. Implement Agent Call 2 (`/api/v1/static_lookup`)
5. Test with Sample 5 (RB2B intent list — row 4 has no company)

**Exit criteria:** Person-only rows resolve company, get DOL lookup run, complete to SF.

### Phase 4 — NAICS lookup + industry config

1. Load NAICS code table into DuckDB
2. Implement `lookup_naics()` tool
3. Implement industry detection from company name patterns
4. Wire industry config to persona filter and confidence ceiling
5. Test across all seven industries

### Phase 5 — Contact expansion (T2)

1. Configure Clay T2 table with Sculptor prompts
2. Implement `run_people_expansion` flag logic in Agent Call 1
3. Configure Find People Clay column in T1 with persona filter
4. Set up "write to other table" Clay action
5. Test with Sample 3 (franchise account list — company-only, wants contacts)
6. Verify DOL plan administrator free contact path

### Phase 6 — Legal entity resolution

1. Implement Claygent column in T1 for `run_legal_entity = true AND website IS NOT NULL`
2. Implement `build_sos_query()` for no-website cases in Agent Call 1
3. Wire agent web search for high DBA-risk rows without websites
4. Test with franchise lists (auto dealers, hotels)

### Phase 7 — Additional static sources

1. Define interface for new sources (StaticToolResult envelope)
2. Load next dataset into DuckDB
3. Implement tool function
4. Add to parallel call list in Agent Call 1 and 2
5. Add columns to T1

---

*End of specification*
