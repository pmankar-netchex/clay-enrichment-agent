# GTM Enrichment Agent

FastAPI service that acts as a pre-intelligence layer for Clay GTM list enrichment: classification, static lookups, synthesis, and human-in-the-loop review.

## Prerequisites

- **Python** 3.11+ (3.13 is used in development)
- Optional: DOL Form 5500 and NAICS CSV/Parquet files for production-quality lookups (the app starts without them using empty placeholder tables)

## Installation

1. **Clone or copy** this repository and enter the project directory.

2. **Create and activate a virtual environment** (recommended):

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate   # Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**:

   ```bash
   pip install -U pip
   pip install -r requirements.txt
   ```

4. **Install dev dependencies** (needed only for running tests):

   ```bash
   pip install -r requirements-dev.txt
   ```

5. **Environment variables** — copy the example file and edit values:

   ```bash
   cp .env.example .env
   ```

   At minimum, adjust paths if your data lives elsewhere (`DUCKDB_PATH`, `DOL_DATA_PATH`, `NAICS_DATA_PATH`). For full Clay/Mailjet/review flows, set the API keys and URLs described in `.env.example`.

6. **Optional: load reference data** into DuckDB (or rely on first-start loading):

   - Put DOL data at the path in `DOL_DATA_PATH` (Parquet or CSV), or run:

     ```bash
     python scripts/load_dol_data.py --source /path/to/dol_form5500.parquet
     ```

   - Put NAICS CSV at `NAICS_DATA_PATH`, or run:

     ```bash
     python scripts/load_naics_data.py --source /path/to/naics_codes.csv
     ```

   On startup, if those files exist, the app will materialise tables from them. See `data/README.md` for legacy CSV layout notes.

## Run the API

From the project root, with the virtual environment activated:

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

- **Health check**: [http://127.0.0.1:8000/health](http://127.0.0.1:8000/health)
- **OpenAPI docs**: [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)
- **API prefix**: routes are mounted under `/api/v1` (e.g. `/api/v1/...`).

For production, drop `--reload` and configure process management (systemd, Docker, etc.) as you prefer.

## Deploy on Replit

The repo includes [`.replit`](.replit) and [`replit.nix`](replit.nix) so you can import from GitHub, use **Run** for local preview (port **8000**), then **Publish → Autoscale** for a public `https://…replit.app` URL.

1. Import the repository into a new Replit App (or upload the project).
2. Open **Secrets** (lock icon) and add variables from `.env.example` (`CLAY_API_KEY`, `DUCKDB_PATH`, etc.). Replit does not use a committed `.env` in production.
3. Optional: upload DOL / NAICS files under `data/` and set `DOL_DATA_PATH` / `NAICS_DATA_PATH` if paths differ.
4. **Publish** and choose **Autoscale**. Set **Clay / review URLs** (e.g. `AGENT_BASE_URL`, `REVIEW_UI_BASE_URL`) to your deployed `https://<your-repl>.replit.app` (or custom domain).

**Persistence:** Published Autoscale instances should not rely on the container filesystem for durable data. The default DuckDB file under `data/` is fine for demos; for production enrichment data, use a [Replit database / object storage](https://docs.replit.com/category/storage-and-databases) or an external database, or a **Reserved VM** deployment if you accept single-machine file semantics.

## Testing

Tests use **pytest** and **pytest-asyncio**. Install dev requirements first (`requirements-dev.txt`).

Run the full suite from the project root:

```bash
pytest tests/ -v
```

Useful variants:

```bash
pytest tests/ -q                 # quiet summary
pytest tests/test_synthesise.py  # single file
pytest tests/ -k "legal_entity"  # name filter
```

Tests mock external services and use in-memory DuckDB where needed, so they do not require a populated `static_data.duckdb` or live Clay/Mailjet credentials.

## Project layout (short)

| Path | Role |
|------|------|
| `app/` | FastAPI app, routers, services, tools |
| `tests/` | Pytest suite |
| `scripts/` | DOL / NAICS loaders |
| `data/` | DuckDB file and optional source data (gitignored as appropriate) |

For product behaviour and API contracts, see `GTM_Enrichment_Agent_Spec.md`.
