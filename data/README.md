# Data Directory

Place your DOL Form 5500 CSV files here before running `./load.sh` or `python load_data.py`.

## Companies list (existing accounts)

- **`companies.csv`** — Your existing accounts for Include/Exclude filtering. Columns: `CompanyName`, `CompanyCode`, `EIN`. Add rows manually or replace with your export. Duplicate Company Codes are skipped when loading.
- You can add more companies in the app (Existing Accounts page); duplicates by Company Code are not added.

## Expected DOL files (per year)

- `f_5500_YYYY_latest.csv` — Main Form 5500 filings
- `F_SCH_A_YYYY_latest.csv` — Schedule A (Insurance)
- `F_SCH_A_PART1_YYYY_latest.csv` — Schedule A Part 1 (Brokers)
- `F_SCH_C_YYYY_latest.csv` — Schedule C (Service Provider header)
- `F_SCH_C_PART1_ITEM2_YYYY_latest.csv` — Schedule C Part 1 Item 2 (Service Providers)
- `form_5500_business_codes_complete.csv` — NAICS business code mapping

The loader auto-detects the table type and year from filenames. File names just need to contain the table identifier and a 4-digit year.

## Generated database (star schema)

After running `./load.sh`, the loader creates `dol_data.db` with:

**Dimension tables**
- `dim_companies` — One row per employer (EIN) with latest filing year data, business code prefixes
- `dim_brokers` — Unique brokers with name, city, state, zip
- `dim_providers` — Unique service providers with name, EIN, city, state
- `dim_business_codes` — NAICS code to sector/industry mapping

**Fact tables**
- `fact_broker_company` — Broker–company relationships (broker_id, company_ein, filing_year, commissions)
- `fact_provider_company` — Provider–company relationships (provider_id, company_ein, filing_year, compensation)

**Other**
- `companies` — Your existing accounts (from `companies.csv`)
