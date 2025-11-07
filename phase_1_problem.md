# Phase 1 — Problem Statement & Data Sources (Local, 100% Free)

## Problem Statement
Build a **local, cloud‑free** streaming pipeline that simulates real‑time **healthcare claims events** using publicly available **synthetic Medicare claims** data. The pipeline must:

- Ingest (simulate) claims events from static CSV/Parquet into a timed event stream.
- Persist raw + curated data to local Parquet files and query via **DuckDB**.
- Compute **rolling aggregates** (e.g., claims count, average allowed amount) and **simple anomaly flags** (e.g., z‑score spikes per provider/region/service line) in near real‑time.
- Produce lightweight artifacts (CSV/Parquet + a tiny HTML chart or SQL output) that later phases can consume.

This phase is **engineering‑first** (ingestion → transform → query) and lays the foundation for later modeling (Phase 2), market joins (Phase 3), Power BI storytelling (Phase 4), and CI/CD (Phase 5).

## Constraints (Hard Boundaries)
- **100% free & open‑source**; no trials; no vendor lock‑in.
- **Local only**: runs via `python` (or `docker compose`) without cloud credentials.
- **Deterministic & reproducible**: seedable event generator; `.env.example` only for optional keys (not required in Phase 1).
- **Schema‑first**: clear column dictionary for claims events, providers, and regions to keep Phase 2 seamless.

## Success Criteria (Phase 1)
- `make up` (or a single `python` command) starts the event simulator and writes Parquet.
- DuckDB query returns the **last N events** and **daily aggregates** by provider/region.
- A minimal anomaly flag (e.g., |z| ≥ 3) is computed and stored for each window.
- A small HTML/CSV artifact is saved under `artifacts/` for screenshots.

---

## Primary Data Sources (Free Forever)

### 1) CMS **DE‑SynPUF** — Synthetic Medicare Claims (Public)
- Overview: synthetic, privacy‑preserving Medicare claims; ideal for engineering demos and prototyping.
- Start here (overview/downloads): https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-claims-synthetic-public-use-files
- 2008–2010 DE‑SynPUF main page: https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-claims-synthetic-public-use-files/cms-2008-2010-data-entrepreneurs-synthetic-public-use-file-de-synpuf
- Sample download index (explains file breakdown): https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-claims-synthetic-public-use-files/cms-2008-2010-data-entrepreneurs-synthetic-public-use-file-de-synpuf/de10-sample-1

### 2) **FRED** — Macroeconomic Time Series (for later phases; free API key)
- API docs: https://fred.stlouisfed.org/docs/api/fred/
- API key registration (free): https://fred.stlouisfed.org/docs/api/api_key.html  
*(Phase 1 does not require the key; include as optional in `.env.example` for future use.)*

### 3) **SEC EDGAR** — Company Filings & XBRL (no key required)
- EDGAR APIs index: https://www.sec.gov/search-filings/edgar-application-programming-interfaces
- Developer resources: https://www.sec.gov/about/developer-resources  
*(Useful in later phases for disclosure density or risk overlays.)*

### 4) **Stooq** — Free Historical Market Data (CSV)
- Database index: https://stooq.com/db/
- Example S&P 500 history page (CSV download UI): https://stooq.com/q/d/?s=%5Espx  
*(Used later to simulate market overlays without paid data.)*

### 5) (Optional) **HL7 FHIR** References — Healthcare Schema Alignment
- FHIR overview: https://www.hl7.org/fhir/overview.html
- US Public Health library (profiles/examples): https://build.fhir.org/ig/HL7/fhir-us-ph-library/en/StructureDefinition-us-ph-specification-bundle-testing.html  
*(For naming conventions or future interoperability—no API usage required.)*

---

## Minimal Schema Draft (to finalize before coding)
**claims_event** (streamed)
- `event_ts` (datetime), `claim_id` (str), `bene_id` (str), `provider_id` (str), `place_of_service` (str), `hcpcs_code` (str), `drg_cd` (str, nullable),
- `claim_type` (enum: inpatient/outpatient/carrier/pde), `allowed_amt` (float), `paid_amt` (float), `state` (str, 2‑letter), `county_fips` (str, nullable)

**provider_dim** (lookup)
- `provider_id`, `npi` (nullable), `org_name` (nullable), `state`, `county_fips`

**region_dim** (lookup)
- `state`, `county_fips`, `region_name`

**facts_daily** (derived)
- `date`, `provider_id`, `state`, `claims_cnt`, `avg_allowed_amt`, `zscore_allowed_amt`

---

## Next Step (Phase 1 Build Plan)
1) Download a small DE‑SynPUF sample; define `claims_event` mapping.
2) Write `ingestion/generator.py` that emits timed events from CSV to Parquet append.
3) Create DuckDB views for **last N** events + daily aggregates.
4) Compute z‑score anomaly flags and save `facts_daily` as Parquet.
5) Export a tiny CSV + HTML chart under `artifacts/` for the first LinkedIn post.

