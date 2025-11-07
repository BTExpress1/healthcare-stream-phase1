-- DuckDB schema for Phase 1
CREATE TABLE IF NOT EXISTS claims_events (
event_ts TIMESTAMP,
claim_id VARCHAR,
bene_id VARCHAR,
provider_id VARCHAR,
place_of_service VARCHAR,
hcpcs_code VARCHAR,
drg_cd VARCHAR,
claim_type VARCHAR,
allowed_amt DOUBLE,
paid_amt DOUBLE,
state VARCHAR,
county_fips VARCHAR
);


CREATE OR REPLACE VIEW v_last_events AS
SELECT *
FROM claims_events
ORDER BY event_ts DESC
LIMIT 1000;


CREATE TABLE IF NOT EXISTS facts_daily AS
SELECT
DATE_TRUNC('day', event_ts) AS date,
provider_id,
state,
COUNT(*) AS claims_cnt,
AVG(allowed_amt) AS avg_allowed_amt,
0.0 AS zscore_allowed_amt
FROM claims_events
WHERE 1=0;