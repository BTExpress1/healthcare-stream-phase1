-- events table
CREATE TABLE IF NOT EXISTS claims_events (
    event_ts         TIMESTAMP,
    claim_id         VARCHAR,
    bene_id          VARCHAR,
    provider_id      VARCHAR,
    place_of_service VARCHAR,
    hcpcs_code       VARCHAR,
    drg_cd           VARCHAR,
    claim_type       VARCHAR,
    allowed_amt      DOUBLE,
    paid_amt         DOUBLE,
    state            VARCHAR,
    county_fips      VARCHAR
);

-- convenience view
CREATE OR REPLACE VIEW v_last_events AS
SELECT *
FROM claims_events
ORDER BY event_ts DESC
LIMIT 1000;

-- empty daily facts table (no CTAS)
CREATE TABLE IF NOT EXISTS facts_daily (
    date               DATE,
    provider_id        VARCHAR,
    state              VARCHAR,
    claims_cnt         BIGINT,
    avg_allowed_amt    DOUBLE,
    zscore_allowed_amt DOUBLE
);
