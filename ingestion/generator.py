import os, time, uuid, random
from datetime import datetime, timedelta

import duckdb
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from rich import print

# --- env & constants ---
load_dotenv(dotenv_path=".env")

DS_START_DATE = os.getenv("DS_START_DATE", "2008-01-01")
JITTER_DAYS = int(os.getenv("JITTER_DAYS", "90"))  # ±days applied to event_ts (0 to disable)

DATA_DIR = os.getenv("DATA_DIR", "./data")
ARTIFACTS_DIR = os.getenv("ARTIFACTS_DIR", "./artifacts")
STREAM_RATE = int(os.getenv("STREAM_RATE_PER_SEC", "20"))
CHUNK_ROWS = int(os.getenv("CHUNK_ROWS", "500"))
DUCKDB_PATH = os.getenv("DUCKDB_PATH", os.path.join(DATA_DIR, "warehouse.duckdb"))
PARQUET_EVENTS = os.getenv("PARQUET_EVENTS_PATH", os.path.join(DATA_DIR, "curated/claims_events.parquet"))
SAMPLE_FILE = os.getenv("SAMPLE_FILE", "").strip() or None
SEED = int(os.getenv("SEED", "42"))

os.makedirs(os.path.join(DATA_DIR, "curated"), exist_ok=True)
os.makedirs(ARTIFACTS_DIR, exist_ok=True)

random.seed(SEED); np.random.seed(SEED)

COLUMNS = [
    "event_ts","claim_id","bene_id","provider_id","place_of_service",
    "hcpcs_code","drg_cd","claim_type","allowed_amt","paid_amt","state","county_fips"
]
states = ["WA","OR","CA","ID","MT","UT","NV","AZ"]
places = ["11","22","23","24"]  # office, outpatient, ER, ambulatory


# --- helpers ---
def _parse_claim_date(series: pd.Series) -> pd.Series:
    s = series
    # Handle numeric yyyymmdd that may come as scientific notation in CSVs
    if not s.dtype == "object":
        s = s.astype(str)
    s = s.str.replace(r"\.0$", "", regex=True).str.replace(" ", "", regex=False)
    # normalize to 8 chars if looks like yyyymmdd
    s8 = s.str.len() == 8
    parsed = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns, UTC]")
    parsed[s8] = pd.to_datetime(s[s8], format="%Y%m%d", errors="coerce", utc=True)
    # fallback free-parse for others
    parsed = parsed.fillna(pd.to_datetime(s, errors="coerce", utc=True))
    return parsed


def synthesize_rows(n: int, base_ts: datetime) -> pd.DataFrame:
    ts = [base_ts + timedelta(seconds=i) for i in range(n)]
    df = pd.DataFrame({
        "event_ts": ts,
        "claim_id": [str(uuid.uuid4()) for _ in range(n)],
        "bene_id": np.random.randint(10_000, 99_999, size=n).astype(str),
        "provider_id": np.random.randint(100, 999, size=n).astype(str),
        "place_of_service": np.random.choice(places, size=n),
        "hcpcs_code": np.random.choice(["99213","99214","93000","80050","J1885"], size=n),
        "drg_cd": np.random.choice([None, "470", "291", "377"], size=n),
        "claim_type": np.random.choice(["carrier","outpatient","inpatient"], size=n, p=[0.6,0.3,0.1]),
        "allowed_amt": np.round(np.random.lognormal(mean=4.2, sigma=0.6, size=n), 2),
        "paid_amt": 0.0,
        "state": np.random.choice(states, size=n),
        "county_fips": np.random.choice(["033","035","061","073","077","017"], size=n),
    })[COLUMNS]
    df["paid_amt"] = (df["allowed_amt"] * np.random.uniform(0.7, 0.98, size=n)).round(2)
    return df


def load_sample_rows(n: int, base_ts: datetime) -> pd.DataFrame:
    try:
        raw = pd.read_csv(SAMPLE_FILE, dtype=str, low_memory=False)
        raw.columns = [c.lower() for c in raw.columns]
        raw = raw.sample(min(n, len(raw)), random_state=None).reset_index(drop=True)

        # prefer real claim dates
        event_ts = None
        from_dt = _parse_claim_date(raw["clm_from_dt"]) if "clm_from_dt" in raw.columns else None
        thru_dt  = _parse_claim_date(raw["clm_thru_dt"]) if "clm_thru_dt" in raw.columns else None
        if from_dt is not None and from_dt.notna().any():
            if thru_dt is not None and thru_dt.notna().any():
                # midpoint of from/thru
                span = (thru_dt - from_dt)
                event_ts = (from_dt + (span / 2)).fillna(from_dt)
            else:
                event_ts = from_dt
        else:
            # fallback: synthetic daily series from DS_START_DATE
            start = pd.Timestamp(DS_START_DATE, tz="UTC")
            event_ts = pd.Series([start + timedelta(days=i) for i in range(len(raw))])

        # optional jitter ±JITTER_DAYS to widen timeline
        if JITTER_DAYS > 0:
            event_ts = event_ts + pd.to_timedelta(
                np.random.randint(-JITTER_DAYS, JITTER_DAYS + 1, size=len(event_ts)),
                unit="D"
            )

        # provider id (prefer facility/provider number, else NPI, else random)
        if "prvdr_num" in raw.columns:
            provider_series = raw["prvdr_num"].astype(str)
        elif "prf_physn_npi_1" in raw.columns:
            provider_series = raw["prf_physn_npi_1"].astype(str)
        else:
            provider_series = pd.Series(np.random.randint(100, 999, len(raw)).astype(str), index=raw.index)

        df = pd.DataFrame({
            "event_ts": event_ts,
            "claim_id": raw.get("clm_id", pd.Series([str(uuid.uuid4())]*len(raw), index=raw.index)),
            "bene_id": raw.get("desynpuf_id", pd.Series(np.random.randint(10_000,99_999,len(raw)).astype(str), index=raw.index)),
            "provider_id": provider_series,
            "place_of_service": raw.get("line_place_of_srvc_cd", pd.Series(np.random.choice(places, len(raw)), index=raw.index)),
            "hcpcs_code": raw.get("hcpcs_cd", pd.Series(np.random.choice(["99213","99214"], len(raw)), index=raw.index)),
            "drg_cd": raw.get("drg_cd", pd.Series([None]*len(raw), index=raw.index)),
            "claim_type": pd.Series(["carrier"]*len(raw), index=raw.index),
            "allowed_amt": pd.to_numeric(raw.get("line_alowd_chrg_amt",
                                pd.Series(np.round(np.random.lognormal(4.2,0.6,len(raw)),2), index=raw.index)),
                                errors="coerce"),
            "paid_amt": pd.to_numeric(raw.get("line_nch_pmt_amt",
                                pd.Series(np.nan, index=raw.index)),
                                errors="coerce"),
            "state": raw.get("prvdr_state_cd", pd.Series(np.random.choice(states, len(raw)), index=raw.index)),
            "county_fips": pd.Series(np.random.choice(["033","035","061","073","077","017"], len(raw)), index=raw.index),
        })[COLUMNS]

        # fill missing paid_amt as a fraction of allowed
        mask = df["paid_amt"].isna()
        if mask.any():
            df.loc[mask, "paid_amt"] = (df.loc[mask, "allowed_amt"].astype(float)
                                        * np.random.uniform(0.7, 0.98, mask.sum())).round(2)
        return df

    except Exception as e:
        print(f"[yellow]Sample load failed ({e}); using synthetic rows[/yellow]")
        return synthesize_rows(n, base_ts)


# --- main loop ---
def main():
    con = duckdb.connect(DUCKDB_PATH)
    with open("warehouse/ddl.sql", "r") as f:
        con.execute(f.read())

    print(f"[bold green]Streaming...[/bold green] writing Parquet → {PARQUET_EVENTS}\nDB → {DUCKDB_PATH}")

    base_ts = pd.Timestamp(DS_START_DATE, tz="UTC").to_pydatetime()
    buffer = []
    total = 0
    while True:
        batch = load_sample_rows(STREAM_RATE, base_ts) if SAMPLE_FILE else synthesize_rows(STREAM_RATE, base_ts)
        if batch is None or not isinstance(batch, pd.DataFrame) or batch.empty:
            batch = synthesize_rows(STREAM_RATE, base_ts)

        base_ts += timedelta(seconds=STREAM_RATE)
        buffer.append(batch)

        # log mode
        print(f"[blue]mode={'CSV' if SAMPLE_FILE else 'SYN'}, rows={len(batch)}[/blue]")

        if sum(len(b) for b in buffer) >= CHUNK_ROWS:
            df = pd.concat(buffer, ignore_index=True)

            # write shard (pyarrow has no append)
            events_dir = os.path.splitext(PARQUET_EVENTS)[0] + "_shards"
            os.makedirs(events_dir, exist_ok=True)
            shard_path = os.path.join(events_dir, f"part-{int(time.time()*1000)}.parquet")
            df.to_parquet(shard_path, engine="pyarrow", compression="snappy", index=False)

            # also into DuckDB table
            con.execute("INSERT INTO claims_events SELECT * FROM df")
            total += len(df)
            print(f"[cyan]flushed {len(df)} rows — total {total}[/cyan]")
            buffer = []

        time.sleep(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[red]Stopped.[/red]")
