import os
"hcpcs_code": np.random.choice(["99213","99214","93000","80050","J1885"], size=n),
"drg_cd": np.random.choice([None, "470", "291", "377"], size=n),
"claim_type": np.random.choice(["carrier","outpatient","inpatient"], size=n, p=[0.6,0.3,0.1]),
"allowed_amt": np.round(np.random.lognormal(mean=4.2, sigma=0.6, size=n), 2),
"paid_amt": lambda x: x, # filled below
"state": np.random.choice(states, size=n),
"county_fips": np.random.choice(["033","035","061","073","077","017"], size=n)
})
# small noise between allowed and paid
df["paid_amt"] = (df["allowed_amt"] * np.random.uniform(0.7, 0.98, size=n)).round(2)
return df[COLUMNS]




def load_sample_rows(n: int, base_ts: datetime) -> pd.DataFrame:
# Minimal mapper from a DE‑SynPUF-like CSV to our columns; fallback to synth if mapping fails
try:
raw = pd.read_csv(SAMPLE_FILE)
raw = raw.sample(min(n, len(raw)), random_state=SEED).reset_index(drop=True)
df = pd.DataFrame({
"event_ts": [base_ts + timedelta(seconds=i) for i in range(len(raw))],
"claim_id": raw.get("clm_id", pd.Series([str(uuid.uuid4())]*len(raw))),
"bene_id": raw.get("bene_id", pd.Series(np.random.randint(10_000,99_999,len(raw)).astype(str))),
"provider_id": raw.get("prvdr_num", pd.Series(np.random.randint(100,999,len(raw)).astype(str))),
"place_of_service": raw.get("line_place_of_srvc_cd", pd.Series(np.random.choice(places, len(raw)))),
"hcpcs_code": raw.get("hcpcs_cd", pd.Series(np.random.choice(["99213","99214"], len(raw)))),
"drg_cd": raw.get("drg_cd", pd.Series([None]*len(raw))),
"claim_type": pd.Series(["carrier"]*len(raw)),
"allowed_amt": raw.get("line_alowd_chrg_amt", pd.Series(np.round(np.random.lognormal(4.2,0.6,len(raw)),2))),
"paid_amt": raw.get("line_nch_pmt_amt", pd.Series(0.0, index=range(len(raw)))),
"state": raw.get("prvdr_state_cd", pd.Series(np.random.choice(states, len(raw)))),
"county_fips": pd.Series(np.random.choice(["033","035","061","073","077","017"], len(raw)))
})
df["paid_amt"] = df["paid_amt"].mask(df["paid_amt"].isna(), df["allowed_amt"]*np.random.uniform(0.7,0.98,len(df))).round(2)
return df[COLUMNS]
except Exception as e:
print(f"[yellow]Sample load failed ({e}); using synthetic rows[/yellow]")
return synthesize_rows(n, base_ts)




def main():
con = duckdb.connect(DUCKDB_PATH)
with open("warehouse/ddl.sql", "r") as f:
con.execute(f.read())


print(f"[bold green]Streaming...[/bold green] writing Parquet → {PARQUET_EVENTS}\nDB → {DUCKDB_PATH}")


base_ts = datetime.utcnow()
buffer = []
total = 0
while True:
# Select source: file or synthetic
batch = load_sample_rows(STREAM_RATE, base_ts) if SAMPLE_FILE else synthesize_rows(STREAM_RATE, base_ts)
base_ts += timedelta(seconds=STREAM_RATE)
buffer.append(batch)


if sum(len(b) for b in buffer) >= CHUNK_ROWS:
df = pd.concat(buffer, ignore_index=True)
# append to Parquet (via pyarrow dataset write)
df.to_parquet(PARQUET_EVENTS, engine="pyarrow", compression="snappy", append=os.path.exists(PARQUET_EVENTS))
# also append to DuckDB
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