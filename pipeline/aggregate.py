import os
import duckdb
import pandas as pd
from dotenv import load_dotenv
from scipy.stats import zscore
from rich import print

load_dotenv()
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "./data/warehouse.duckdb")
PARQUET_FACTS = os.getenv("PARQUET_FACTS_DAILY_PATH", "./data/curated/facts_daily.parquet")
ARTIFACTS_DIR = os.getenv("ARTIFACTS_DIR", "./artifacts")

os.makedirs(os.path.dirname(PARQUET_FACTS), exist_ok=True)
os.makedirs(ARTIFACTS_DIR, exist_ok=True)

con = duckdb.connect(DUCKDB_PATH)

# Build daily aggregates
q = """
WITH base AS (
  SELECT DATE_TRUNC('day', event_ts) AS date,
         provider_id,
         state,
         COUNT(*) AS claims_cnt,
         AVG(allowed_amt) AS avg_allowed_amt
  FROM claims_events
  GROUP BY 1,2,3
)
SELECT * FROM base ORDER BY date, provider_id;
"""

df = con.execute(q).df()

if len(df) == 0:
    print("[yellow]No events yet; run ingestion first.[/yellow]")
    raise SystemExit(0)

# z-score by provider across time
if "avg_allowed_amt" in df.columns and len(df) > 3:
    df["zscore_allowed_amt"] = df.groupby("provider_id")["avg_allowed_amt"].transform(
        lambda s: zscore(s, nan_policy='omit')
    )
else:
    df["zscore_allowed_amt"] = 0.0

# Save
df.to_parquet(PARQUET_FACTS, engine="pyarrow", compression="snappy")

# Export lightweight artifacts
csv_path = os.path.join(ARTIFACTS_DIR, "facts_daily.csv")
df.to_csv(csv_path, index=False)

# Minimal HTML chart with Plotly
import plotly.express as px
fig = px.line(df.sort_values("date"), x="date", y="avg_allowed_amt",
              color="provider_id", title="Avg Allowed Amount by Provider (Daily)")
html_path = os.path.join(ARTIFACTS_DIR, "daily_trend.html")
fig.write_html(html_path, include_plotlyjs='cdn')

print(f"[green]Wrote[/green] {PARQUET_FACTS}\n[green]Artifacts:[/green] {csv_path} | {html_path}")
