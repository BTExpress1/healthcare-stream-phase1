## Quick Start (no cloud, 100% free)

### 1) Clone & enter

git clone https://github.com/BTExpress1/healthcare-stream-phase1.git
cd  healthcare-stream-phase1

### 2) Create a virtual env & install
#### Windows (PowerShell)

python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

#### macOS/Linux

python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

### 3) Configure .env

cp .env.example .env

Open .env and confirm:

SAMPLE_FILE=./data/raw/samples/de_synpuf_carrier_claims_sample_small.csv
CHUNK_ROWS=60          # lower = faster first flush
DS_START_DATE=2008-01-01

### 4) Add sample data

Place the CSV here (small file recommended):

data/raw/samples/de_synpuf_carrier_claims_sample_small.csv

### 5) Run the streaming generator (Terminal A)

python ingestion/generator.py

You should see lines like:
mode=CSV, rows=20
flushed 60 rows — total 60

### 6) Aggregate & visualize (Terminal B)
python pipeline/aggregate.py

Outputs:

artifacts/facts_daily.csv

artifacts/daily_trend.html ← open in a browser

### 7) Peek recent events (optional)

python scripts/query_last_events.py --limit 10

### Stop / Reset

Stop generator: Ctrl+C in Terminal A

Reset data for a clean rerun:

rm -f data/warehouse.duckdb
rm -rf data/curated/claims_events_shards

### Troubleshooting (fast)

If No events yet, wait for the first flushed ... line or lower CHUNK_ROWS.

If packages missing, rerun: pip install -r requirements.txt.

If chart shows one vertical line, ensure .env exists and SAMPLE_FILE points to the CSV.

### Repo:
https://github.com/BTExpress1/healthcare-stream-phase1/





