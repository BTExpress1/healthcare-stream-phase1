python -m venv .venv
source .venv/bin/activate # or .venv\Scripts\activate on Windows
pip install -r requirements.txt
cp .env.example .env
mkdir -p data/raw/samples # optional: place a DE‑SynPUF CSV sample here
python ingestion/generator.py # in one terminal (Ctrl+C to stop)
# in another terminal
python pipeline/aggregate.py
python scripts/query_last_events.py --limit 10