.PHONY: venv install up aggregate query clean


venv:
python -m venv .venv && . .venv/bin/activate || . .venv/Scripts/activate


install:
. .venv/bin/activate || . .venv/Scripts/activate; \
pip install -r requirements.txt


up:
. .venv/bin/activate || . .venv/Scripts/activate; \
python ingestion/generator.py


aggregate:
. .venv/bin/activate || . .venv/Scripts/activate; \
python pipeline/aggregate.py


query:
. .venv/bin/activate || . .venv/Scripts/activate; \
python scripts/query_last_events.py --limit 10


clean:
rm -rf .venv data/curated/* data/warehouse.duckdb artifacts/*