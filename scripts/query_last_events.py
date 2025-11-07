import os
import duckdb
import typer


app = typer.Typer()


@app.command()
def main(limit: int = 10):
db = os.getenv("DUCKDB_PATH", "./data/warehouse.duckdb")
con = duckdb.connect(db)
q = """
SELECT * FROM claims_events
ORDER BY event_ts DESC
LIMIT ?
"""
res = con.execute(q, [limit]).fetchdf()
if res.empty:
print("No events yet. Start the generator first.")
else:
print(res)


if __name__ == "__main__":
app()