import pandas as pd, numpy as np
df = pd.read_csv(r"./data/raw/samples/de_synpuf_carrier_claims_sample_small.csv", dtype=str, low_memory=False)
df.columns = [c.lower() for c in df.columns]
from_dt = pd.to_datetime(df["clm_from_dt"], format="%Y%m%d", errors="coerce", utc=True)
thru_dt = pd.to_datetime(df.get("clm_thru_dt"), format="%Y%m%d", errors="coerce", utc=True)
evt = (from_dt + (thru_dt - from_dt)/2).fillna(from_dt)
evt = evt + pd.to_timedelta(np.random.randint(-90,90,len(evt)), unit="D")
print("distinct days:", evt.dt.date.nunique(), "| min:", evt.min(), "| max:", evt.max())
PY
