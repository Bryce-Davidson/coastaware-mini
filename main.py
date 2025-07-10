import requests
import time
import polars as pl
import pandas as pd
from io import StringIO


HEADER_LINE = 0
HEADER_TYPE_LINE = 1

STATION_ID = "46088"
URL = f"https://www.ndbc.noaa.gov/data/realtime2/{STATION_ID}.txt"

session = requests.Session()
last_mod = None


while True:
    headers = {}
    if last_mod:
        headers["If-Modified-Since"] = last_mod

    resp = session.get(URL, headers=headers, timeout=30)

    for header_name, header_value in resp.headers.items():
        print(f"  {header_name}: {header_value}")

    with open("resp.txt", "w") as f:
        f.write(resp.text)

    if resp.status_code == 200:
        last_mod = resp.headers.get("Last-Modified")

        pdf = pd.read_fwf(StringIO(resp.text), comment="#")
        df = pl.from_pandas(pdf)

        print(df.dtypes)
        print(f"Shape: {df.shape}")
        print(df.head())

    else:
        resp.raise_for_status()

    time.sleep(30 * 60)
