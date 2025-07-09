import requests
import time

STATION_ID = "46088"
URL = f"https://www.ndbc.noaa.gov/data/realtime2/{STATION_ID}.swdir2"

session = requests.Session()
last_mod = None

while True:
    headers = {}
    if last_mod:
        headers["If-Modified-Since"] = last_mod

    resp = session.get(URL, headers=headers, timeout=30)
    if resp.status_code == 304:
        print("No change since", last_mod)
    elif resp.status_code == 200:
        last_mod = resp.headers.get("Last-Modified")
        lines = resp.text.splitlines()
        print(f"Fetched {len(lines)} lines at {time.ctime()}")
        for line in lines[len(lines) - 10 :]:
            print(line)
    else:
        resp.raise_for_status()

    time.sleep(30 * 60)
