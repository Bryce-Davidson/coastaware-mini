import requests

STATION_ID = "46088"
url = f"https://www.ndbc.noaa.gov/data/realtime2/{STATION_ID}.swdir2"

resp = requests.get(url, timeout=30)
print(resp.text.split("\n"))
