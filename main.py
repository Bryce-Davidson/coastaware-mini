import requests
import time
import datetime
import csv
import os

HEADER_LINE = 0
HEADER_TYPE_LINE = 1
DATE_RANGE = slice(0, 5)
DATA_RANGE = slice(6, 19)

STATION_ID = "46088"
URL = f"https://www.ndbc.noaa.gov/data/realtime2/{STATION_ID}.txt"

session = requests.Session()

last_modified = None
latest_ts = None
header_written = False

while True:
    headers = {"If-Modified-Since": last_modified} if last_modified else {}
    resp = session.get(URL, headers=headers, timeout=30, stream=True)

    if resp.status_code == 200:
        lines = []
        header_lines = []

        for line in resp.iter_lines(decode_unicode=True):
            if not line:
                continue

            # Capture header lines (they start with #)
            if line.startswith("#"):
                header_lines.append(line[1:].strip())  # Remove # and strip whitespace
                continue

            parts = line.split()
            try:
                cur_line_ts = datetime.datetime(*map(int, parts[DATE_RANGE]))
            except (ValueError, IndexError):
                continue

            if latest_ts and cur_line_ts <= latest_ts:
                break

            lines.append(line)

        if lines:
            parts = lines[0].split()
            latest_ts = datetime.datetime(*map(int, parts[DATE_RANGE]))

            csv_file = f"./data/stations/{STATION_ID}.csv"

            # Check if we need to write header
            write_header = not header_written and (
                not os.path.exists(csv_file) or os.path.getsize(csv_file) == 0
            )

            with open(csv_file, "a", newline="") as f:
                writer = csv.writer(f)

                # Write header if needed
                if write_header and header_lines:
                    # Write the column names from the first header line
                    if header_lines:
                        writer.writerow(header_lines[0].split())
                        header_written = True

                for line in lines:
                    data = line.split()[DATA_RANGE]
                    writer.writerow(data)
                    print(data)

        resp.close()
        last_modified = resp.headers.get("Last-Modified")

    elif resp.status_code == 304:
        print("Already up to date.")

    else:
        print(f"Unexpected status {resp.status_code}")
        break

    time.sleep(30)
