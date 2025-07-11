import json
import requests
import datetime
import os
import boto3
import psycopg2
import time
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
rds = boto3.client("rds")

# ------------------------------------------------------------

STATION_ID = os.environ.get("STATION_ID", "46088")
TABLE_NAME = os.environ.get("DYNAMODB_TABLE")
DB_HOST = os.environ.get("DB_HOST")
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")

if not all([TABLE_NAME, DB_HOST, DB_NAME, DB_USER, DB_PASSWORD]):
    raise ValueError("Missing required environment variables")

URL = f"https://www.ndbc.noaa.gov/data/realtime2/{STATION_ID}.txt"

RDS_connection_pool = None

# ------------------------------------------------------------

# NOAA Standard Meteorological Data Format
# Reference: https://www.ndbc.noaa.gov/faq/measdes.shtml
# Format: #YY MM DD hh mm WDIR WSPD GST WVHT DPD APD MWD PRES ATMP WTMP DEWP VIS PTDY TIDE
NOAA_COLUMNS = [
    ("WDIR", "wind_direction_deg"),  # Wind direction (degrees from true N)
    ("WSPD", "wind_speed_ms"),  # Wind speed (m/s)
    ("GST", "gust_speed_ms"),  # Peak gust speed (m/s)
    ("WVHT", "wave_height_m"),  # Significant wave height (m)
    ("DPD", "dominant_wave_period_s"),  # Dominant wave period (sec)
    ("APD", "average_wave_period_s"),  # Average wave period (sec)
    ("MWD", "wave_direction_deg"),  # Wave direction (degrees from true N)
    ("PRES", "pressure_hpa"),  # Sea level pressure (hPa)
    ("ATMP", "air_temp_c"),  # Air temperature (Celsius)
    ("WTMP", "water_temp_c"),  # Water temperature (Celsius)
    ("DEWP", "dewpoint_temp_c"),  # Dew point temperature (Celsius)
    ("VIS", "visibility_nm"),  # Visibility (nautical miles)
    ("PTDY", "pressure_tendency_hpa"),  # Pressure tendency (hPa)
    ("TIDE", "water_level_ft"),  # Water level (ft above/below MLLW)
]

DB_COLUMNS = [col[1] for col in NOAA_COLUMNS]

# ------------------------------------------------------------


def get_connection_pool():
    global RDS_connection_pool
    if not RDS_connection_pool or RDS_connection_pool.closed:
        RDS_connection_pool = psycopg2.pool.SimpleConnectionPool(
            1,
            3,
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            connect_timeout=5,
        )
    return RDS_connection_pool


def get_db_connection(retries=3):
    for attempt in range(retries):
        try:
            pool = get_connection_pool()
            return pool.getconn()
        except Exception as e:
            if attempt == retries - 1:
                raise
            time.sleep(2**attempt)
            logger.warning(f"DB connection attempt {attempt + 1} failed: {e}")


def return_connection(conn):
    if conn and RDS_connection_pool and not RDS_connection_pool.closed:
        RDS_connection_pool.putconn(conn)


def ensure_table_exists():
    """Create the buoy_data table with NOAA standard column names"""
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS buoy_data (
                    id SERIAL PRIMARY KEY,
                    station_id VARCHAR(10) NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    wind_direction_deg DECIMAL(5,1),      -- degrees from true N
                    wind_speed_ms DECIMAL(5,1),           -- m/s
                    gust_speed_ms DECIMAL(5,1),           -- m/s
                    wave_height_m DECIMAL(5,2),           -- meters
                    dominant_wave_period_s DECIMAL(5,1),  -- seconds
                    average_wave_period_s DECIMAL(5,1),   -- seconds
                    wave_direction_deg DECIMAL(5,1),      -- degrees from true N
                    pressure_hpa DECIMAL(7,1),            -- hPa
                    air_temp_c DECIMAL(5,1),              -- Celsius
                    water_temp_c DECIMAL(5,1),            -- Celsius
                    dewpoint_temp_c DECIMAL(5,1),         -- Celsius
                    visibility_nm DECIMAL(5,1),           -- nautical miles
                    pressure_tendency_hpa DECIMAL(5,1),   -- hPa
                    water_level_ft DECIMAL(6,2),          -- feet above/below MLLW
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(station_id, timestamp)
                )
            """
            )
            conn.commit()
    finally:
        return_connection(conn)


def get_latest_timestamp(station_id):
    """Get the latest timestamp for a station from RDS"""
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT MAX(timestamp) FROM buoy_data WHERE station_id = %s",
                (station_id,),
            )
            result = cur.fetchone()
            return result[0] if result[0] else None
    except Exception as e:
        logger.error(f"Error getting latest timestamp: {e}")
        return None
    finally:
        return_connection(conn)


def validate_sensor_value(val, col_name):
    """Basic validation for sensor values"""
    if val is None:
        return None

    if col_name.endswith("_deg") and not (0 <= val <= 360):
        return None
    elif col_name in ["wave_height_m", "wind_speed_ms", "gust_speed_ms"] and val < 0:
        return None
    elif col_name.endswith("_c") and not (-50 <= val <= 50):
        return None

    return val


def insert_buoy_rows(station_id, data_rows):
    """Insert new buoy data into RDS using NOAA standard format"""
    if not data_rows:
        return 0

    inserted_count = 0
    conn = None
    batch_size = 100

    try:
        conn = get_db_connection()
        conn.autocommit = False

        with conn.cursor() as cur:
            for i, row in enumerate(data_rows):
                try:
                    if len(row) < 5:
                        continue

                    timestamp = datetime.datetime(*map(int, row[:5]))

                    data_values = []
                    for idx, val in enumerate(row[5:]):
                        if val == "MM" or val == "" or val == "999" or val == "9999":
                            data_values.append(None)
                        else:
                            try:
                                float_val = float(val)
                                if idx < len(DB_COLUMNS):
                                    float_val = validate_sensor_value(
                                        float_val, DB_COLUMNS[idx]
                                    )
                                data_values.append(float_val)
                            except ValueError:
                                data_values.append(None)

                    while len(data_values) < len(DB_COLUMNS):
                        data_values.append(None)

                    columns = ", ".join(DB_COLUMNS)
                    placeholders = ", ".join(["%s"] * len(DB_COLUMNS))

                    cur.execute(
                        f"""
                        INSERT INTO buoy_data
                        (station_id, timestamp, {columns})
                        VALUES (%s, %s, {placeholders})
                        ON CONFLICT (station_id, timestamp) DO NOTHING
                        """,
                        (station_id, timestamp, *data_values[: len(DB_COLUMNS)]),
                    )

                    if cur.rowcount > 0:
                        inserted_count += 1

                    if (i + 1) % batch_size == 0:
                        conn.commit()

                except Exception as e:
                    logger.warning(f"Error inserting row {row}: {e}")
                    conn.rollback()
                    continue

            conn.commit()

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error in insert_buoy_rows: {e}")
        raise
    finally:
        return_connection(conn)

    return inserted_count


def handler(event, context):
    """
    AWS Lambda handler function to fetch and store NOAA buoy data in RDS
    """
    request_id = context.request_id if context else "local"
    logger.info(f"Starting request {request_id} for station {STATION_ID}")

    try:
        ensure_table_exists()

        table = dynamodb.Table(TABLE_NAME)

        for attempt in range(3):
            try:
                response = table.get_item(Key={"station_id": STATION_ID})
                state = response.get("Item", {})
                last_modified = state.get("last_modified")
                break
            except Exception as e:
                if attempt == 2:
                    logger.error(f"Failed to read DynamoDB state: {e}")
                    last_modified = None
                else:
                    time.sleep(1)

        latest_ts = get_latest_timestamp(STATION_ID)

        session = requests.Session()
        headers = {"If-Modified-Since": last_modified} if last_modified else {}

        try:
            resp = session.get(URL, headers=headers, timeout=30, stream=True)
            resp.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data from NOAA: {e}")
            return {
                "statusCode": 503,
                "body": json.dumps(
                    {
                        "error": "Failed to fetch data from NOAA",
                        "request_id": request_id,
                    }
                ),
            }

        if resp.status_code == 200:
            new_data_rows = []
            parse_errors = 0

            for line in resp.iter_lines(decode_unicode=True):
                if not line or line.startswith("#"):
                    continue

                parts = line.split()
                try:
                    if len(parts) >= 5:
                        cur_line_ts = datetime.datetime(*map(int, parts[:5]))

                        if latest_ts and cur_line_ts <= latest_ts:
                            continue

                        new_data_rows.append(parts)
                except (ValueError, IndexError) as e:
                    parse_errors += 1
                    if parse_errors <= 5:
                        logger.warning(f"Error parsing line: {line[:50]}... - {e}")
                    continue

            if parse_errors > 5:
                logger.warning(f"Total parse errors: {parse_errors}")

            inserted_count = 0
            if new_data_rows:
                try:
                    inserted_count = insert_buoy_rows(STATION_ID, new_data_rows)

                    if inserted_count > 0:
                        latest_ts = datetime.datetime(*map(int, new_data_rows[0][:5]))
                        for attempt in range(3):
                            try:
                                table.put_item(
                                    Item={
                                        "station_id": STATION_ID,
                                        "last_modified": resp.headers.get(
                                            "Last-Modified"
                                        ),
                                        "latest_ts": latest_ts.isoformat(),
                                        "request_id": request_id,
                                        "updated_at": datetime.datetime.utcnow().isoformat(),
                                    }
                                )
                                break
                            except Exception as e:
                                if attempt == 2:
                                    logger.error(
                                        f"Failed to update DynamoDB state: {e}"
                                    )
                                else:
                                    time.sleep(1)

                except Exception as e:
                    logger.error(f"Failed to insert data: {e}")
                    return {
                        "statusCode": 500,
                        "body": json.dumps(
                            {
                                "error": "Failed to insert data",
                                "request_id": request_id,
                                "rows_processed": len(new_data_rows),
                            }
                        ),
                    }

            return {
                "statusCode": 200,
                "body": json.dumps(
                    {
                        "message": f"Successfully inserted {inserted_count} new records",
                        "total_rows_processed": len(new_data_rows),
                        "parse_errors": parse_errors,
                        "latest_timestamp": (
                            latest_ts.isoformat() if latest_ts else None
                        ),
                        "request_id": request_id,
                    }
                ),
            }

        elif resp.status_code == 304:
            return {
                "statusCode": 200,
                "body": json.dumps(
                    {
                        "message": "Data not modified since last check",
                        "request_id": request_id,
                    }
                ),
            }

        else:
            return {
                "statusCode": resp.status_code,
                "body": json.dumps(
                    {
                        "error": f"Unexpected status code: {resp.status_code}",
                        "request_id": request_id,
                    }
                ),
            }

    except Exception as e:
        logger.error(f"Lambda error: {e}", exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps(
                {"error": "Internal server error", "request_id": request_id}
            ),
        }
    finally:
        if RDS_connection_pool and not RDS_connection_pool.closed:
            RDS_connection_pool.closeall()
