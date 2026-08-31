"""
Real-Time Flight & Airport Operations Analytics
Airport Metadata Seed (containerized)

Downloads the OpenFlights airport reference dataset (~7,700 airports
worldwide, includes ICAO/IATA codes, coordinates, and IANA timezone)
and loads it into dim_airport.

Source: https://github.com/jpatokal/openflights
(This is the standard open dataset used across most aviation
data projects — maintained, free, no API key required.)

This runs as a one-shot `airport-seed` service in docker-compose.yml,
before the ingestion/etl services start. The load is an upsert
(ON DUPLICATE KEY UPDATE), so it's safe to run on every `docker compose
up` — it just refreshes the reference data.
"""

import csv
import io
import os
import sys
import time

import mysql.connector
from mysql.connector import Error
import urllib.request

# ---------------------------------------------------------------------
# Config — from environment (set via .env / docker-compose.yml)
# ---------------------------------------------------------------------
DB_CONFIG = {
    "host": os.getenv("FLIGHT_DB_HOST", "mysql"),
    "user": os.getenv("FLIGHT_DB_USER", "root"),
    "password": os.getenv("FLIGHT_DB_PASSWORD", ""),
    "database": os.getenv("FLIGHT_DB_NAME", "flight_ops_analytics"),
}

OPENFLIGHTS_URL = (
    "https://raw.githubusercontent.com/jpatokal/openflights/"
    "master/data/airports.dat"
)

DB_CONNECT_RETRIES = 10
DB_CONNECT_RETRY_DELAY_SECONDS = 5

# OpenFlights airports.dat has NO header row. Columns, in order:
# 0 Airport ID, 1 Name, 2 City, 3 Country, 4 IATA, 5 ICAO,
# 6 Latitude, 7 Longitude, 8 Altitude(ft), 9 Timezone(UTC offset),
# 10 DST, 11 Tz database timezone, 12 Type, 13 Source


def null_if_placeholder(value: str):
    """OpenFlights uses the literal string \\N for missing values."""
    value = value.strip()
    if value in ("", "\\N"):
        return None
    return value


def fetch_airport_rows():
    print(f"Downloading airport dataset from {OPENFLIGHTS_URL} ...")
    with urllib.request.urlopen(OPENFLIGHTS_URL) as resp:
        raw = resp.read().decode("utf-8")

    reader = csv.reader(io.StringIO(raw))
    rows = list(reader)
    print(f"Downloaded {len(rows)} airport records.")
    return rows


def transform_rows(rows):
    """Map OpenFlights columns -> dim_airport columns, filter junk rows."""
    cleaned = []
    skipped_no_icao = 0

    for r in rows:
        if len(r) < 12:
            continue

        icao = null_if_placeholder(r[5])
        iata = null_if_placeholder(r[4])
        name = r[1].strip()
        city = null_if_placeholder(r[2])
        country = null_if_placeholder(r[3])
        tz = null_if_placeholder(r[11])

        # We require ICAO code since it's our UNIQUE key in dim_airport.
        # Airports without one (mostly tiny heliports/private strips)
        # aren't useful for commercial flight tracking anyway.
        if icao is None:
            skipped_no_icao += 1
            continue

        try:
            lat = float(r[6])
            lon = float(r[7])
        except ValueError:
            continue

        try:
            elevation_ft = int(float(r[8]))
        except ValueError:
            elevation_ft = None

        cleaned.append((icao, iata, name, city, country, lat, lon, elevation_ft, tz))

    print(f"Kept {len(cleaned)} airports with valid ICAO codes "
          f"(skipped {skipped_no_icao} without one).")
    return cleaned


def connect_with_retry():
    """
    The airport-seed service depends on mysql's healthcheck, but a small
    race is still possible right after MySQL reports healthy. Retry
    instead of crash-looping.
    """
    last_err = None
    for attempt in range(1, DB_CONNECT_RETRIES + 1):
        try:
            return mysql.connector.connect(**DB_CONFIG)
        except Error as e:
            last_err = e
            print(f"MySQL not ready yet (attempt {attempt}/{DB_CONNECT_RETRIES}): {e}",
                  file=sys.stderr)
            time.sleep(DB_CONNECT_RETRY_DELAY_SECONDS)
    print(f"Could not connect to MySQL after {DB_CONNECT_RETRIES} attempts: {last_err}",
          file=sys.stderr)
    sys.exit(1)


def load_into_mysql(records):
    conn = connect_with_retry()
    try:
        cursor = conn.cursor()

        insert_sql = """
            INSERT INTO dim_airport
                (icao_code, iata_code, airport_name, city, country,
                 latitude, longitude, elevation_ft, timezone)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                iata_code = VALUES(iata_code),
                airport_name = VALUES(airport_name),
                city = VALUES(city),
                country = VALUES(country),
                latitude = VALUES(latitude),
                longitude = VALUES(longitude),
                elevation_ft = VALUES(elevation_ft),
                timezone = VALUES(timezone)
        """

        cursor.executemany(insert_sql, records)
        conn.commit()
        print(f"Inserted/updated {cursor.rowcount} rows in dim_airport "
              f"(note: rowcount counts updates as 2, inserts as 1).")

        cursor.execute("SELECT COUNT(*) FROM dim_airport")
        total = cursor.fetchone()[0]
        print(f"dim_airport now has {total} total rows.")

    except Error as e:
        print(f"MySQL error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()


def main():
    rows = fetch_airport_rows()
    records = transform_rows(rows)
    load_into_mysql(records)
    print("Airport seed complete.")


if __name__ == "__main__":
    main()
