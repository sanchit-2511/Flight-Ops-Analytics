"""
Real-Time Flight & Airport Operations Analytics
Gold Zone Table Builder (FactFlightActivity, AirportCongestionMetrics)

WHY THIS IS PYTHON, NOT SQL
----------------------------
An earlier version of this pipeline did airport-matching entirely in
MySQL using a haversine_km() SQL function, cross-joined against every
airport. That works fine on toy data but becomes unusably slow at real
scale: MySQL scalar/stored functions carry heavy per-call overhead, and
a cross join of (telemetry rows) x (airports) can reach tens of millions
of function calls. On ~24,000 telemetry rows against just ~110 nearby
airports, that query did not finish in several minutes.

The same distance calculation, done as a single vectorized NumPy
operation (matrix broadcasting instead of row-by-row function calls),
computes the identical 2.7 million distances in well under a second.
This is a good general lesson: SQL is excellent for set-based
relational logic (the dedup + journey segmentation in step4a), but
numerically heavy operations like geospatial distance matrices are
often much faster done in the application layer with a vectorized
library than pushed into per-row SQL functions.

Prereq: stg_state_vectors / stg_flight_journeys must already exist
(built by step4a_dedup_and_journeys.sql). In the containerized setup,
pipeline_worker.py runs that SQL file immediately before calling run()
below on every ETL cycle, so this is handled automatically.

Usage (standalone):
    python build_gold_tables.py
"""

import os
import sys
import time
import warnings

import numpy as np
import pandas as pd
import mysql.connector
from mysql.connector import Error

warnings.filterwarnings("ignore", category=UserWarning)  # pandas+DBAPI2 read_sql notice

DB_CONFIG = {
    "host": os.getenv("FLIGHT_DB_HOST", "mysql"),
    "user": os.getenv("FLIGHT_DB_USER", "root"),
    "password": os.getenv("FLIGHT_DB_PASSWORD", ""),
    "database": os.getenv("FLIGHT_DB_NAME", "flight_ops_analytics"),
}

# Same corridor as the ingestion bounding box, with a small buffer so
# airports just outside the strict box aren't missed for edge-of-range
# journeys/congestion checks.
AIRPORT_LAT_RANGE = (
    float(os.getenv("BBOX_LAMIN", "6.0")),
    float(os.getenv("BBOX_LAMAX", "33.0")),
)
AIRPORT_LON_RANGE = (
    float(os.getenv("BBOX_LOMIN", "66.0")),
    float(os.getenv("BBOX_LOMAX", "83.0")),
)

AIRPORT_MATCH_RADIUS_KM = float(os.getenv("AIRPORT_MATCH_RADIUS_KM", "50"))
CONGESTION_RADIUS_KM = float(os.getenv("CONGESTION_RADIUS_KM", "50"))
CONGESTION_BUCKET_SECONDS = int(os.getenv("CONGESTION_BUCKET_SECONDS", "900"))  # 15 min


def haversine_vectorized(lat1, lon1, lat2, lon2):
    """
    Vectorized great-circle distance in km. Inputs are NumPy arrays
    that broadcast against each other (e.g. (n,1) against (1,m) to
    produce an (n,m) distance matrix).
    """
    R = 6371.0
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    return R * 2 * np.arcsin(np.sqrt(a))


def load_data(conn):
    journeys = pd.read_sql("SELECT * FROM stg_flight_journeys", conn)
    states = pd.read_sql(
        "SELECT icao24, latitude, longitude, velocity_mps, baro_altitude_m, last_contact "
        "FROM stg_state_vectors",
        conn,
    )
    airports = pd.read_sql(
        "SELECT airport_id, icao_code, airport_name, latitude, longitude FROM dim_airport "
        "WHERE latitude BETWEEN %s AND %s AND longitude BETWEEN %s AND %s",
        conn,
        params=(*AIRPORT_LAT_RANGE, *AIRPORT_LON_RANGE),
    )
    return journeys, states, airports


def build_fact_flight_activity(journeys, airports):
    if journeys.empty or airports.empty:
        return pd.DataFrame()

    a_lat = airports["latitude"].values[None, :]
    a_lon = airports["longitude"].values[None, :]

    def nearest_airport(lat_col, lon_col):
        lat = journeys[lat_col].values.astype(float)[:, None]
        lon = journeys[lon_col].values.astype(float)[:, None]
        dist = haversine_vectorized(lat, lon, a_lat, a_lon)
        idx = dist.argmin(axis=1)
        best_dist = dist.min(axis=1)
        matched = best_dist <= AIRPORT_MATCH_RADIUS_KM
        airport_ids = np.where(matched, airports["airport_id"].values[idx], None)
        icao_codes = np.where(matched, airports["icao_code"].values[idx], None)
        return airport_ids, icao_codes

    origin_id, origin_icao = nearest_airport("start_lat", "start_lon")
    dest_id, dest_icao = nearest_airport("end_lat", "end_lon")

    route_km = haversine_vectorized(
        journeys["start_lat"].values.astype(float),
        journeys["start_lon"].values.astype(float),
        journeys["end_lat"].values.astype(float),
        journeys["end_lon"].values.astype(float),
    )

    fact = pd.DataFrame({
        "journey_id": journeys["journey_id"],
        "icao24": journeys["icao24"],
        "callsign": journeys["callsign"],
        "start_time": pd.to_datetime(journeys["start_time"], unit="s"),
        "end_time": pd.to_datetime(journeys["end_time"], unit="s"),
        "duration_minutes": journeys["duration_minutes"].astype(float).round(2),
        "num_pings": journeys["num_pings"],
        "avg_velocity_mps": journeys["avg_velocity_mps"].astype(float).round(1),
        "avg_altitude_m": journeys["avg_altitude_m"].astype(float).round(1),
        "max_altitude_m": journeys["max_altitude_m"].astype(float).round(1),
        "origin_airport_id": origin_id,
        "origin_icao": origin_icao,
        "destination_airport_id": dest_id,
        "destination_icao": dest_icao,
        "route_distance_km": np.round(route_km, 1),
    })
    return fact


def build_congestion_metrics(states, airports):
    if states.empty or airports.empty:
        return pd.DataFrame()

    s_lat = states["latitude"].values[:, None]
    s_lon = states["longitude"].values[:, None]
    a_lat = airports["latitude"].values[None, :]
    a_lon = airports["longitude"].values[None, :]

    dist_matrix = haversine_vectorized(s_lat, s_lon, a_lat, a_lon)  # (n_states, n_airports)
    within_radius = dist_matrix <= CONGESTION_RADIUS_KM

    state_idx, airport_idx = np.nonzero(within_radius)
    if len(state_idx) == 0:
        return pd.DataFrame()

    pairs = pd.DataFrame({
        "airport_id": airports["airport_id"].values[airport_idx],
        "icao_code": airports["icao_code"].values[airport_idx],
        "airport_name": airports["airport_name"].values[airport_idx],
        "icao24": states["icao24"].values[state_idx],
        "velocity_mps": states["velocity_mps"].values[state_idx],
        "baro_altitude_m": states["baro_altitude_m"].values[state_idx],
        "last_contact": states["last_contact"].values[state_idx],
        "dist_km": dist_matrix[state_idx, airport_idx],
    })
    pairs["time_bucket"] = (
        (pairs["last_contact"] // CONGESTION_BUCKET_SECONDS) * CONGESTION_BUCKET_SECONDS
    )

    congestion = (
        pairs.groupby(["airport_id", "icao_code", "airport_name", "time_bucket"])
        .agg(
            aircraft_count=("icao24", "nunique"),
            avg_velocity_mps=("velocity_mps", "mean"),
            avg_altitude_m=("baro_altitude_m", "mean"),
            closest_approach_km=("dist_km", "min"),
        )
        .reset_index()
    )
    congestion["time_bucket"] = pd.to_datetime(congestion["time_bucket"], unit="s")
    congestion["avg_velocity_mps"] = congestion["avg_velocity_mps"].round(1)
    congestion["avg_altitude_m"] = congestion["avg_altitude_m"].round(1)
    congestion["closest_approach_km"] = congestion["closest_approach_km"].round(1)
    return congestion


def write_table(conn, df, table_name, create_sql, insert_sql, columns):
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    cursor.execute(create_sql)
    if not df.empty:
        records = [tuple(None if pd.isna(v) else v for v in row) for row in df[columns].itertuples(index=False)]
        cursor.executemany(insert_sql, records)
    conn.commit()
    cursor.close()
    return len(df)


def run(conn):
    """
    Build both gold tables using an already-open connection. Used both
    by main() below (standalone run) and by pipeline_worker.py (looped
    run inside the etl container).
    """
    t0 = time.time()
    journeys, states, airports = load_data(conn)
    print(f"Loaded {len(journeys)} journeys, {len(states)} state vectors, "
          f"{len(airports)} corridor airports ({time.time()-t0:.2f}s)")

    t0 = time.time()
    fact = build_fact_flight_activity(journeys, airports)
    congestion = build_congestion_metrics(states, airports)
    print(f"Computed geospatial matches in {time.time()-t0:.2f}s")

    n_fact = write_table(
        conn, fact, "FactFlightActivity",
        create_sql="""
            CREATE TABLE FactFlightActivity (
                journey_id VARCHAR(50) PRIMARY KEY,
                icao24 VARCHAR(10),
                callsign VARCHAR(20),
                start_time DATETIME,
                end_time DATETIME,
                duration_minutes DECIMAL(10,2),
                num_pings INT,
                avg_velocity_mps DECIMAL(10,1),
                avg_altitude_m DECIMAL(10,1),
                max_altitude_m DECIMAL(10,1),
                origin_airport_id INT NULL,
                origin_icao VARCHAR(4) NULL,
                destination_airport_id INT NULL,
                destination_icao VARCHAR(4) NULL,
                route_distance_km DECIMAL(10,1)
            )
        """,
        insert_sql="""
            INSERT INTO FactFlightActivity
                (journey_id, icao24, callsign, start_time, end_time, duration_minutes,
                 num_pings, avg_velocity_mps, avg_altitude_m, max_altitude_m,
                 origin_airport_id, origin_icao, destination_airport_id, destination_icao,
                 route_distance_km)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        columns=["journey_id", "icao24", "callsign", "start_time", "end_time",
                 "duration_minutes", "num_pings", "avg_velocity_mps", "avg_altitude_m",
                 "max_altitude_m", "origin_airport_id", "origin_icao",
                 "destination_airport_id", "destination_icao", "route_distance_km"],
    )

    n_congestion = write_table(
        conn, congestion, "AirportCongestionMetrics",
        create_sql="""
            CREATE TABLE AirportCongestionMetrics (
                airport_id INT,
                icao_code VARCHAR(4),
                airport_name VARCHAR(150),
                time_bucket DATETIME,
                aircraft_count INT,
                avg_velocity_mps DECIMAL(10,1),
                avg_altitude_m DECIMAL(10,1),
                closest_approach_km DECIMAL(10,1),
                PRIMARY KEY (airport_id, time_bucket)
            )
        """,
        insert_sql="""
            INSERT INTO AirportCongestionMetrics
                (airport_id, icao_code, airport_name, time_bucket, aircraft_count,
                 avg_velocity_mps, avg_altitude_m, closest_approach_km)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        columns=["airport_id", "icao_code", "airport_name", "time_bucket",
                 "aircraft_count", "avg_velocity_mps", "avg_altitude_m", "closest_approach_km"],
    )

    print(f"FactFlightActivity: {n_fact} rows")
    print(f"AirportCongestionMetrics: {n_congestion} rows")


def main():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
    except Error as e:
        print(f"Could not connect to MySQL: {e}", file=sys.stderr)
        sys.exit(1)

    run(conn)
    conn.close()


if __name__ == "__main__":
    main()
