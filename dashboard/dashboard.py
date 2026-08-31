
"""
Real-Time Flight & Airport Operations Analytics
Step 5: Streamlit Serving Layer

Run:
    pip install -r requirements_dashboard.txt
    streamlit run dashboard.py

The dashboard reads only from the MySQL tables produced by the existing
pipeline:
    raw_state_vectors
    ingestion_log
    dim_airport
    FactFlightActivity
    AirportCongestionMetrics

No transformation is performed in the dashboard itself.
"""

import os
from datetime import datetime

import mysql.connector
import pandas as pd
import pydeck as pdk
import streamlit as st
from mysql.connector import Error

try:
    from streamlit_autorefresh import st_autorefresh
except ImportError:
    st_autorefresh = None


# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

st.set_page_config(
    page_title="Flight & Airport Operations Analytics",
    page_icon="✈️",
    layout="wide",
    initial_sidebar_state="expanded",
)

DB_CONFIG = {
    "host": os.getenv("FLIGHT_DB_HOST", "mysql"),
    "user": os.getenv("FLIGHT_DB_USER", "root"),
    "password": os.getenv("FLIGHT_DB_PASSWORD", ""),
    "database": os.getenv("FLIGHT_DB_NAME", "flight_ops_analytics"),
}


# ---------------------------------------------------------------------
# Styling
# ---------------------------------------------------------------------

st.markdown(
    """
    <style>
        .main-title {
            font-size: 2.25rem;
            font-weight: 750;
            margin-bottom: 0;
        }
        .subtitle {
            color: #6b7280;
            margin-top: 0.15rem;
            margin-bottom: 1.2rem;
        }
        .section-title {
            font-size: 1.35rem;
            font-weight: 700;
            margin-top: 0.6rem;
            margin-bottom: 0.5rem;
        }
        div[data-testid="stMetric"] {
            border: 1px solid rgba(128,128,128,0.18);
            border-radius: 12px;
            padding: 12px;
        }
    </style>
    """,
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------

@st.cache_resource
def get_connection():
    return mysql.connector.connect(**DB_CONFIG)


def run_query(sql, params=None):
    """Execute a SELECT query and return a DataFrame."""
    conn = get_connection()
    try:
        return pd.read_sql(sql, conn, params=params)
    except Exception:
        # A dropped/restarted MySQL connection can happen during development.
        try:
            conn.close()
        except Exception:
            pass
        get_connection.clear()
        conn = get_connection()
        return pd.read_sql(sql, conn, params=params)


# ---------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------

LIVE_AIRCRAFT_SQL = """
SELECT
    r.icao24,
    COALESCE(NULLIF(TRIM(r.callsign), ''), 'UNKNOWN') AS callsign,
    r.origin_country,
    r.latitude,
    r.longitude,
    r.baro_altitude_m,
    r.velocity_mps,
    r.true_track_deg,
    r.on_ground,
    r.last_contact,
    r.ingested_at,
    r.batch_id
FROM raw_state_vectors r
JOIN ingestion_log l
    ON r.batch_id = l.batch_id
WHERE l.status = 'SUCCESS'
  AND r.ingested_at >= (
      SELECT MAX(r2.ingested_at)
      FROM raw_state_vectors r2
      JOIN ingestion_log l2
          ON r2.batch_id = l2.batch_id
      WHERE l2.status = 'SUCCESS'
  ) - INTERVAL 90 SECOND
  AND r.latitude IS NOT NULL
  AND r.longitude IS NOT NULL
  AND r.on_ground = 0
ORDER BY r.ingested_at DESC
"""

PIPELINE_HEALTH_SQL = """
SELECT
    batch_id,
    source,
    started_at,
    completed_at,
    records_fetched,
    records_inserted,
    status,
    error_message
FROM ingestion_log
ORDER BY started_at DESC
LIMIT 20
"""

PIPELINE_SUMMARY_SQL = """
SELECT
    COUNT(*) AS total_batches,
    SUM(status = 'SUCCESS') AS successful_batches,
    SUM(status = 'FAILED') AS failed_batches,
    COALESCE(SUM(records_inserted), 0) AS total_records_inserted,
    MAX(started_at) AS latest_run,
    MAX(CASE WHEN status = 'SUCCESS' THEN started_at END) AS latest_success
FROM ingestion_log
"""

CONGESTION_RANKING_SQL = """
SELECT
    icao_code,
    airport_name,
    MAX(aircraft_count) AS peak_aircraft,
    ROUND(AVG(aircraft_count), 1) AS avg_aircraft,
    ROUND(AVG(avg_velocity_mps) * 3.6, 1) AS avg_speed_kmh,
    ROUND(MIN(closest_approach_km), 1) AS closest_approach_km
FROM AirportCongestionMetrics
GROUP BY airport_id, icao_code, airport_name
ORDER BY peak_aircraft DESC, avg_aircraft DESC
LIMIT 15
"""

CONGESTION_TREND_SQL = """
SELECT
    time_bucket,
    icao_code,
    airport_name,
    aircraft_count,
    avg_velocity_mps,
    avg_altitude_m,
    closest_approach_km
FROM AirportCongestionMetrics
ORDER BY time_bucket ASC
"""

FLIGHT_ACTIVITY_SQL = """
SELECT
    journey_id,
    icao24,
    COALESCE(NULLIF(TRIM(callsign), ''), 'UNKNOWN') AS callsign,
    start_time,
    end_time,
    duration_minutes,
    num_pings,
    avg_velocity_mps,
    avg_altitude_m,
    max_altitude_m,
    COALESCE(origin_icao, 'N/A') AS origin_icao,
    COALESCE(destination_icao, 'N/A') AS destination_icao,
    route_distance_km
FROM FactFlightActivity
ORDER BY start_time DESC
"""

AIRPORTS_SQL = """
SELECT
    airport_id,
    icao_code,
    iata_code,
    airport_name,
    city,
    country,
    latitude,
    longitude
FROM dim_airport
WHERE latitude IS NOT NULL
  AND longitude IS NOT NULL
"""


# ---------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------

@st.cache_data(ttl=10)
def load_live_aircraft():
    return run_query(LIVE_AIRCRAFT_SQL)


@st.cache_data(ttl=15)
def load_pipeline_health():
    return run_query(PIPELINE_HEALTH_SQL)


@st.cache_data(ttl=15)
def load_pipeline_summary():
    return run_query(PIPELINE_SUMMARY_SQL)


@st.cache_data(ttl=30)
def load_congestion_ranking():
    return run_query(CONGESTION_RANKING_SQL)


@st.cache_data(ttl=30)
def load_congestion_trend():
    return run_query(CONGESTION_TREND_SQL)


@st.cache_data(ttl=30)
def load_flight_activity():
    return run_query(FLIGHT_ACTIVITY_SQL)


@st.cache_data(ttl=300)
def load_airports():
    return run_query(AIRPORTS_SQL)


# ---------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------

def safe_number(value, default=0):
    if pd.isna(value):
        return default
    return value


def format_dt(value):
    if pd.isna(value):
        return "N/A"
    if isinstance(value, pd.Timestamp):
        return value.strftime("%d %b %Y, %H:%M:%S")
    if isinstance(value, datetime):
        return value.strftime("%d %b %Y, %H:%M:%S")
    return str(value)


# ---------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------

st.sidebar.title("✈️ Flight Ops")

st.sidebar.caption("Serving layer • MySQL → Streamlit")

auto_refresh = st.sidebar.checkbox("Auto-refresh dashboard", value=True)

refresh_seconds = st.sidebar.selectbox(
    "Refresh interval",
    [15, 30, 60, 120],
    index=1,
    disabled=not auto_refresh,
)

if st.sidebar.button("🔄 Refresh now", use_container_width=True):
    st.cache_data.clear()
    st.rerun()

st.sidebar.divider()
st.sidebar.markdown("**Pipeline architecture**")
st.sidebar.code(
    """OpenSky API
     ↓
Python ingestion
     ↓
MySQL Bronze
     ↓
SQL + Python ETL
     ↓
Gold tables
     ↓
Streamlit serving""",
    language="text",
)

if auto_refresh and st_autorefresh is not None:
    st_autorefresh(
        interval=refresh_seconds * 1000,
        key="flight_ops_refresh",
    )
elif auto_refresh and st_autorefresh is None:
    st.sidebar.warning(
        "Install streamlit-autorefresh for automatic updates."
    )


# ---------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------

st.markdown(
    '<div class="main-title">✈️ Real-Time Flight & Airport Operations Analytics</div>',
    unsafe_allow_html=True,
)
st.markdown(
    '<div class="subtitle">Live airspace activity • airport congestion • flight journeys • pipeline observability</div>',
    unsafe_allow_html=True,
)

# Connection test
try:
    summary = load_pipeline_summary()
except Exception as exc:
    st.error(
        "Could not connect to MySQL. Check that MySQL is running and that "
        "FLIGHT_DB_HOST / FLIGHT_DB_USER / FLIGHT_DB_PASSWORD / "
        "FLIGHT_DB_NAME are correct."
    )
    st.exception(exc)
    st.stop()


# ---------------------------------------------------------------------
# KPI row
# ---------------------------------------------------------------------

live_df = load_live_aircraft()
ranking_df = load_congestion_ranking()
activity_df = load_flight_activity()
health_df = load_pipeline_health()

total_batches = int(safe_number(summary.loc[0, "total_batches"]))
successful_batches = int(safe_number(summary.loc[0, "successful_batches"]))
failed_batches = int(safe_number(summary.loc[0, "failed_batches"]))
total_inserted = int(safe_number(summary.loc[0, "total_records_inserted"]))

success_rate = (
    (successful_batches / total_batches) * 100
    if total_batches else 0
)

latest_success = summary.loc[0, "latest_success"]

c1, c2, c3, c4, c5 = st.columns(5)

c1.metric("Aircraft in latest snapshot", f"{len(live_df):,}")
c2.metric("Flight journeys", f"{len(activity_df):,}")
c3.metric(
    "Airports with congestion data",
    f"{len(ranking_df):,}",
)
c4.metric("Pipeline success rate", f"{success_rate:.1f}%")
c5.metric("Records ingested", f"{total_inserted:,}")

st.caption(
    f"Latest successful ingestion: {format_dt(latest_success)}"
)


# ---------------------------------------------------------------------
# View 1 — Live Airspace Map
# ---------------------------------------------------------------------

st.markdown(
    '<div class="section-title">🌍 Live Airspace Activity</div>',
    unsafe_allow_html=True,
)

if live_df.empty:
    st.info(
        "No recent airborne telemetry was found. Start the OpenSky ingestion "
        "script and wait for the next successful polling cycle."
    )
else:
    live_df["altitude_ft"] = live_df["baro_altitude_m"] * 3.28084
    live_df["speed_kmh"] = live_df["velocity_mps"] * 3.6
    live_df["label"] = live_df["callsign"].fillna("UNKNOWN")

    # Latest successful snapshot is generally concentrated around India.
    view_state = pdk.ViewState(
        latitude=float(live_df["latitude"].mean()),
        longitude=float(live_df["longitude"].mean()),
        zoom=4.5,
        pitch=35,
    )

    aircraft_layer = pdk.Layer(
        "ScatterplotLayer",
        data=live_df,
        get_position="[longitude, latitude]",
        get_radius=5000,
        get_fill_color="[30, 136, 229, 210]",
        get_line_color="[255, 255, 255, 220]",
        line_width_min_pixels=1,
        pickable=True,
        auto_highlight=True,
    )

    map_tooltip = {
        "html": """
        <b>✈ {label}</b><br/>
        ICAO24: {icao24}<br/>
        Country: {origin_country}<br/>
        Altitude: {altitude_ft} ft<br/>
        Speed: {speed_kmh} km/h<br/>
        Heading: {true_track_deg}°<br/>
        Lat/Lon: {latitude}, {longitude}
        """,
        "style": {"backgroundColor": "white", "color": "black"},
    }

    st.pydeck_chart(
        pdk.Deck(
            layers=[aircraft_layer],
            initial_view_state=view_state,
            tooltip=map_tooltip,
            map_style=None,
        ),
        use_container_width=True,
    )

    st.caption(
        f"Showing {len(live_df):,} airborne aircraft from the latest "
        f"~90-second successful ingestion window."
    )


# ---------------------------------------------------------------------
# View 2 — Congestion Analytics
# ---------------------------------------------------------------------

st.markdown(
    '<div class="section-title">🛫 Airport Congestion Analytics</div>',
    unsafe_allow_html=True,
)

if ranking_df.empty:
    st.info("No congestion metrics available yet.")
else:
    left, right = st.columns([1.15, 1])

    with left:
        chart_df = ranking_df.head(10).copy()
        chart_df["airport_label"] = (
            chart_df["icao_code"] + " — " + chart_df["airport_name"].str.slice(0, 25)
        )
        st.bar_chart(
            chart_df.set_index("airport_label")["peak_aircraft"],
            horizontal=True,
            x_label="Peak aircraft within 50 km",
            y_label="Airport",
        )

    with right:
        display_ranking = ranking_df.copy()
        display_ranking.columns = [
            "ICAO",
            "Airport",
            "Peak Aircraft",
            "Avg Aircraft",
            "Avg Speed (km/h)",
            "Closest Approach (km)",
        ]
        st.dataframe(
            display_ranking,
            use_container_width=True,
            hide_index=True,
        )


# ---------------------------------------------------------------------
# Congestion trend
# ---------------------------------------------------------------------

if not ranking_df.empty and not load_congestion_trend().empty:
    trend_df = load_congestion_trend().copy()

    st.markdown("**Congestion trend over time**")

    selected_airports = st.multiselect(
        "Airports to compare",
        options=sorted(trend_df["icao_code"].dropna().unique().tolist()),
        default=sorted(trend_df["icao_code"].dropna().unique().tolist())[:5],
        key="congestion_airports",
    )

    if selected_airports:
        filtered_trend = trend_df[
            trend_df["icao_code"].isin(selected_airports)
        ].copy()

        pivot = filtered_trend.pivot_table(
            index="time_bucket",
            columns="icao_code",
            values="aircraft_count",
            aggfunc="max",
        ).sort_index()

        st.line_chart(
            pivot,
            x_label="Time",
            y_label="Aircraft within 50 km",
        )


# ---------------------------------------------------------------------
# View 3 — Flight Activity Table
# ---------------------------------------------------------------------

st.markdown(
    '<div class="section-title">📋 Flight Journey Activity</div>',
    unsafe_allow_html=True,
)

if activity_df.empty:
    st.info("No flight journeys available yet.")
else:
    f1, f2, f3 = st.columns(3)

    with f1:
        min_duration = float(activity_df["duration_minutes"].min())
        max_duration = float(activity_df["duration_minutes"].max())
        duration_range = st.slider(
            "Journey duration (minutes)",
            min_value=float(min_duration),
            max_value=float(max_duration) if max_duration > min_duration else min_duration + 1,
            value=(float(min_duration), float(max_duration) if max_duration > min_duration else min_duration + 1),
        )

    with f2:
        origin_filter = st.multiselect(
            "Origin airport",
            sorted(activity_df["origin_icao"].dropna().unique().tolist()),
            default=[],
        )

    with f3:
        destination_filter = st.multiselect(
            "Destination airport",
            sorted(activity_df["destination_icao"].dropna().unique().tolist()),
            default=[],
        )

    filtered_activity = activity_df[
        activity_df["duration_minutes"].between(
            duration_range[0],
            duration_range[1],
        )
    ].copy()

    if origin_filter:
        filtered_activity = filtered_activity[
            filtered_activity["origin_icao"].isin(origin_filter)
        ]

    if destination_filter:
        filtered_activity = filtered_activity[
            filtered_activity["destination_icao"].isin(destination_filter)
        ]

    table = filtered_activity.copy()
    table["start_time"] = pd.to_datetime(table["start_time"]).dt.strftime(
        "%d %b %H:%M"
    )
    table["end_time"] = pd.to_datetime(table["end_time"]).dt.strftime(
        "%d %b %H:%M"
    )
    table["avg_speed_kmh"] = (table["avg_velocity_mps"] * 3.6).round(1)
    table["avg_altitude_ft"] = (table["avg_altitude_m"] * 3.28084).round(0)
    table["max_altitude_ft"] = (table["max_altitude_m"] * 3.28084).round(0)

    table = table[
        [
            "journey_id",
            "icao24",
            "callsign",
            "start_time",
            "end_time",
            "duration_minutes",
            "num_pings",
            "origin_icao",
            "destination_icao",
            "route_distance_km",
            "avg_speed_kmh",
            "avg_altitude_ft",
            "max_altitude_ft",
        ]
    ]

    table.columns = [
        "Journey",
        "ICAO24",
        "Callsign",
        "Start",
        "End",
        "Duration (min)",
        "Pings",
        "Origin",
        "Destination",
        "Route (km)",
        "Avg Speed (km/h)",
        "Avg Altitude (ft)",
        "Max Altitude (ft)",
    ]

    st.dataframe(
        table,
        use_container_width=True,
        hide_index=True,
        height=420,
    )

    st.caption(
        f"Showing {len(table):,} of {len(activity_df):,} journeys."
    )


# ---------------------------------------------------------------------
# View 4 — Pipeline Health / Observability
# ---------------------------------------------------------------------

st.markdown(
    '<div class="section-title">🩺 Pipeline Health & Observability</div>',
    unsafe_allow_html=True,
)

health_left, health_right = st.columns([1.25, 1])

with health_left:
    if health_df.empty:
        st.info("No ingestion log records available.")
    else:
        health_display = health_df.copy()
        health_display["started_at"] = pd.to_datetime(
            health_display["started_at"]
        ).dt.strftime("%d %b %H:%M:%S")
        health_display["completed_at"] = pd.to_datetime(
            health_display["completed_at"]
        ).dt.strftime("%d %b %H:%M:%S")

        health_display = health_display[
            [
                "batch_id",
                "started_at",
                "completed_at",
                "records_fetched",
                "records_inserted",
                "status",
                "error_message",
            ]
        ]

        health_display.columns = [
            "Batch",
            "Started",
            "Completed",
            "Fetched",
            "Inserted",
            "Status",
            "Error",
        ]

        st.dataframe(
            health_display,
            use_container_width=True,
            hide_index=True,
            height=350,
        )

with health_right:
    if not health_df.empty:
        status_counts = health_df["status"].value_counts()
        st.write("**Recent batch status**")
        st.bar_chart(status_counts)

        st.write(
            "The ingestion log provides observability into the pipeline: "
            "batch execution, fetched vs inserted records, completion status "
            "and failure messages."
        )


# ---------------------------------------------------------------------
# Data lineage footer
# ---------------------------------------------------------------------

st.divider()

st.markdown(
    """
    **Data lineage**

    `OpenSky Network API` → `Python ingestion` → `raw_state_vectors`
    → `stg_state_vectors / stg_flight_journeys`
    → `FactFlightActivity / AirportCongestionMetrics`
    → **Streamlit Dashboard**
    """
)

st.caption(
    "Real-Time Flight & Airport Operations Analytics • "
    "Data Engineering Fundamentals Mini Project"
)
