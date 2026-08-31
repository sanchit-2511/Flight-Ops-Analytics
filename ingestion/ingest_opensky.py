"""
Real-Time Flight & Airport Operations Analytics
Ingestion Pipeline (containerized)

Continuously polls the OpenSky Network REST API for live aircraft state
vectors within the tracked airspace corridor and bulk-inserts them into
raw_state_vectors (Bronze zone).

Every poll cycle is tracked as one batch in ingestion_log, so you can
monitor pipeline health (records fetched/inserted, failures) over time.

All configuration is read from environment variables (see .env.example
at the project root). This script is meant to run as the `ingestion`
service in docker-compose.yml, polling forever until the container is
stopped.
"""

import os
import sys
import time
import uuid
from datetime import datetime, timezone

import requests
import mysql.connector
from mysql.connector import Error

# ---------------------------------------------------------------------
# Config — all from environment (set via .env / docker-compose.yml)
# ---------------------------------------------------------------------
DB_CONFIG = {
    "host": os.getenv("FLIGHT_DB_HOST", "mysql"),
    "user": os.getenv("FLIGHT_DB_USER", "root"),
    "password": os.getenv("FLIGHT_DB_PASSWORD", ""),
    "database": os.getenv("FLIGHT_DB_NAME", "flight_ops_analytics"),
}

# Leave both blank to run anonymously (works, but rate-limited to
# 400 credits/day — fine for short test runs, not for hours of polling).
# NEVER hardcode these — set them in your local .env file, which is
# gitignored and never baked into the image.
CLIENT_ID = os.getenv("OPENSKY_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("OPENSKY_CLIENT_SECRET", "")

TOKEN_URL = (
    "https://auth.opensky-network.org/auth/realms/"
    "opensky-network/protocol/openid-connect/token"
)
STATES_URL = "https://opensky-network.org/api/states/all"

# Bounding box roughly covering the tracked corridor (generous buffer
# so we capture en-route traffic, not just terminal areas). Overridable
# via env so you don't have to rebuild the image to change coverage.
BOUNDING_BOX = {
    "lamin": float(os.getenv("BBOX_LAMIN", "8.0")),
    "lomin": float(os.getenv("BBOX_LOMIN", "68.0")),
    "lamax": float(os.getenv("BBOX_LAMAX", "31.0")),
    "lomax": float(os.getenv("BBOX_LOMAX", "81.0")),
}

POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))
REQUEST_TIMEOUT_SECONDS = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "15"))

# "loop" = original Docker behavior, poll forever until stopped.
# "once" = run exactly one poll cycle then exit — used by the GitHub
# Actions workflow, which is itself the scheduler (cron), so the script
# shouldn't try to be its own scheduler too.
RUN_MODE = os.getenv("RUN_MODE", "loop")

DB_CONNECT_RETRIES = 10
DB_CONNECT_RETRY_DELAY_SECONDS = 5


# ---------------------------------------------------------------------
# OAuth2 token management (client credentials flow)
# ---------------------------------------------------------------------
class TokenManager:
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = None
        self.expires_at = 0

    @property
    def enabled(self):
        return bool(self.client_id and self.client_secret)

    def get_token(self):
        if not self.enabled:
            return None
        if self.token and time.time() < self.expires_at:
            return self.token
        return self._refresh()

    def _refresh(self):
        resp = requests.post(
            TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        resp.raise_for_status()
        data = resp.json()
        self.token = data["access_token"]
        # refresh a bit early to avoid using an expired token mid-request
        self.expires_at = time.time() + data.get("expires_in", 1800) - 30
        return self.token

    def auth_headers(self):
        token = self.get_token()
        return {"Authorization": f"Bearer {token}"} if token else {}


# ---------------------------------------------------------------------
# Fetch + parse
# ---------------------------------------------------------------------
def fetch_states(token_manager):
    resp = requests.get(
        STATES_URL,
        params=BOUNDING_BOX,
        headers=token_manager.auth_headers(),
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    resp.raise_for_status()
    return resp.json()


def parse_state_vector(s, batch_id):
    """Map one OpenSky state-vector array to a raw_state_vectors row tuple."""
    def clean_callsign(cs):
        return cs.strip() if cs else None

    return (
        s[0],                      # icao24
        clean_callsign(s[1]),      # callsign
        s[2],                      # origin_country
        s[3],                      # time_position
        s[4],                      # last_contact
        s[5],                      # longitude
        s[6],                      # latitude
        s[7],                      # baro_altitude_m
        1 if s[8] else 0,          # on_ground
        s[9],                      # velocity_mps
        s[10],                     # true_track_deg
        s[11],                     # vertical_rate_mps
        s[13],                     # geo_altitude_m
        s[14],                     # squawk
        1 if s[15] else 0,         # spi
        s[16],                     # position_source
        batch_id,
    )


# ---------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------
INSERT_STATE_SQL = """
    INSERT INTO raw_state_vectors
        (icao24, callsign, origin_country, time_position, last_contact,
         longitude, latitude, baro_altitude_m, on_ground, velocity_mps,
         true_track_deg, vertical_rate_mps, geo_altitude_m, squawk, spi,
         position_source, batch_id)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

LOG_START_SQL = """
    INSERT INTO ingestion_log (batch_id, source, started_at, status)
    VALUES (%s, 'opensky', %s, 'RUNNING')
"""

LOG_COMPLETE_SQL = """
    UPDATE ingestion_log
    SET completed_at = %s, records_fetched = %s, records_inserted = %s, status = %s
    WHERE batch_id = %s
"""

LOG_FAIL_SQL = """
    UPDATE ingestion_log
    SET completed_at = %s, status = 'FAILED', error_message = %s
    WHERE batch_id = %s
"""


def connect_with_retry():
    """
    In Docker, the ingestion container can start before MySQL is ready
    to accept connections even with a healthcheck-gated depends_on (the
    healthcheck passes, but there's always a small race). Retry instead
    of crash-looping.
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


def run_one_cycle(conn, token_manager):
    batch_id = f"batch_{uuid.uuid4().hex[:12]}"
    started_at = datetime.now(timezone.utc)
    cursor = conn.cursor()

    cursor.execute(LOG_START_SQL, (batch_id, started_at))
    conn.commit()

    try:
        payload = fetch_states(token_manager)
        states = payload.get("states") or []
        records = [parse_state_vector(s, batch_id) for s in states]

        if records:
            cursor.executemany(INSERT_STATE_SQL, records)
            conn.commit()

        cursor.execute(
            LOG_COMPLETE_SQL,
            (datetime.now(timezone.utc), len(states), len(records), "SUCCESS", batch_id),
        )
        conn.commit()

        print(f"[{started_at.strftime('%H:%M:%S')}] batch {batch_id}: "
              f"{len(records)} aircraft states ingested.")

    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code if e.response is not None else "?"
        msg = f"HTTP {status_code}: {e}"
        cursor.execute(LOG_FAIL_SQL, (datetime.now(timezone.utc), msg, batch_id))
        conn.commit()
        print(f"[{started_at.strftime('%H:%M:%S')}] batch {batch_id} FAILED: {msg}",
              file=sys.stderr)
        if status_code == 429:
            print("Rate limited by OpenSky — consider increasing "
                  "POLL_INTERVAL_SECONDS or adding API credentials.",
                  file=sys.stderr)

    except Exception as e:
        cursor.execute(LOG_FAIL_SQL, (datetime.now(timezone.utc), str(e), batch_id))
        conn.commit()
        print(f"[{started_at.strftime('%H:%M:%S')}] batch {batch_id} FAILED: {e}",
              file=sys.stderr)

    finally:
        cursor.close()


def main():
    token_manager = TokenManager(CLIENT_ID, CLIENT_SECRET)
    if not token_manager.enabled:
        print("Running in ANONYMOUS mode (no OPENSKY_CLIENT_ID/SECRET set). "
              "Rate limit: 400 credits/day.")
    else:
        print("Running AUTHENTICATED (OAuth2 client credentials).")

    conn = connect_with_retry()

    if RUN_MODE == "once":
        print("RUN_MODE=once: running a single poll cycle and exiting.")
        run_one_cycle(conn, token_manager)
        conn.close()
        return

    print(f"Polling every {POLL_INTERVAL_SECONDS}s. Ctrl+C to stop.\n")

    try:
        while True:
            cycle_start = time.time()
            try:
                if not conn.is_connected():
                    conn.reconnect(attempts=3, delay=5)
                run_one_cycle(conn, token_manager)
            except Error as e:
                print(f"Lost MySQL connection mid-cycle, reconnecting: {e}", file=sys.stderr)
                conn = connect_with_retry()
            elapsed = time.time() - cycle_start
            sleep_for = max(0, POLL_INTERVAL_SECONDS - elapsed)
            time.sleep(sleep_for)
    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        if conn.is_connected():
            conn.close()


if __name__ == "__main__":
    main()
