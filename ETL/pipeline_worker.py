"""
Real-Time Flight & Airport Operations Analytics
ETL Orchestrator (containerized)

This is the piece that automates what used to be two manual steps:

    mysql -u root -p flight_ops_analytics < step4a_dedup_and_journeys.sql
    python build_gold_tables.py

On a loop, every ETL_INTERVAL_SECONDS, this script:
  1. Re-runs step4a_dedup_and_journeys.sql (dedup + journey segmentation)
  2. Calls build_gold_tables.run() to rebuild FactFlightActivity and
     AirportCongestionMetrics from the refreshed staging tables

Runs as the `etl` service in docker-compose.yml, independently of the
`ingestion` service — ingestion keeps polling OpenSky on its own
schedule (default 60s) while this rebuilds the gold layer on a slower
cadence (default 5 min), since journey segmentation and geospatial
matching are more expensive and don't need to run every single minute.
"""

import os
import sys
import time
from pathlib import Path

import mysql.connector
from mysql.connector import Error

import build_gold_tables

DB_CONFIG = {
    "host": os.getenv("FLIGHT_DB_HOST", "mysql"),
    "user": os.getenv("FLIGHT_DB_USER", "root"),
    "password": os.getenv("FLIGHT_DB_PASSWORD", ""),
    "database": os.getenv("FLIGHT_DB_NAME", "flight_ops_analytics"),
}

ETL_INTERVAL_SECONDS = int(os.getenv("ETL_INTERVAL_SECONDS", "300"))  # 5 minutes
RUN_MODE = os.getenv("RUN_MODE", "loop")  # "once" for GitHub Actions, "loop" for Docker
SQL_FILE = Path(__file__).parent / "step4a_dedup_and_journeys.sql"

DB_CONNECT_RETRIES = 10
DB_CONNECT_RETRY_DELAY_SECONDS = 5


def connect_with_retry():
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


def run_dedup_and_journeys_sql(conn):
    """
    Execute step4a_dedup_and_journeys.sql as a multi-statement script.
    mysql-connector-python's multi=True mode returns an iterator of
    cursors, one per statement — we have to walk it to actually execute
    everything (it's lazy).
    """
    sql_script = SQL_FILE.read_text()
    cursor = conn.cursor()
    for result in cursor.execute(sql_script, multi=True):
        if result.with_rows:
            # Only the final "sanity check" SELECT in the script returns
            # rows; surface it so it shows up in `docker compose logs etl`.
            rows = result.fetchall()
            for row in rows:
                print(f"  {row}")
    conn.commit()
    cursor.close()


def run_one_cycle(conn):
    print("Running dedup + journey segmentation (step4a)...")
    run_dedup_and_journeys_sql(conn)

    print("Rebuilding gold tables (FactFlightActivity, AirportCongestionMetrics)...")
    build_gold_tables.run(conn)


def main():
    conn = connect_with_retry()

    if RUN_MODE == "once":
        print("RUN_MODE=once: running a single ETL cycle and exiting.")
        run_one_cycle(conn)
        conn.close()
        return

    print(f"ETL worker started. Rebuilding gold layer every {ETL_INTERVAL_SECONDS}s.\n")

    try:
        while True:
            cycle_start = time.time()
            try:
                if not conn.is_connected():
                    conn.reconnect(attempts=3, delay=5)
                run_one_cycle(conn)
            except Error as e:
                print(f"ETL cycle failed (MySQL error), reconnecting: {e}", file=sys.stderr)
                conn = connect_with_retry()
            except Exception as e:
                print(f"ETL cycle failed: {e}", file=sys.stderr)

            elapsed = time.time() - cycle_start
            sleep_for = max(0, ETL_INTERVAL_SECONDS - elapsed)
            print(f"Cycle done in {elapsed:.1f}s. Sleeping {sleep_for:.0f}s.\n")
            time.sleep(sleep_for)
    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        if conn.is_connected():
            conn.close()


if __name__ == "__main__":
    main()
