-- =====================================================================
-- Real-Time Flight & Airport Operations Analytics
-- Step 4a: Dedup + Journey Segmentation (SQL portion)
-- =====================================================================
-- This file handles the parts that SQL genuinely excels at: window
-- functions for deduplication and gap-based journey segmentation.
-- Geospatial matching (nearest airport, congestion radius) is done in
-- step4b_build_gold_tables.py using NumPy instead — see that file's
-- header comment for why.
--
-- Safe to re-run any time; drops and rebuilds from raw_state_vectors.
-- Run with:  mysql -u root -p flight_ops_analytics < step4a_dedup_and_journeys.sql
-- =====================================================================

USE flight_ops_analytics;

-- ---------------------------------------------------------------------
-- Deduplication: keep the most recently ingested copy of each
-- (icao24, last_contact) reading, in case overlapping ingestion polls
-- captured it twice.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS stg_state_vectors;

CREATE TABLE stg_state_vectors AS
SELECT * FROM (
    SELECT
        rsv.*,
        ROW_NUMBER() OVER (
            PARTITION BY icao24, last_contact
            ORDER BY ingested_at DESC
        ) AS rn
    FROM raw_state_vectors rsv
) ranked
WHERE rn = 1;

ALTER TABLE stg_state_vectors DROP COLUMN rn;
ALTER TABLE stg_state_vectors ADD PRIMARY KEY (id);
CREATE INDEX idx_stg_icao_time ON stg_state_vectors (icao24, last_contact);


-- ---------------------------------------------------------------------
-- Journey segmentation: group each aircraft's pings into distinct
-- flight legs. A gap larger than SESSION_GAP_SECONDS between
-- consecutive pings means the aircraft went out of coverage (landed,
-- or left the tracked airspace) — the next ping starts a new journey.
-- ---------------------------------------------------------------------
SET @session_gap_seconds = 1800;  -- 30 minutes

DROP TABLE IF EXISTS stg_flight_journeys;

CREATE TABLE stg_flight_journeys AS
WITH gapped AS (
    SELECT
        *,
        LAG(last_contact) OVER (
            PARTITION BY icao24 ORDER BY last_contact
        ) AS prev_contact
    FROM stg_state_vectors
),
flagged AS (
    SELECT
        *,
        CASE
            WHEN prev_contact IS NULL THEN 1
            WHEN last_contact - prev_contact > @session_gap_seconds THEN 1
            ELSE 0
        END AS is_new_journey
    FROM gapped
),
numbered AS (
    SELECT
        *,
        SUM(is_new_journey) OVER (
            PARTITION BY icao24 ORDER BY last_contact
            ROWS UNBOUNDED PRECEDING
        ) AS journey_seq
    FROM flagged
)
SELECT
    CONCAT(icao24, '_', journey_seq) AS journey_id,
    icao24,
    ANY_VALUE(callsign) AS callsign,
    MIN(last_contact) AS start_time,
    MAX(last_contact) AS end_time,
    (MAX(last_contact) - MIN(last_contact)) / 60.0 AS duration_minutes,
    SUBSTRING_INDEX(GROUP_CONCAT(latitude ORDER BY last_contact ASC), ',', 1) + 0.0 AS start_lat,
    SUBSTRING_INDEX(GROUP_CONCAT(longitude ORDER BY last_contact ASC), ',', 1) + 0.0 AS start_lon,
    SUBSTRING_INDEX(GROUP_CONCAT(latitude ORDER BY last_contact DESC), ',', 1) + 0.0 AS end_lat,
    SUBSTRING_INDEX(GROUP_CONCAT(longitude ORDER BY last_contact DESC), ',', 1) + 0.0 AS end_lon,
    AVG(velocity_mps) AS avg_velocity_mps,
    MAX(baro_altitude_m) AS max_altitude_m,
    AVG(baro_altitude_m) AS avg_altitude_m,
    COUNT(*) AS num_pings
FROM numbered
GROUP BY icao24, journey_seq;

ALTER TABLE stg_flight_journeys ADD PRIMARY KEY (journey_id);

-- Sanity check
SELECT 'stg_state_vectors' AS table_name, COUNT(*) AS row_count FROM stg_state_vectors
UNION ALL
SELECT 'stg_flight_journeys', COUNT(*) FROM stg_flight_journeys;
