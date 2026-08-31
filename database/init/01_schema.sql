-- =====================================================================
-- Real-Time Flight & Airport Operations Analytics
-- Database Schema — Bronze Zone
-- =====================================================================
-- This file is auto-executed by the official MySQL Docker image on
-- first container startup (files in /docker-entrypoint-initdb.d run
-- once, only when the data directory is empty). No manual step needed.
-- =====================================================================

CREATE DATABASE IF NOT EXISTS flight_ops_analytics
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE flight_ops_analytics;

-- ---------------------------------------------------------------------
-- 1. dim_airport
-- Reference/dimension table: static airport metadata.
-- Loaded once in Step 2, refreshed rarely.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_airport (
    airport_id      INT AUTO_INCREMENT PRIMARY KEY,
    icao_code       VARCHAR(4)  NOT NULL,
    iata_code       VARCHAR(3)  NULL,
    airport_name    VARCHAR(150) NOT NULL,
    city            VARCHAR(100) NULL,
    country         VARCHAR(100) NULL,
    latitude        DECIMAL(10,6) NOT NULL,
    longitude       DECIMAL(10,6) NOT NULL,
    elevation_ft    INT NULL,
    timezone        VARCHAR(60) NULL,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE KEY uq_icao (icao_code),
    KEY idx_iata (iata_code),
    KEY idx_country (country)
) ENGINE=InnoDB;

-- ---------------------------------------------------------------------
-- 2. raw_state_vectors  (Bronze Zone)
-- One row per aircraft state-vector "ping" pulled from OpenSky.
-- Stored as close to the raw API response as possible — no dedup,
-- no derived columns here. That happens in the transformation layer.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_state_vectors (
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    icao24              VARCHAR(10)  NOT NULL,      -- unique aircraft transponder id
    callsign            VARCHAR(20)  NULL,           -- flight number / callsign
    origin_country      VARCHAR(100) NULL,
    time_position       BIGINT NULL,                 -- unix ts of last position update
    last_contact        BIGINT NOT NULL,              -- unix ts of last update of any kind
    longitude           DECIMAL(10,6) NULL,
    latitude            DECIMAL(10,6) NULL,
    baro_altitude_m     DECIMAL(10,2) NULL,           -- barometric altitude, meters
    on_ground           TINYINT(1) NOT NULL DEFAULT 0,
    velocity_mps        DECIMAL(10,2) NULL,           -- ground speed, m/s
    true_track_deg      DECIMAL(6,2) NULL,            -- heading, degrees from north
    vertical_rate_mps   DECIMAL(10,2) NULL,
    geo_altitude_m      DECIMAL(10,2) NULL,           -- geometric (GPS) altitude
    squawk              VARCHAR(10) NULL,
    spi                 TINYINT(1) NOT NULL DEFAULT 0,
    position_source     TINYINT NULL,                 -- 0=ADS-B,1=ASTERIX,2=MLAT,3=FLARM

    batch_id            VARCHAR(50) NOT NULL,          -- FK to ingestion_log
    ingested_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    KEY idx_icao24 (icao24),
    KEY idx_batch (batch_id),
    KEY idx_ingested_at (ingested_at),
    KEY idx_lat_lon (latitude, longitude),
    KEY idx_callsign (callsign)
) ENGINE=InnoDB;

-- ---------------------------------------------------------------------
-- 3. ingestion_log
-- Tracks every pipeline run for observability — how many records
-- fetched vs inserted, timing, and errors. Useful to show in the
-- dashboard ("pipeline health") and in your project report.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ingestion_log (
    batch_id            VARCHAR(50) PRIMARY KEY,
    source              VARCHAR(50) NOT NULL DEFAULT 'opensky',
    started_at          TIMESTAMP NOT NULL,
    completed_at        TIMESTAMP NULL,
    records_fetched     INT DEFAULT 0,
    records_inserted    INT DEFAULT 0,
    status              ENUM('RUNNING','SUCCESS','FAILED','PARTIAL') DEFAULT 'RUNNING',
    error_message       TEXT NULL
) ENGINE=InnoDB;

-- ---------------------------------------------------------------------
-- Sanity check
-- ---------------------------------------------------------------------
SHOW TABLES;
