#!/bin/bash
# Runs once, on the first start of an empty ClickHouse data volume (docker-entrypoint-initdb.d).
# Creates the database, three tables and two least-privilege users. Passwords come from the
# environment (see .env.example); nothing secret is stored in this file.
set -euo pipefail

: "${BSA_WRITER_PASSWORD:?BSA_WRITER_PASSWORD must be set}"
: "${BSA_READER_PASSWORD:?BSA_READER_PASSWORD must be set}"

clickhouse client --user "${CLICKHOUSE_USER}" --password "${CLICKHOUSE_PASSWORD}" --multiquery <<SQL
CREATE DATABASE IF NOT EXISTS bsa;

-- Accepted samples. One row per (cell, cycle, seq).
-- ReplacingMergeTree: rows with the same ORDER BY key are collapsed during background merges,
-- keeping the one with the highest inserted_at. A sample written twice after a Flink restart
-- therefore ends up once; queries that must be exact use FINAL.
CREATE TABLE IF NOT EXISTS bsa.telemetry
(
    cell_id        LowCardinality(String),
    cycle          UInt32,
    seq            UInt32,
    event_time_ms  Int64,
    event_time     DateTime64(3, 'UTC') DEFAULT fromUnixTimestamp64Milli(event_time_ms, 'UTC'),
    voltage_v      Float64,
    current_a      Float64,
    temperature_c  Float64,
    ingest_ms      Int64,
    inserted_at    DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMM(event_time)
ORDER BY (cell_id, event_time, cycle, seq);

-- Alerts. alert_id is deterministic (cell, type, window/cycle/sample), so a re-emitted alert has the
-- same key as the original and collapses with it.
CREATE TABLE IF NOT EXISTS bsa.alerts
(
    alert_id        UUID,
    cell_id         LowCardinality(String),
    alert_type      LowCardinality(String),
    severity        LowCardinality(String),
    event_time_ms   Int64,
    event_time      DateTime64(3, 'UTC') DEFAULT fromUnixTimestamp64Milli(event_time_ms, 'UTC'),
    window_start_ms Int64,
    window_end_ms   Int64,
    cycle           Int32,
    value           Nullable(Float64),
    threshold       Nullable(Float64),
    detail          String,
    max_ingest_ms   Int64,
    emitted_ms      Int64,
    inserted_at     DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(inserted_at)
PARTITION BY toYYYYMM(event_time)
ORDER BY (cell_id, alert_type, event_time, alert_id);

-- One row per discharge cycle (small table: no partitioning).
CREATE TABLE IF NOT EXISTS bsa.cycles
(
    cell_id             LowCardinality(String),
    cycle               UInt32,
    start_ms            Int64,
    end_ms              Int64,
    samples             UInt32,
    missing_samples     UInt32,
    capacity_ah         Float64,
    min_voltage_v       Float64,
    max_temperature_c   Float64,
    initial_capacity_ah Nullable(Float64),
    rolling_baseline_ah Nullable(Float64),
    soh                 Nullable(Float64),
    max_ingest_ms       Int64,
    inserted_at         DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (cell_id, cycle);

-- The Flink job may only insert; the API may only read.
CREATE USER IF NOT EXISTS bsa_writer IDENTIFIED WITH sha256_password BY '${BSA_WRITER_PASSWORD}';
CREATE USER IF NOT EXISTS bsa_reader IDENTIFIED WITH sha256_password BY '${BSA_READER_PASSWORD}' SETTINGS readonly = 2;
GRANT INSERT ON bsa.* TO bsa_writer;
GRANT SELECT ON bsa.* TO bsa_reader;
SQL

echo "bsa schema and users created"
