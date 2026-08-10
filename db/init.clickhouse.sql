-- ClickHouse Initialization Script for Fleet Tracker

CREATE DATABASE IF NOT EXISTS fleet;
USE fleet;

-- Asset metadata (append-mostly reference data)
CREATE TABLE IF NOT EXISTS assets
(
    id          String,
    driver_name String,
    created_at  DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY id;


-- Seed default assets
INSERT INTO assets (id, driver_name) VALUES ('asset-1', 'Alice');
INSERT INTO assets (id, driver_name) VALUES ('asset-2', 'Bob');
INSERT INTO assets (id, driver_name) VALUES ('asset-3', 'Charlie');

-- Raw telemetry history (replaces location_history hypertable)
CREATE TABLE IF NOT EXISTS location_history
(
    asset_id    String,
    time        DateTime64(3, 'UTC'),
    latitude    Float64 CODEC(ZSTD(1)),
    longitude   Float64 CODEC(ZSTD(1)),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMMDD(time)
ORDER BY (asset_id, time)
TTL toDateTime(time) + INTERVAL 6 MONTH DELETE
SETTINGS index_granularity = 8192;

-- Hourly aggregated asset statistics (continuous aggregate equivalent)
CREATE TABLE IF NOT EXISTS hourly_asset_stats
(
    asset_id   String,
    hour       DateTime,
    ping_count AggregateFunction(count),
    last_lat   AggregateFunction(argMax, Float64, DateTime64(3, 'UTC')),
    last_lng   AggregateFunction(argMax, Float64, DateTime64(3, 'UTC'))
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (asset_id, hour);

-- Materialized View to populate hourly_asset_stats automatically from location_history
CREATE MATERIALIZED VIEW IF NOT EXISTS hourly_asset_stats_mv
TO hourly_asset_stats
AS
SELECT
    asset_id,
    toStartOfHour(time) AS hour,
    countState()         AS ping_count,
    argMaxState(latitude, time)  AS last_lat,
    argMaxState(longitude, time) AS last_lng
FROM location_history
GROUP BY asset_id, hour;
