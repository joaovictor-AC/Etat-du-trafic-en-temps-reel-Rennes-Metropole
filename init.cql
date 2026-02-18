-- ============================================================
-- Cassandra Schema for Rennes Traffic Lambda Architecture
-- ============================================================
-- Run this file with:
-- docker exec -i etat-du-trafic-en-temps-reel-rennes-metropole-cassandra-1 cqlsh < init.cql

-- ------------------------------------------------------------
-- KEYSPACE
-- SimpleStrategy is fine for a single-node dev environment.
-- Replication factor 1 means there is only 1 copy of the data.
-- ------------------------------------------------------------
CREATE KEYSPACE IF NOT EXISTS traffic_keyspace
    WITH replication = {
        'class': 'SimpleStrategy',
        'replication_factor': 1
    };

-- Use this keyspace for all tables below
USE traffic_keyspace;

-- ------------------------------------------------------------
-- TABLE 1: Speed Layer (Hot Data)
-- Goal: Store the LATEST status of each street for the real-time map.
--
-- Design decision:
--   street_name is the full PRIMARY KEY (partition key only).
--   This means each INSERT for the same street OVERWRITES the old row.
--   That is exactly what we want: always keep only the latest snapshot.
--
-- Cassandra guarantees O(1) lookup by street_name.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS traffic_realtime (
    street_name  TEXT,
    last_updated TIMESTAMP,
    speed        INT,
    speed_limit  INT,
    status_color TEXT,   -- 'GREEN' (normal) or 'RED' (speeding)
    lat          DOUBLE,
    lon          DOUBLE,
    PRIMARY KEY (street_name)
);

-- ------------------------------------------------------------
-- TABLE 2: Batch Layer (Cold Data)
-- Goal: Store daily aggregated history for trend analysis.
--
-- Design decision:
--   street_name  = partition key  -> all days for one street live together.
--   record_date  = clustering key -> rows are sorted by date inside the partition.
--   WITH CLUSTERING ORDER BY (record_date DESC) means the most recent
--   day comes first when you query a street's history.
--
-- This supports efficient queries like:
--   "Give me the last 30 days of data for Rue de Fougeres"
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS traffic_history (
    street_name     TEXT,
    record_date     DATE,
    avg_speed       DOUBLE,
    violation_count INT,
    PRIMARY KEY (street_name, record_date)
) WITH CLUSTERING ORDER BY (record_date DESC);
