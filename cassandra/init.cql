-- Cria o Keyspace
CREATE KEYSPACE IF NOT EXISTS traffic_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE traffic_keyspace;

-- Chave de partição por data, ordenado pelo tempo e depois pela rua
CREATE TABLE IF NOT EXISTS traffic_realtime (
    date_partition text,
    timestamp_parsed timestamp,
    street_name text,
    current_speed int,
    speed_limit int,
    lat double,
    lon double,
    status_color text,
    processing_time timestamp,
    PRIMARY KEY ((date_partition), timestamp_parsed, street_name)
) WITH CLUSTERING ORDER BY (timestamp_parsed DESC);

-- Cria a tabela de Média Diária (Batch Layer)
CREATE TABLE IF NOT EXISTS traffic_daily (
    street_name text,
    record_date date,
    avg_speed_today double,
    speed_limit int,
    PRIMARY KEY (street_name, record_date)
);

-- Cria a tabela de Horários de Pico (Batch Layer)
CREATE TABLE IF NOT EXISTS traffic_hourly (
    street_name text,
    record_date date,
    hour int,
    avg_speed_hour double,
    PRIMARY KEY ((record_date), hour)
);

-- Cria a tabela de Zonas Geográficas (Batch Layer)
CREATE TABLE IF NOT EXISTS traffic_zones (
    zone_lat double,
    zone_lon double,
    record_date date,
    zone_avg_speed double,
    streets_in_zone bigint,
    PRIMARY KEY ((zone_lat, zone_lon), record_date)
);

-- Cria a tabela de Ranking Semanal (Batch Layer)
CREATE TABLE IF NOT EXISTS traffic_weekly_ranking (
    street_name text PRIMARY KEY,
    red_flag_count bigint
);