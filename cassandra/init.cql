-- Create keyspace
create keyspace if not exists rennes_traffic with replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Use the keyspace
use rennes_traffic;

-- Cassandra's tables

-- Table for real-time traffic data (Streaming Layer)
create table if not exists traffic_realtime (
    date_time timestamp,
    denomination text,
    avg_speed double,
    travel_time int,
    traffic_status text,
    geo_shape text,
    hierarchie text,
    hierarchie_dv text,
    max_speed double,
    date_partition text,
    primary key ((denomination), date_time)
) with clustering order by (date_time desc);


-- Table for mapping of road segments (Batch Layer)
create table if not exists traffic_mapping (
    date_time timestamp,
    denomination text,
    traffic_status text,
    point_order int,
    latitude double,
    longitude double,
    hierarchie text,
    primary key ((denomination, hierarchie), date_time, point_order)
) with clustering order by (date_time desc, point_order asc);

-- Table for day analysis (Batch Layer)
create table if not exists traffic_daily_analysis (
    denomination text,
    hierarchie text,
    avg_speed_today double,
    max_speed_today double,
    speed_ratio_today double,
    primary key ((denomination, hierarchie))
);