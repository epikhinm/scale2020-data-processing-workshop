CREATE TABLE scale2020.queue (
    timestamp DateTime,
    location_id UInt32,
    latitude Float64,
    longitude Float64,
    country FixedString(2),
    temperature Nullable(Float64) DEFAULT NULL,
    humidity Nullable(Float64) DEFAULT NULL,
    pressure Nullable(Float64) DEFAULT NULL,
    P1 Nullable(Float64) DEFAULT NULL,
    P2 Nullable(Float64) DEFAULT NULL
) ENGINE = Kafka
  SETTINGS kafka_broker_list = 'broker:9092',
           kafka_topic_list = 'combined',
           kafka_group_name = 'queue',
           kafka_format = 'CSV',
           kafka_skip_broken_messages = 1,
           kafka_num_consumers = 1,
           kafka_max_block_size = 1048576;

SELECT * FROM scale2020.queue LIMIT 5;

CREATE TABLE scale2020.air_quality (
    timestamp DateTime,
    location_id UInt32,
    latitude Float64,
    longitude Float64,
    country FixedString(2),
    temperature Nullable(Float64) DEFAULT NULL,
    humidity Nullable(Float64) DEFAULT NULL,
    pressure Nullable(Float64) DEFAULT NULL,
    P1 Nullable(Float64) DEFAULT NULL,
    P2 Nullable(Float64) DEFAULT NULL
) ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp);

CREATE MATERIALIZED VIEW scale2020.air_quality_mv TO scale2020.air_quality AS
SELECT * FROM scale2020.queue;

SELECT timestamp, count(*) AS sensors FROM scale2020.air_quality
GROUP BY timestamp
ORDER BY timestamp DESC;
