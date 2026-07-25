-- ==========================================================================
-- schema_streaming_kafka.sql — DDL cho bảng ingestion từ Kafka
-- ==========================================================================

CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.kafka_events (
    event_id              VARCHAR(36)   PRIMARY KEY,
    event_timestamp       TIMESTAMPTZ,
    entity_id             INTEGER,
    zone_id               INTEGER,
    event_type            VARCHAR(50),
    duration              INTEGER,
    value                 DECIMAL(12,2),
    sub_value             DECIMAL(12,2),
    total_value           DECIMAL(12,2),
    payment_method        VARCHAR(50),
    passenger_count       SMALLINT,
    vendor_id             SMALLINT,
    _ingested_at          TIMESTAMPTZ   DEFAULT NOW()
);

