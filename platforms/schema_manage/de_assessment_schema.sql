-- ==========================================================================
-- de_assessment_schema.sql — Staging Layer DDL
-- DE Assessment — Star Schema
--
-- Vị trí: platforms/schema_manage/  (theo nguyên tắc SOLID — schema definition
--         là trách nhiệm riêng, không thuộc storage layer hay processing layer)
--
-- Chạy idempotent: CREATE TABLE IF NOT EXISTS + CREATE INDEX IF NOT EXISTS
-- Thứ tự: schema → lookup dims → SCD2 dims → fact table
-- ==========================================================================

CREATE SCHEMA IF NOT EXISTS staging;

-- ==========================================================================
-- LOOKUP DIMENSION TABLES  (SCD Type 1 — UPSERT in-place)
-- ==========================================================================

-- ── dim_vendor ─────────────────────────────────────────────────────────────
-- Chỉ có 2 vendor (1, 2). Nếu tên vendor đổi thì overwrite.
CREATE TABLE IF NOT EXISTS staging.dim_vendor (
    vendor_key    SERIAL       PRIMARY KEY,
    vendor_id     SMALLINT     NOT NULL UNIQUE,   -- business key từ CSV
    vendor_name   VARCHAR(100),
    created_at    TIMESTAMPTZ  DEFAULT NOW(),
    updated_at    TIMESTAMPTZ  DEFAULT NOW()
);

-- ── dim_rate ───────────────────────────────────────────────────────────────
-- rate_type: 1–5 từ CSV. Lookup nhỏ, stable.
CREATE TABLE IF NOT EXISTS staging.dim_rate (
    rate_key      SERIAL       PRIMARY KEY,
    rate_type     SMALLINT     NOT NULL UNIQUE,   -- business key từ CSV
    rate_name     VARCHAR(100),
    updated_at    TIMESTAMPTZ  DEFAULT NOW()
);

-- Seed data mặc định cho dim_rate (5 loại rate chuẩn NYC taxi)
INSERT INTO staging.dim_rate (rate_type, rate_name) VALUES
    (1, 'Standard Rate'),
    (2, 'JFK'),
    (3, 'Newark'),
    (4, 'Nassau or Westchester'),
    (5, 'Negotiated Fare')
ON CONFLICT (rate_type) DO NOTHING;

-- ── dim_event_type ─────────────────────────────────────────────────────────
-- standard, express, premium, bulk, scheduled → bảng lookup riêng
CREATE TABLE IF NOT EXISTS staging.dim_event_type (
    event_type_key  SERIAL       PRIMARY KEY,
    event_type_name VARCHAR(50)  NOT NULL UNIQUE,  -- business key từ CSV
    updated_at      TIMESTAMPTZ  DEFAULT NOW()
);

-- ── dim_payment_method ─────────────────────────────────────────────────────
-- card, cash, voucher, account → bảng lookup riêng
CREATE TABLE IF NOT EXISTS staging.dim_payment_method (
    payment_method_key  SERIAL       PRIMARY KEY,
    payment_method_name VARCHAR(50)  NOT NULL UNIQUE,  -- business key từ CSV
    updated_at          TIMESTAMPTZ  DEFAULT NOW()
);

-- ==========================================================================
-- SCD TYPE 2 DIMENSION TABLES
-- Dùng surrogate key (BIGSERIAL) + valid_from / valid_to / is_current
-- Partial unique index trên (natural_key) WHERE is_current = TRUE
--   → đảm bảo chỉ có 1 bản ghi current tại mỗi thời điểm
-- ==========================================================================

-- ── dim_zone ───────────────────────────────────────────────────────────────
-- zone_id (pickup zone) — tên zone, borough có thể thay đổi theo thời gian
CREATE TABLE IF NOT EXISTS staging.dim_zone (
    zone_key    BIGSERIAL    PRIMARY KEY,
    zone_id     INTEGER      NOT NULL,     -- natural key từ CSV
    zone_name   VARCHAR(200),              -- cần data enrichment từ nguồn ngoài
    borough     VARCHAR(100),
    -- SCD2 validity window
    valid_from  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    valid_to    TIMESTAMPTZ,               -- NULL = đang có hiệu lực
    is_current  BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ  DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_zone_current
    ON staging.dim_zone (zone_id)
    WHERE is_current = TRUE;

CREATE INDEX IF NOT EXISTS idx_dim_zone_id
    ON staging.dim_zone (zone_id);

-- ── dim_destination ────────────────────────────────────────────────────────
-- destination_id (dropoff location) — tách riêng để rõ ngữ cảnh nghiệp vụ
-- zone_key trỏ đến dim_zone để biết điểm đến thuộc zone nào
CREATE TABLE IF NOT EXISTS staging.dim_destination (
    destination_key  BIGSERIAL    PRIMARY KEY,
    destination_id   INTEGER      NOT NULL,    -- natural key từ CSV
    zone_key         BIGINT       REFERENCES staging.dim_zone(zone_key),
    destination_name VARCHAR(200),             -- cần data enrichment
    -- SCD2 validity window
    valid_from       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    valid_to         TIMESTAMPTZ,
    is_current       BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at       TIMESTAMPTZ  DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_destination_current
    ON staging.dim_destination (destination_id)
    WHERE is_current = TRUE;

CREATE INDEX IF NOT EXISTS idx_dim_destination_id
    ON staging.dim_destination (destination_id);

-- ── dim_entity ─────────────────────────────────────────────────────────────
-- Driver / Vehicle — có thể thay đổi zone_id hoạt động theo thời gian
CREATE TABLE IF NOT EXISTS staging.dim_entity (
    entity_key  BIGSERIAL    PRIMARY KEY,
    entity_id   INTEGER      NOT NULL,     -- natural key từ CSV
    zone_id     INTEGER,                   -- khu vực hoạt động chính
    -- SCD2 validity window
    valid_from  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    valid_to    TIMESTAMPTZ,
    is_current  BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ  DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_entity_current
    ON staging.dim_entity (entity_id)
    WHERE is_current = TRUE;

CREATE INDEX IF NOT EXISTS idx_dim_entity_id
    ON staging.dim_entity (entity_id);

-- ==========================================================================
-- FACT TABLE — append-only, grain = 1 chuyến đi (trip)
-- ==========================================================================

CREATE TABLE IF NOT EXISTS staging.fact_trips (
    -- Surrogate PK
    trip_key              BIGSERIAL     PRIMARY KEY,
    -- Natural key — idempotent insert via ON CONFLICT DO NOTHING
    event_id              VARCHAR(36)   NOT NULL UNIQUE,
    event_timestamp       TIMESTAMPTZ   NOT NULL,

    -- ── Dimension Foreign Keys (surrogate → snapshot đúng tại thời điểm trip) ──
    entity_key            BIGINT        REFERENCES staging.dim_entity(entity_key),
    destination_key       BIGINT        REFERENCES staging.dim_destination(destination_key),
    vendor_key            INTEGER       REFERENCES staging.dim_vendor(vendor_key),
    rate_key              INTEGER       REFERENCES staging.dim_rate(rate_key),
    event_type_key        INTEGER       REFERENCES staging.dim_event_type(event_type_key),
    payment_method_key    INTEGER       REFERENCES staging.dim_payment_method(payment_method_key),
    zone_key              BIGINT        REFERENCES staging.dim_zone(zone_key),

    -- ── Measures ─────────────────────────────────────────────────────────────
    duration              INTEGER       CHECK (duration >= 0),
    passenger_count       SMALLINT      CHECK (passenger_count >= 0),
    value                 DECIMAL(12,2),
    sub_value             DECIMAL(12,2),
    total_value           DECIMAL(12,2),

    -- ── Audit ────────────────────────────────────────────────────────────────
    _inserted_at          TIMESTAMPTZ   DEFAULT NOW(),
    _batch_id             VARCHAR(50)
);

-- Indexes cho các query phổ biến (time-range, entity, zone, destination)
CREATE INDEX IF NOT EXISTS idx_fact_trips_ts
    ON staging.fact_trips (event_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_fact_trips_entity
    ON staging.fact_trips (entity_key);

CREATE INDEX IF NOT EXISTS idx_fact_trips_date
    ON staging.fact_trips (event_timestamp);

CREATE INDEX IF NOT EXISTS idx_fact_trips_zone
    ON staging.fact_trips (zone_key);

CREATE INDEX IF NOT EXISTS idx_fact_trips_destination
    ON staging.fact_trips (destination_key);
