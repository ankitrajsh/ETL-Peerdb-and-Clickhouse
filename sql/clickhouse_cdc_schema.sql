-- =============================================================================
-- VIPANI — CLICKHOUSE CDC RAW TABLES
-- =============================================================================
-- These are the *landing* tables that PeerDB writes to directly via CDC.
-- They mirror the PostgreSQL source schema 1-to-1 and use ReplacingMergeTree
-- so that UPDATE/DELETE events from WAL are deduplicated automatically.
--
-- A separate view layer (or MATERIALIZED VIEW) maps these raw tables to the
-- clean analytics schema used by dashboards.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS vipaniv2;

-- =============================================================================
-- DIM RAW TABLES (CDC landing)
-- =============================================================================

-- dim_organization_raw  ← public.userApis_organization
CREATE TABLE IF NOT EXISTS vipaniv2.dim_organization_raw
(
    id                    UInt32,
    name                  String,
    business_type         LowCardinality(String),
    country               LowCardinality(String),
    state                 LowCardinality(String),
    city_name             String,
    industries            String,
    annual_turnover       String,
    no_of_employees       String,
    business_size         String,
    buyer_avg_rating      Float32,
    supplier_avg_rating   Float32,
    buyer_rating_count    UInt32,
    supplier_rating_count UInt32,
    is_active             UInt8,
    is_blocked            UInt8,
    created_date          DateTime,
    updated_date          DateTime,
    -- PeerDB CDC metadata columns
    _peerdb_synced_at     DateTime64(9) DEFAULT now64(),
    _peerdb_is_deleted    UInt8         DEFAULT 0,
    _peerdb_version       Int64         DEFAULT 0
)
ENGINE = ReplacingMergeTree(_peerdb_version)
PARTITION BY toYYYYMM(created_date)
ORDER BY (id)
COMMENT 'CDC raw: userApis_organization → PeerDB → ClickHouse';


-- dim_user_raw  ← public.userApis_user
CREATE TABLE IF NOT EXISTS vipaniv2.dim_user_raw
(
    id              UInt64,
    organization_id UInt32,
    first_name      String,
    last_name       String,
    email           String,
    user_type       LowCardinality(String),
    employee_type   LowCardinality(String),
    job_title       String,
    is_active       UInt8,
    date_joined     DateTime,
    _peerdb_synced_at  DateTime64(9) DEFAULT now64(),
    _peerdb_is_deleted UInt8         DEFAULT 0,
    _peerdb_version    Int64         DEFAULT 0
)
ENGINE = ReplacingMergeTree(_peerdb_version)
ORDER BY (id)
COMMENT 'CDC raw: userApis_user → PeerDB → ClickHouse';


-- dim_category_raw  ← public.categories
CREATE TABLE IF NOT EXISTS vipaniv2.dim_category_raw
(
    id              UInt32,
    name            String,
    code            String,
    parent_id       UInt32,
    is_active       UInt8,
    created_at      DateTime,
    _peerdb_synced_at  DateTime64(9) DEFAULT now64(),
    _peerdb_is_deleted UInt8         DEFAULT 0,
    _peerdb_version    Int64         DEFAULT 0
)
ENGINE = ReplacingMergeTree(_peerdb_version)
ORDER BY (id)
COMMENT 'CDC raw: categories → PeerDB → ClickHouse';


-- dim_product_raw  ← public.master_products
CREATE TABLE IF NOT EXISTS vipaniv2.dim_product_raw
(
    id                UInt64,
    name              String,
    code              String,
    category_id       UInt32,
    brand             String,
    uom               LowCardinality(String),
    hsn_code          String,
    is_active         UInt8,
    created_at        DateTime,
    _peerdb_synced_at  DateTime64(9) DEFAULT now64(),
    _peerdb_is_deleted UInt8         DEFAULT 0,
    _peerdb_version    Int64         DEFAULT 0
)
ENGINE = ReplacingMergeTree(_peerdb_version)
ORDER BY (id)
COMMENT 'CDC raw: master_products → PeerDB → ClickHouse';


-- dim_warehouse_raw  ← public.userApis_warehouse
CREATE TABLE IF NOT EXISTS vipaniv2.dim_warehouse_raw
(
    id              UInt32,
    org_id          UInt32,
    name            String,
    city            String,
    state           LowCardinality(String),
    pincode         String,
    is_active       UInt8,
    created_at      DateTime,
    _peerdb_synced_at  DateTime64(9) DEFAULT now64(),
    _peerdb_is_deleted UInt8         DEFAULT 0,
    _peerdb_version    Int64         DEFAULT 0
)
ENGINE = ReplacingMergeTree(_peerdb_version)
ORDER BY (id)
COMMENT 'CDC raw: userApis_warehouse → PeerDB → ClickHouse';


-- =============================================================================
-- FACT RAW TABLES (CDC landing)
-- =============================================================================

-- fact_po_raw  ← public.po_details
CREATE TABLE IF NOT EXISTS vipaniv2.fact_po_raw
(
    id                  UInt64,
    buyer_org_id        UInt32,
    supplier_org_id     UInt32,
    created_by          UInt64,
    po_number           String,
    status              UInt8,
    total_amount        Decimal(18,4),
    currency            LowCardinality(String),
    po_date             DateTime,
    expected_delivery   Date,
    delivery_address    String,
    payment_terms       String,
    is_deleted          UInt8,
    created_at          DateTime,
    updated_at          DateTime,
    _peerdb_synced_at   DateTime64(9) DEFAULT now64(),
    _peerdb_is_deleted  UInt8         DEFAULT 0,
    _peerdb_version     Int64         DEFAULT 0
)
ENGINE = ReplacingMergeTree(_peerdb_version)
PARTITION BY toYYYYMM(po_date)
ORDER BY (buyer_org_id, po_date, id)
COMMENT 'CDC raw: po_details → PeerDB → ClickHouse';


-- fact_po_item_raw  ← public.po_item
CREATE TABLE IF NOT EXISTS vipaniv2.fact_po_item_raw
(
    id              UInt64,
    po_id           UInt64,
    product_id      UInt64,
    category_id     UInt32,
    quantity        Decimal(18,4),
    unit_price      Decimal(18,4),
    total_price     Decimal(18,4),
    uom             LowCardinality(String),
    hsn_code        String,
    tax_rate        Decimal(8,4),
    tax_amount      Decimal(18,4),
    created_at      DateTime,
    _peerdb_synced_at  DateTime64(9) DEFAULT now64(),
    _peerdb_is_deleted UInt8         DEFAULT 0,
    _peerdb_version    Int64         DEFAULT 0
)
ENGINE = ReplacingMergeTree(_peerdb_version)
PARTITION BY toYYYYMM(created_at)
ORDER BY (po_id, id)
COMMENT 'CDC raw: po_item → PeerDB → ClickHouse';


-- fact_rfq_raw  ← public.bt_rfqs
CREATE TABLE IF NOT EXISTS vipaniv2.fact_rfq_raw
(
    id              UInt64,
    buyer_org_id    UInt32,
    category_id     UInt32,
    rfq_number      String,
    status          UInt8,
    total_budget    Decimal(18,4),
    currency        LowCardinality(String),
    rfq_date        DateTime,
    close_date      Date,
    quote_count     UInt16,
    awarded_to      UInt32,
    created_at      DateTime,
    updated_at      DateTime,
    _peerdb_synced_at  DateTime64(9) DEFAULT now64(),
    _peerdb_is_deleted UInt8         DEFAULT 0,
    _peerdb_version    Int64         DEFAULT 0
)
ENGINE = ReplacingMergeTree(_peerdb_version)
PARTITION BY toYYYYMM(rfq_date)
ORDER BY (buyer_org_id, rfq_date, id)
COMMENT 'CDC raw: bt_rfqs → PeerDB → ClickHouse';


-- fact_rfq_quote_raw  ← public.bt_rfq_quotes
CREATE TABLE IF NOT EXISTS vipaniv2.fact_rfq_quote_raw
(
    id              UInt64,
    rfq_id          UInt64,
    supplier_org_id UInt32,
    status          UInt8,
    quoted_amount   Decimal(18,4),
    currency        LowCardinality(String),
    quoted_at       DateTime,
    updated_at      DateTime,
    _peerdb_synced_at  DateTime64(9) DEFAULT now64(),
    _peerdb_is_deleted UInt8         DEFAULT 0,
    _peerdb_version    Int64         DEFAULT 0
)
ENGINE = ReplacingMergeTree(_peerdb_version)
PARTITION BY toYYYYMM(quoted_at)
ORDER BY (rfq_id, id)
COMMENT 'CDC raw: bt_rfq_quotes → PeerDB → ClickHouse';


-- fact_prf_raw  ← public.purchase_request_form
CREATE TABLE IF NOT EXISTS vipaniv2.fact_prf_raw
(
    id              UInt64,
    org_id          UInt32,
    requested_by    UInt64,
    category_id     UInt32,
    status          UInt8,
    estimated_value Decimal(18,4),
    currency        LowCardinality(String),
    request_date    DateTime,
    required_by     Date,
    created_at      DateTime,
    _peerdb_synced_at  DateTime64(9) DEFAULT now64(),
    _peerdb_is_deleted UInt8         DEFAULT 0,
    _peerdb_version    Int64         DEFAULT 0
)
ENGINE = ReplacingMergeTree(_peerdb_version)
PARTITION BY toYYYYMM(request_date)
ORDER BY (org_id, request_date, id)
COMMENT 'CDC raw: purchase_request_form → PeerDB → ClickHouse';


-- fact_delivery_raw  ← public.po_delivery
CREATE TABLE IF NOT EXISTS vipaniv2.fact_delivery_raw
(
    id                  UInt64,
    po_id               UInt64,
    buyer_org_id        UInt32,
    supplier_org_id     UInt32,
    warehouse_id        UInt32,
    scheduled_date      Date,
    actual_date         Date,
    status              LowCardinality(String),
    delay_days          Int16,
    created_at          DateTime,
    _peerdb_synced_at   DateTime64(9) DEFAULT now64(),
    _peerdb_is_deleted  UInt8         DEFAULT 0,
    _peerdb_version     Int64         DEFAULT 0
)
ENGINE = ReplacingMergeTree(_peerdb_version)
PARTITION BY toYYYYMM(scheduled_date)
ORDER BY (buyer_org_id, scheduled_date, id)
COMMENT 'CDC raw: po_delivery → PeerDB → ClickHouse';


-- fact_invoice_raw  ← public.po_invoice
CREATE TABLE IF NOT EXISTS vipaniv2.fact_invoice_raw
(
    id                  UInt64,
    po_id               UInt64,
    buyer_org_id        UInt32,
    supplier_org_id     UInt32,
    invoice_number      String,
    invoice_date        Date,
    due_date            Date,
    amount              Decimal(18,4),
    tax_amount          Decimal(18,4),
    paid_amount         Decimal(18,4),
    status              LowCardinality(String),
    payment_date        Date,
    created_at          DateTime,
    _peerdb_synced_at   DateTime64(9) DEFAULT now64(),
    _peerdb_is_deleted  UInt8         DEFAULT 0,
    _peerdb_version     Int64         DEFAULT 0
)
ENGINE = ReplacingMergeTree(_peerdb_version)
PARTITION BY toYYYYMM(invoice_date)
ORDER BY (buyer_org_id, invoice_date, id)
COMMENT 'CDC raw: po_invoice → PeerDB → ClickHouse';


-- fact_contract_raw  ← public.bt_contracts
CREATE TABLE IF NOT EXISTS vipaniv2.fact_contract_raw
(
    id              UInt64,
    buyer_org_id    UInt32,
    supplier_org_id UInt32,
    contract_number String,
    status          LowCardinality(String),
    total_value     Decimal(18,4),
    currency        LowCardinality(String),
    start_date      Date,
    end_date        Date,
    signed_date     Date,
    created_at      DateTime,
    _peerdb_synced_at  DateTime64(9) DEFAULT now64(),
    _peerdb_is_deleted UInt8         DEFAULT 0,
    _peerdb_version    Int64         DEFAULT 0
)
ENGINE = ReplacingMergeTree(_peerdb_version)
PARTITION BY toYYYYMM(start_date)
ORDER BY (buyer_org_id, start_date, id)
COMMENT 'CDC raw: bt_contracts → PeerDB → ClickHouse';


-- fact_inventory_movement_raw  ← public.inventory_stock
CREATE TABLE IF NOT EXISTS vipaniv2.fact_inventory_movement_raw
(
    id              UInt64,
    org_id          UInt32,
    warehouse_id    UInt32,
    product_id      UInt64,
    movement_type   LowCardinality(String),
    quantity        Decimal(18,4),
    unit_cost       Decimal(18,4),
    movement_date   Date,
    po_id           UInt64,
    created_at      DateTime,
    _peerdb_synced_at  DateTime64(9) DEFAULT now64(),
    _peerdb_is_deleted UInt8         DEFAULT 0,
    _peerdb_version    Int64         DEFAULT 0
)
ENGINE = ReplacingMergeTree(_peerdb_version)
PARTITION BY toYYYYMM(movement_date)
ORDER BY (org_id, movement_date, id)
COMMENT 'CDC raw: inventory_stock → PeerDB → ClickHouse';


-- fact_rating_raw  ← public.bt_ratings
CREATE TABLE IF NOT EXISTS vipaniv2.fact_rating_raw
(
    id              UInt64,
    rater_org_id    UInt32,
    rated_org_id    UInt32,
    po_id           UInt64,
    rating          Float32,
    comment         String,
    rated_at        DateTime,
    _peerdb_synced_at  DateTime64(9) DEFAULT now64(),
    _peerdb_is_deleted UInt8         DEFAULT 0,
    _peerdb_version    Int64         DEFAULT 0
)
ENGINE = ReplacingMergeTree(_peerdb_version)
PARTITION BY toYYYYMM(rated_at)
ORDER BY (rated_org_id, rated_at, id)
COMMENT 'CDC raw: bt_ratings → PeerDB → ClickHouse';


-- fact_engagement_raw  ← public.user_activity_logs
CREATE TABLE IF NOT EXISTS vipaniv2.fact_engagement_raw
(
    id              UInt64,
    org_id          UInt32,
    user_id         UInt64,
    event_type      LowCardinality(String),
    page_url        String,
    session_id      String,
    duration_secs   UInt32,
    event_ts        DateTime,
    _peerdb_synced_at  DateTime64(9) DEFAULT now64(),
    _peerdb_is_deleted UInt8         DEFAULT 0,
    _peerdb_version    Int64         DEFAULT 0
)
ENGINE = ReplacingMergeTree(_peerdb_version)
PARTITION BY toYYYYMM(event_ts)
ORDER BY (org_id, event_ts, id)
COMMENT 'CDC raw: user_activity_logs → PeerDB → ClickHouse';


-- =============================================================================
-- CLEAN ANALYTICS VIEWS
-- =============================================================================
-- Views that project raw CDC tables into the clean schema used by dashboards.
-- Filters out soft-deleted rows and surfaces only the latest version of each row
-- (FINAL keyword triggers ReplacingMergeTree deduplication).
-- =============================================================================

CREATE OR REPLACE VIEW vipaniv2.dim_organization AS
SELECT
    id          AS org_id,
    name        AS company_name,
    business_type,
    country, state, city_name,
    industries, annual_turnover, no_of_employees, business_size,
    buyer_avg_rating, supplier_avg_rating,
    buyer_rating_count, supplier_rating_count,
    is_active, is_blocked,
    created_date, updated_date,
    _peerdb_synced_at AS _etl_loaded_at
FROM vipaniv2.dim_organization_raw FINAL
WHERE _peerdb_is_deleted = 0;

CREATE OR REPLACE VIEW vipaniv2.dim_user AS
SELECT
    id          AS user_id,
    organization_id AS org_id,
    first_name, last_name, email,
    user_type, employee_type, job_title,
    is_active,
    date_joined AS created_date,
    _peerdb_synced_at AS _etl_loaded_at
FROM vipaniv2.dim_user_raw FINAL
WHERE _peerdb_is_deleted = 0;

CREATE OR REPLACE VIEW vipaniv2.dim_category AS
SELECT
    id       AS category_id,
    name     AS category_name,
    code     AS category_code,
    parent_id,
    is_active,
    created_at,
    _peerdb_synced_at AS _etl_loaded_at
FROM vipaniv2.dim_category_raw FINAL
WHERE _peerdb_is_deleted = 0;

CREATE OR REPLACE VIEW vipaniv2.dim_product AS
SELECT
    id         AS product_id,
    name       AS product_name,
    code       AS product_code,
    category_id, brand, uom, hsn_code, is_active, created_at,
    _peerdb_synced_at AS _etl_loaded_at
FROM vipaniv2.dim_product_raw FINAL
WHERE _peerdb_is_deleted = 0;

CREATE OR REPLACE VIEW vipaniv2.dim_warehouse AS
SELECT
    id          AS warehouse_id,
    org_id, name, city, state, pincode, is_active, created_at,
    _peerdb_synced_at AS _etl_loaded_at
FROM vipaniv2.dim_warehouse_raw FINAL
WHERE _peerdb_is_deleted = 0;

-- Fact clean views (example: fact_po, fact_rfq)
CREATE OR REPLACE VIEW vipaniv2.fact_po AS
SELECT
    id AS po_id, buyer_org_id, supplier_org_id, created_by,
    po_number, status, total_amount, currency,
    po_date     AS metric_date,
    expected_delivery, delivery_address, payment_terms,
    created_at, updated_at,
    _peerdb_synced_at AS _etl_loaded_at
FROM vipaniv2.fact_po_raw FINAL
WHERE _peerdb_is_deleted = 0 AND is_deleted = 0;

CREATE OR REPLACE VIEW vipaniv2.fact_rfq AS
SELECT
    id AS rfq_id, buyer_org_id, category_id,
    rfq_number, status, total_budget, currency,
    rfq_date    AS metric_date,
    close_date, quote_count, awarded_to,
    created_at, updated_at,
    _peerdb_synced_at AS _etl_loaded_at
FROM vipaniv2.fact_rfq_raw FINAL
WHERE _peerdb_is_deleted = 0;
