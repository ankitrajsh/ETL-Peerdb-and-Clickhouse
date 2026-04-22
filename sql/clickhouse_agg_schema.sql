-- =============================================================================
-- VIPANI — CLICKHOUSE AGGREGATION SCHEMA (Post-CDC)
-- =============================================================================
-- These tables are populated by agg_refresh.py after PeerDB syncs the raw tables.
-- They are rebuilt incrementally every 10 minutes (MEDIUM_INTERVAL in scheduler).
-- =============================================================================

-- agg_daily_buyer  — B1/B2 Buyer Overview & Spend Analytics
CREATE TABLE IF NOT EXISTS vipaniv2.agg_daily_buyer
(
    metric_date         Date,
    org_id              UInt32,
    po_count            UInt32,
    po_approved_count   UInt32,
    po_pending_count    UInt32,
    po_cancelled_count  UInt32,
    total_spend         Decimal(18,4),
    approved_spend      Decimal(18,4),
    rfq_count           UInt32,
    rfq_awarded_count   UInt32,
    quote_received      UInt32,
    avg_quote_per_rfq   Float32,
    invoice_count       UInt32,
    invoice_paid        UInt32,
    invoice_overdue     UInt32,
    on_time_delivery_rate Float32,
    avg_supplier_rating Float32,
    _etl_loaded_at      DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_etl_loaded_at)
PARTITION BY toYYYYMM(metric_date)
ORDER BY (org_id, metric_date)
COMMENT 'Daily buyer KPIs — rebuilt by agg_refresh.py';


-- agg_daily_seller  — S1 Seller Overview
CREATE TABLE IF NOT EXISTS vipaniv2.agg_daily_seller
(
    metric_date         Date,
    org_id              UInt32,
    rfq_received        UInt32,
    quotes_submitted    UInt32,
    quotes_won          UInt32,
    win_rate            Float32,
    revenue             Decimal(18,4),
    avg_deal_size       Decimal(18,4),
    po_received         UInt32,
    po_fulfilled        UInt32,
    on_time_delivery    Float32,
    avg_buyer_rating    Float32,
    _etl_loaded_at      DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_etl_loaded_at)
PARTITION BY toYYYYMM(metric_date)
ORDER BY (org_id, metric_date)
COMMENT 'Daily seller KPIs — rebuilt by agg_refresh.py';


-- agg_daily_category_spend  — B5 Category Spend
CREATE TABLE IF NOT EXISTS vipaniv2.agg_daily_category_spend
(
    metric_date     Date,
    org_id          UInt32,
    category_id     UInt32,
    po_count        UInt32,
    total_spend     Decimal(18,4),
    avg_unit_price  Decimal(18,4),
    _etl_loaded_at  DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_etl_loaded_at)
PARTITION BY toYYYYMM(metric_date)
ORDER BY (org_id, category_id, metric_date)
COMMENT 'Category spend roll-up per buyer per day';


-- agg_daily_supplier_spend  — B3/B4 Supplier Risk & Pricing
CREATE TABLE IF NOT EXISTS vipaniv2.agg_daily_supplier_spend
(
    metric_date         Date,
    buyer_org_id        UInt32,
    supplier_org_id     UInt32,
    po_count            UInt32,
    total_spend         Decimal(18,4),
    avg_delivery_days   Float32,
    on_time_rate        Float32,
    rating              Float32,
    invoice_overdue_amt Decimal(18,4),
    _etl_loaded_at      DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_etl_loaded_at)
PARTITION BY toYYYYMM(metric_date)
ORDER BY (buyer_org_id, supplier_org_id, metric_date)
COMMENT 'Per-supplier spend & risk KPIs per buyer';


-- agg_daily_rfq_pipeline  — S2 RFQ Pipeline
CREATE TABLE IF NOT EXISTS vipaniv2.agg_daily_rfq_pipeline
(
    metric_date         Date,
    org_id              UInt32,
    rfq_open            UInt32,
    rfq_closed          UInt32,
    rfq_awarded         UInt32,
    avg_response_hours  Float32,
    avg_quotes_per_rfq  Float32,
    pipeline_value      Decimal(18,4),
    _etl_loaded_at      DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_etl_loaded_at)
PARTITION BY toYYYYMM(metric_date)
ORDER BY (org_id, metric_date)
COMMENT 'RFQ pipeline funnel KPIs';


-- agg_daily_revenue_territory  — S5 Territory Finance
CREATE TABLE IF NOT EXISTS vipaniv2.agg_daily_revenue_territory
(
    metric_date     Date,
    org_id          UInt32,
    state           LowCardinality(String),
    city            String,
    revenue         Decimal(18,4),
    order_count     UInt32,
    avg_order_value Decimal(18,4),
    _etl_loaded_at  DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_etl_loaded_at)
PARTITION BY toYYYYMM(metric_date)
ORDER BY (org_id, state, metric_date)
COMMENT 'Revenue by geography per seller';


-- agg_daily_engagement  — B6/S4 Engagement
CREATE TABLE IF NOT EXISTS vipaniv2.agg_daily_engagement
(
    metric_date         Date,
    org_id              UInt32,
    active_users        UInt32,
    sessions            UInt32,
    avg_session_secs    Float32,
    page_views          UInt32,
    top_event_type      String,
    _etl_loaded_at      DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_etl_loaded_at)
PARTITION BY toYYYYMM(metric_date)
ORDER BY (org_id, metric_date)
COMMENT 'Platform engagement per org per day';


-- snapshot_org_daily  — point-in-time org health
CREATE TABLE IF NOT EXISTS vipaniv2.snapshot_org_daily
(
    snapshot_date       Date,
    org_id              UInt32,
    open_po_count       UInt32,
    open_po_value       Decimal(18,4),
    open_rfq_count      UInt32,
    pending_invoices    UInt32,
    pending_invoice_amt Decimal(18,4),
    inventory_value     Decimal(18,4),
    _etl_loaded_at      DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_etl_loaded_at)
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (org_id, snapshot_date)
COMMENT 'Nightly point-in-time capture of open positions';


-- snapshot_inventory_sku_daily  — B5/S4 Inventory
CREATE TABLE IF NOT EXISTS vipaniv2.snapshot_inventory_sku_daily
(
    snapshot_date   Date,
    org_id          UInt32,
    warehouse_id    UInt32,
    product_id      UInt64,
    qty_on_hand     Decimal(18,4),
    qty_reserved    Decimal(18,4),
    qty_available   Decimal(18,4),
    unit_cost       Decimal(18,4),
    total_value     Decimal(18,4),
    _etl_loaded_at  DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_etl_loaded_at)
PARTITION BY toYYYYMM(snapshot_date)
ORDER BY (org_id, warehouse_id, product_id, snapshot_date)
COMMENT 'Daily SKU-level inventory snapshot';
