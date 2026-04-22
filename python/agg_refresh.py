#!/usr/bin/env python3
"""
agg_refresh.py
==============
Post-CDC aggregation refresh scheduler for the PeerDB ETL pipeline.

After PeerDB streams raw CDC events into ClickHouse *_raw tables, this script
rebuilds the aggregation tables (agg_daily_*, snapshot_*) on a rolling schedule.

Schedule
--------
  agg_group   — every 10 minutes  (agg_daily_*)
  snapshot_group — every 60 minutes (snapshot_*) 

Usage
-----
  python python/agg_refresh.py               # continuous scheduler
  python python/agg_refresh.py --once        # single pass, then exit
  python python/agg_refresh.py --group agg   # only agg_group
  python python/agg_refresh.py --status      # print last-refresh timestamps

Environment
-----------
  CH_HOST / CH_PORT / CH_DATABASE / CH_USER / CH_PASSWORD
  AGG_INTERVAL     (seconds, default 600)
  SNAP_INTERVAL    (seconds, default 3600)
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import clickhouse_connect

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")
except ImportError:
    pass

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
)
log = logging.getLogger("agg_refresh")

# ── ClickHouse connection ─────────────────────────────────────────────────────
def _ch_client() -> clickhouse_connect.driver.Client:
    return clickhouse_connect.get_client(
        host=os.getenv("CH_HOST", "localhost"),
        port=int(os.getenv("CH_PORT", "8123")),
        database=os.getenv("CH_DATABASE", "vipaniv2"),
        username=os.getenv("CH_USER", "default"),
        password=os.getenv("CH_PASSWORD", ""),
    )


# =============================================================================
# Aggregation SQL queries
# =============================================================================

AGG_QUERIES: dict[str, str] = {

    # ── agg_daily_buyer ──────────────────────────────────────────────────────
    "agg_daily_buyer": """
INSERT INTO vipaniv2.agg_daily_buyer
SELECT
    toDate(p.po_date)                              AS metric_date,
    p.buyer_org_id                                 AS org_id,
    count()                                        AS po_count,
    countIf(p.status = 5)                          AS po_approved_count,
    countIf(p.status IN (1,2,4))                   AS po_pending_count,
    countIf(p.status = 3)                          AS po_cancelled_count,
    sum(p.total_amount)                            AS total_spend,
    sumIf(p.total_amount, p.status = 5)            AS approved_spend,
    uniq(r.id)                                     AS rfq_count,
    countIf(r.status = 14)                         AS rfq_awarded_count,
    sum(r.quote_count)                             AS quote_received,
    avgIf(r.quote_count, r.quote_count > 0)        AS avg_quote_per_rfq,
    uniq(i.id)                                     AS invoice_count,
    countIf(i.status = 'paid')                     AS invoice_paid,
    countIf(i.status = 'overdue')                  AS invoice_overdue,
    avg(if(d.delay_days <= 0, 1, 0))               AS on_time_delivery_rate,
    avg(rt.rating)                                 AS avg_supplier_rating,
    now()                                          AS _etl_loaded_at
FROM vipaniv2.fact_po_raw FINAL p
LEFT JOIN vipaniv2.fact_rfq_raw FINAL r
       ON r.buyer_org_id = p.buyer_org_id
      AND toDate(r.rfq_date) = toDate(p.po_date)
LEFT JOIN vipaniv2.fact_invoice_raw FINAL i
       ON i.po_id = p.id
LEFT JOIN vipaniv2.fact_delivery_raw FINAL d
       ON d.po_id = p.id
LEFT JOIN vipaniv2.fact_rating_raw FINAL rt
       ON rt.rater_org_id = p.buyer_org_id
      AND toDate(rt.rated_at) = toDate(p.po_date)
WHERE p._peerdb_is_deleted = 0
  AND p.is_deleted = 0
  AND toDate(p.po_date) >= today() - 90   -- rolling 90-day window
GROUP BY metric_date, org_id
""",

    # ── agg_daily_seller ─────────────────────────────────────────────────────
    "agg_daily_seller": """
INSERT INTO vipaniv2.agg_daily_seller
SELECT
    toDate(q.quoted_at)                             AS metric_date,
    q.supplier_org_id                               AS org_id,
    uniq(r.id)                                      AS rfq_received,
    count()                                         AS quotes_submitted,
    countIf(q.status IN (6,11,14))                  AS quotes_won,
    if(count() > 0, countIf(q.status IN (6,11,14)) / count(), 0) AS win_rate,
    sum(p.total_amount)                             AS revenue,
    avg(p.total_amount)                             AS avg_deal_size,
    uniq(p.id)                                      AS po_received,
    countIf(d.delay_days <= 0)                      AS po_fulfilled,
    avg(if(d.delay_days <= 0, 1, 0))                AS on_time_delivery,
    avg(rt.rating)                                  AS avg_buyer_rating,
    now()                                           AS _etl_loaded_at
FROM vipaniv2.fact_rfq_quote_raw FINAL q
LEFT JOIN vipaniv2.fact_rfq_raw FINAL r
       ON r.id = q.rfq_id
LEFT JOIN vipaniv2.fact_po_raw FINAL p
       ON p.supplier_org_id = q.supplier_org_id
      AND toDate(p.po_date) = toDate(q.quoted_at)
LEFT JOIN vipaniv2.fact_delivery_raw FINAL d
       ON d.po_id = p.id
LEFT JOIN vipaniv2.fact_rating_raw FINAL rt
       ON rt.rated_org_id = q.supplier_org_id
      AND toDate(rt.rated_at) = toDate(q.quoted_at)
WHERE q._peerdb_is_deleted = 0
  AND toDate(q.quoted_at) >= today() - 90
GROUP BY metric_date, org_id
""",

    # ── agg_daily_category_spend ─────────────────────────────────────────────
    "agg_daily_category_spend": """
INSERT INTO vipaniv2.agg_daily_category_spend
SELECT
    toDate(p.po_date)          AS metric_date,
    p.buyer_org_id             AS org_id,
    pi.category_id,
    count()                    AS po_count,
    sum(pi.total_price)        AS total_spend,
    avg(pi.unit_price)         AS avg_unit_price,
    now()                      AS _etl_loaded_at
FROM vipaniv2.fact_po_item_raw FINAL pi
JOIN vipaniv2.fact_po_raw FINAL p ON p.id = pi.po_id
WHERE pi._peerdb_is_deleted = 0
  AND p._peerdb_is_deleted = 0
  AND p.is_deleted = 0
  AND toDate(p.po_date) >= today() - 90
GROUP BY metric_date, org_id, category_id
""",

    # ── agg_daily_supplier_spend ─────────────────────────────────────────────
    "agg_daily_supplier_spend": """
INSERT INTO vipaniv2.agg_daily_supplier_spend
SELECT
    toDate(p.po_date)                       AS metric_date,
    p.buyer_org_id,
    p.supplier_org_id,
    count()                                 AS po_count,
    sum(p.total_amount)                     AS total_spend,
    avg(d.delay_days + greatest(d.delay_days, 0)) AS avg_delivery_days,
    avg(if(d.delay_days <= 0, 1, 0))        AS on_time_rate,
    avg(rt.rating)                          AS rating,
    sumIf(i.amount - i.paid_amount, i.status = 'overdue') AS invoice_overdue_amt,
    now()                                   AS _etl_loaded_at
FROM vipaniv2.fact_po_raw FINAL p
LEFT JOIN vipaniv2.fact_delivery_raw FINAL d ON d.po_id = p.id
LEFT JOIN vipaniv2.fact_invoice_raw FINAL i  ON i.po_id = p.id
LEFT JOIN vipaniv2.fact_rating_raw FINAL rt
       ON rt.rater_org_id = p.buyer_org_id
      AND rt.rated_org_id = p.supplier_org_id
      AND toDate(rt.rated_at) = toDate(p.po_date)
WHERE p._peerdb_is_deleted = 0
  AND p.is_deleted = 0
  AND toDate(p.po_date) >= today() - 90
GROUP BY metric_date, buyer_org_id, supplier_org_id
""",

    # ── agg_daily_rfq_pipeline ───────────────────────────────────────────────
    "agg_daily_rfq_pipeline": """
INSERT INTO vipaniv2.agg_daily_rfq_pipeline
SELECT
    toDate(r.rfq_date)                      AS metric_date,
    r.buyer_org_id                          AS org_id,
    countIf(r.status NOT IN (14,12,3))      AS rfq_open,
    countIf(r.status IN (12,3))             AS rfq_closed,
    countIf(r.status = 14)                  AS rfq_awarded,
    avg(dateDiff('hour', r.rfq_date, q.quoted_at)) AS avg_response_hours,
    avg(r.quote_count)                      AS avg_quotes_per_rfq,
    sum(r.total_budget)                     AS pipeline_value,
    now()                                   AS _etl_loaded_at
FROM vipaniv2.fact_rfq_raw FINAL r
LEFT JOIN vipaniv2.fact_rfq_quote_raw FINAL q ON q.rfq_id = r.id
WHERE r._peerdb_is_deleted = 0
  AND toDate(r.rfq_date) >= today() - 90
GROUP BY metric_date, org_id
""",

    # ── agg_daily_revenue_territory ──────────────────────────────────────────
    "agg_daily_revenue_territory": """
INSERT INTO vipaniv2.agg_daily_revenue_territory
SELECT
    toDate(p.po_date)       AS metric_date,
    p.supplier_org_id       AS org_id,
    o.state,
    o.city_name             AS city,
    sum(p.total_amount)     AS revenue,
    count()                 AS order_count,
    avg(p.total_amount)     AS avg_order_value,
    now()                   AS _etl_loaded_at
FROM vipaniv2.fact_po_raw FINAL p
JOIN vipaniv2.dim_organization_raw FINAL o ON o.id = p.buyer_org_id
WHERE p._peerdb_is_deleted = 0
  AND p.is_deleted = 0
  AND toDate(p.po_date) >= today() - 90
GROUP BY metric_date, org_id, state, city
""",

    # ── agg_daily_engagement ─────────────────────────────────────────────────
    "agg_daily_engagement": """
INSERT INTO vipaniv2.agg_daily_engagement
SELECT
    toDate(event_ts)            AS metric_date,
    org_id,
    uniq(user_id)               AS active_users,
    uniq(session_id)            AS sessions,
    avg(duration_secs)          AS avg_session_secs,
    count()                     AS page_views,
    topK(1)(event_type)[1]      AS top_event_type,
    now()                       AS _etl_loaded_at
FROM vipaniv2.fact_engagement_raw FINAL
WHERE _peerdb_is_deleted = 0
  AND toDate(event_ts) >= today() - 90
GROUP BY metric_date, org_id
""",
}

SNAPSHOT_QUERIES: dict[str, str] = {

    # ── snapshot_org_daily ───────────────────────────────────────────────────
    "snapshot_org_daily": """
INSERT INTO vipaniv2.snapshot_org_daily
SELECT
    today()                                              AS snapshot_date,
    p.buyer_org_id                                       AS org_id,
    countIf(p.status IN (1,2,4))                         AS open_po_count,
    sumIf(p.total_amount, p.status IN (1,2,4))           AS open_po_value,
    countIf(r.status NOT IN (3,6,12,14))                 AS open_rfq_count,
    countIf(i.status NOT IN ('paid','cancelled'))        AS pending_invoices,
    sumIf(i.amount - i.paid_amount,
          i.status NOT IN ('paid','cancelled'))          AS pending_invoice_amt,
    sum(im.quantity * im.unit_cost)                      AS inventory_value,
    now()                                                AS _etl_loaded_at
FROM vipaniv2.fact_po_raw FINAL p
LEFT JOIN vipaniv2.fact_rfq_raw FINAL r
       ON r.buyer_org_id = p.buyer_org_id
LEFT JOIN vipaniv2.fact_invoice_raw FINAL i
       ON i.po_id = p.id
LEFT JOIN vipaniv2.fact_inventory_movement_raw FINAL im
       ON im.org_id = p.buyer_org_id
WHERE p._peerdb_is_deleted = 0
  AND p.is_deleted = 0
GROUP BY org_id
""",

    # ── snapshot_inventory_sku_daily ─────────────────────────────────────────
    "snapshot_inventory_sku_daily": """
INSERT INTO vipaniv2.snapshot_inventory_sku_daily
SELECT
    today()             AS snapshot_date,
    org_id,
    warehouse_id,
    product_id,
    sumIf(quantity, movement_type = 'inbound')  -
    sumIf(quantity, movement_type = 'outbound') AS qty_on_hand,
    sumIf(quantity, movement_type = 'reserved') AS qty_reserved,
    greatest(
        sumIf(quantity, movement_type = 'inbound') -
        sumIf(quantity, movement_type = 'outbound') -
        sumIf(quantity, movement_type = 'reserved'),
        0
    )                                           AS qty_available,
    avg(unit_cost)                              AS unit_cost,
    (
        sumIf(quantity, movement_type = 'inbound') -
        sumIf(quantity, movement_type = 'outbound')
    ) * avg(unit_cost)                          AS total_value,
    now()                                       AS _etl_loaded_at
FROM vipaniv2.fact_inventory_movement_raw FINAL
WHERE _peerdb_is_deleted = 0
GROUP BY org_id, warehouse_id, product_id
""",
}


# =============================================================================
# Refresh runner
# =============================================================================

_last_refresh: dict[str, datetime] = {}


def _run_query(client: clickhouse_connect.driver.Client, name: str, sql: str) -> None:
    """Drop today's partition and re-insert for idempotency, then run."""
    try:
        # Drop today's partition in the target table before re-inserting
        # (avoids duplicate rows from repeated runs)
        partition_key = datetime.now(timezone.utc).strftime("%Y%m")
        try:
            client.command(
                f"ALTER TABLE vipaniv2.{name} DROP PARTITION '{partition_key}'"
            )
        except Exception:
            pass  # partition may not exist yet on first run

        client.command(sql)
        _last_refresh[name] = datetime.now(timezone.utc)
        log.info("  ✓ Refreshed: %s", name)
    except Exception as exc:
        log.error("  ✗ Failed: %s — %s", name, exc)


def run_agg_group(client: clickhouse_connect.driver.Client) -> None:
    log.info("[agg_group] Running %d aggregation queries…", len(AGG_QUERIES))
    for name, sql in AGG_QUERIES.items():
        _run_query(client, name, sql)


def run_snapshot_group(client: clickhouse_connect.driver.Client) -> None:
    log.info("[snapshot_group] Running %d snapshot queries…", len(SNAPSHOT_QUERIES))
    for name, sql in SNAPSHOT_QUERIES.items():
        _run_query(client, name, sql)


def print_status() -> None:
    print(f"\n{'Table':<45} {'Last Refresh'}")
    print("─" * 75)
    all_tables = list(AGG_QUERIES) + list(SNAPSHOT_QUERIES)
    for name in all_tables:
        ts = _last_refresh.get(name)
        ts_str = ts.strftime("%Y-%m-%d %H:%M:%S UTC") if ts else "never"
        print(f"  {name:<43} {ts_str}")
    print()


# =============================================================================
# Scheduler
# =============================================================================

def main() -> None:
    ap = argparse.ArgumentParser(description="Post-CDC aggregation refresh scheduler")
    ap.add_argument("--once",  action="store_true", help="Single pass then exit")
    ap.add_argument("--group", choices=["agg", "snapshot", "all"], default="all")
    ap.add_argument("--status", action="store_true", help="Print refresh status")
    args = ap.parse_args()

    if args.status:
        print_status()
        return

    agg_interval  = int(os.getenv("AGG_INTERVAL",  "600"))
    snap_interval = int(os.getenv("SNAP_INTERVAL", "3600"))

    log.info("Agg refresh scheduler starting (agg=%ss, snapshot=%ss)",
             agg_interval, snap_interval)

    last_agg_run  = 0.0
    last_snap_run = 0.0

    try:
        client = _ch_client()
    except Exception as exc:
        log.error("Cannot connect to ClickHouse: %s", exc)
        sys.exit(1)

    while True:
        now = time.monotonic()

        if args.group in ("agg", "all") and (now - last_agg_run) >= agg_interval:
            run_agg_group(client)
            last_agg_run = time.monotonic()

        if args.group in ("snapshot", "all") and (now - last_snap_run) >= snap_interval:
            run_snapshot_group(client)
            last_snap_run = time.monotonic()

        if args.once:
            break

        time.sleep(30)  # check every 30s


if __name__ == "__main__":
    main()
