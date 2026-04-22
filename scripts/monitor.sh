#!/usr/bin/env bash
# =============================================================================
# monitor.sh — Check PeerDB mirror status and ClickHouse row counts
# =============================================================================
# Usage:
#   ./scripts/monitor.sh
#   ./scripts/monitor.sh --lag     # show replication lag only
#   ./scripts/monitor.sh --counts  # show CH row counts only
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

if [[ -f "$PROJECT_DIR/.env" ]]; then
    set -a; source "$PROJECT_DIR/.env"; set +a
fi

MODE="${1:-all}"

echo "════════════════════════════════════════════"
echo " PeerDB ETL Pipeline — Monitor"
echo " $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo "════════════════════════════════════════════"

# ── Mirror status via PeerDB API ──────────────────────────────────────────────
if [[ "$MODE" == "all" || "$MODE" == "--lag" ]]; then
    echo ""
    echo "[ Mirror Status ]"
    python "$PROJECT_DIR/python/peerdb_manager.py" status 2>/dev/null || \
        echo "  PeerDB server unreachable — is Docker running?"
fi

# ── ClickHouse row counts ─────────────────────────────────────────────────────
if [[ "$MODE" == "all" || "$MODE" == "--counts" ]]; then
    echo ""
    echo "[ ClickHouse Row Counts ]"
    CH_USER="${CH_USER:-default}"
    CH_PASS="${CH_PASSWORD:-}"
    CH_DB="${CH_DATABASE:-vipaniv2}"

    TABLES=(
        "dim_organization_raw"
        "dim_user_raw"
        "dim_category_raw"
        "dim_product_raw"
        "dim_warehouse_raw"
        "fact_po_raw"
        "fact_po_item_raw"
        "fact_rfq_raw"
        "fact_rfq_quote_raw"
        "fact_delivery_raw"
        "fact_invoice_raw"
        "fact_contract_raw"
        "fact_inventory_movement_raw"
        "fact_rating_raw"
        "fact_engagement_raw"
        "agg_daily_buyer"
        "agg_daily_seller"
        "agg_daily_category_spend"
        "agg_daily_supplier_spend"
        "agg_daily_rfq_pipeline"
        "agg_daily_revenue_territory"
        "agg_daily_engagement"
        "snapshot_org_daily"
        "snapshot_inventory_sku_daily"
    )

    printf "  %-40s %12s\n" "Table" "Rows"
    printf "  %-40s %12s\n" "─────────────────────────────────────────" "────────────"
    for tbl in "${TABLES[@]}"; do
        CNT=$(docker exec peerdb_clickhouse clickhouse-client \
            --user "$CH_USER" --password "$CH_PASS" \
            --query "SELECT formatReadableQuantity(count()) FROM ${CH_DB}.${tbl}" 2>/dev/null || echo "error")
        printf "  %-40s %12s\n" "$tbl" "$CNT"
    done
fi

# ── PG replication slots ───────────────────────────────────────────────────────
if [[ "$MODE" == "all" ]]; then
    echo ""
    echo "[ PostgreSQL Replication Slots ]"
    PGPASSWORD="${PG_PASSWORD}" psql \
        -h "${PG_HOST}" -p "${PG_PORT}" \
        -U "${PG_USER}" -d "${PG_DATABASE}" \
        -c "SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag FROM pg_replication_slots;" \
        2>/dev/null || echo "  Could not connect to PostgreSQL"
fi

echo ""
echo "════════════════════════════════════════════"
