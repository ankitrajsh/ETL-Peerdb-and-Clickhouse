#!/usr/bin/env bash
# =============================================================================
# setup.sh — One-shot setup for the PeerDB ETL pipeline
# =============================================================================
# Run this script once on first deployment:
#   chmod +x scripts/setup.sh
#   ./scripts/setup.sh
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# ── Load .env ─────────────────────────────────────────────────────────────────
if [[ -f "$PROJECT_DIR/.env" ]]; then
    set -a
    source "$PROJECT_DIR/.env"
    set +a
    echo "[setup] Loaded .env"
else
    echo "[setup] ERROR: .env not found. Copy .env.example → .env and fill in values."
    exit 1
fi

# ── 1. Start Docker services ──────────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════"
echo " Step 1: Starting Docker services"
echo "══════════════════════════════════════════════"
cd "$PROJECT_DIR"
docker compose up -d --wait
echo "[setup] Docker services started."

# ── 2. Wait for ClickHouse ────────────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════"
echo " Step 2: Waiting for ClickHouse…"
echo "══════════════════════════════════════════════"
MAX_WAIT=60
ELAPSED=0
until docker exec peerdb_clickhouse wget -qO- http://localhost:8123/ping 2>/dev/null | grep -q "Ok"; do
    sleep 2
    ELAPSED=$((ELAPSED+2))
    if [[ $ELAPSED -ge $MAX_WAIT ]]; then
        echo "[setup] ERROR: ClickHouse did not become healthy within ${MAX_WAIT}s."
        exit 1
    fi
done
echo "[setup] ClickHouse is ready."

# ── 3. Apply ClickHouse schema ────────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════"
echo " Step 3: Applying ClickHouse CDC schema"
echo "══════════════════════════════════════════════"
docker exec -i peerdb_clickhouse clickhouse-client \
    --user "${CH_USER:-default}" \
    --password "${CH_PASSWORD:-}" \
    --multiquery < "$PROJECT_DIR/sql/clickhouse_cdc_schema.sql"
echo "[setup] CDC schema applied."

docker exec -i peerdb_clickhouse clickhouse-client \
    --user "${CH_USER:-default}" \
    --password "${CH_PASSWORD:-}" \
    --multiquery < "$PROJECT_DIR/sql/clickhouse_agg_schema.sql"
echo "[setup] Aggregation schema applied."

# ── 4. Set up PostgreSQL replication ──────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════"
echo " Step 4: PostgreSQL replication setup"
echo "══════════════════════════════════════════════"
PGPASSWORD="${PG_PASSWORD}" psql \
    -h "${PG_HOST}" -p "${PG_PORT}" \
    -U "${PG_USER}" -d "${PG_DATABASE}" \
    -f "$PROJECT_DIR/scripts/pg_replication_setup.sql"
echo "[setup] PostgreSQL replication configured."

# ── 5. Wait for PeerDB server ─────────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════"
echo " Step 5: Waiting for PeerDB server…"
echo "══════════════════════════════════════════════"
ELAPSED=0
until curl -sf "http://localhost:8085/health" >/dev/null 2>&1; do
    sleep 3
    ELAPSED=$((ELAPSED+3))
    if [[ $ELAPSED -ge $MAX_WAIT ]]; then
        echo "[setup] WARNING: PeerDB server not responding after ${MAX_WAIT}s — continuing anyway."
        break
    fi
done
echo "[setup] PeerDB server is ready."

# ── 6. Register peers + create mirrors ────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════"
echo " Step 6: Registering peers & creating mirrors"
echo "══════════════════════════════════════════════"
cd "$PROJECT_DIR"
python python/peerdb_manager.py apply mirrors/peers.yaml
python python/peerdb_manager.py apply mirrors/dims_cdc.yaml
python python/peerdb_manager.py apply mirrors/facts_cdc.yaml

# ── 7. Install & start aggregation scheduler ──────────────────────────────────
echo ""
echo "══════════════════════════════════════════════"
echo " Step 7: Starting aggregation refresh scheduler"
echo "══════════════════════════════════════════════"
nohup python "$PROJECT_DIR/python/agg_refresh.py" \
    >> /tmp/peerdb_agg_refresh.log 2>&1 &
echo "[setup] Aggregation scheduler started (PID $!). Log: /tmp/peerdb_agg_refresh.log"

echo ""
echo "══════════════════════════════════════════════"
echo " ✅  PeerDB ETL pipeline is running!"
echo "══════════════════════════════════════════════"
echo ""
echo "  PeerDB UI  → http://localhost:3000"
echo "  PeerDB API → http://localhost:8085"
echo "  ClickHouse → http://localhost:8123 (play UI: http://localhost:8123/play)"
echo ""
echo "  Monitor mirrors:"
echo "    python python/peerdb_manager.py status"
echo ""
echo "  View agg logs:"
echo "    tail -f /tmp/peerdb_agg_refresh.log"
