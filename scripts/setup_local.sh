#!/usr/bin/env bash
# =============================================================================
# setup_local.sh — Start PeerDB pipeline using LOCAL PostgreSQL
# =============================================================================
# Use this instead of setup.sh when testing locally.
# Local PG: localhost:5432 / vipani_devpy_final_stg / postgres (peer auth)
# wal_level=logical is already set on local PG18.
#
# Run:
#   chmod +x scripts/setup_local.sh
#   ./scripts/setup_local.sh
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOCAL_ENV="$PROJECT_DIR/.env.local"

# ── Load .env.local ───────────────────────────────────────────────────────────
if [[ -f "$LOCAL_ENV" ]]; then
    set -a; source "$LOCAL_ENV"; set +a
    echo "[setup_local] Loaded .env.local"
else
    echo "[setup_local] ERROR: $LOCAL_ENV not found."
    exit 1
fi

# ── Step 1: Seed local DB ─────────────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════"
echo " Step 1: Seeding local PostgreSQL"
echo "══════════════════════════════════════════════"
python "$PROJECT_DIR/scripts/seed_local_pg.py"

# ── Step 2: PostgreSQL replication setup (local) ──────────────────────────────
echo ""
echo "══════════════════════════════════════════════"
echo " Step 2: Setting up replication on local PG"
echo "══════════════════════════════════════════════"
sudo -u postgres psql -p "${PG_PORT:-5432}" -d "${PG_DATABASE}" \
    -f "$PROJECT_DIR/scripts/pg_replication_setup_local.sql"
echo "[setup_local] Replication configured on local PG."

# ── Step 3: Start Docker services ─────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════"
echo " Step 3: Starting Docker services"
echo "══════════════════════════════════════════════"
cd "$PROJECT_DIR"
docker compose up -d --wait
echo "[setup_local] Docker services started."

# ── Step 4: Wait for ClickHouse ───────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════"
echo " Step 4: Waiting for ClickHouse…"
echo "══════════════════════════════════════════════"
for i in $(seq 1 30); do
    if docker exec peerdb_clickhouse wget -qO- http://localhost:8123/ping 2>/dev/null | grep -q "Ok"; then
        echo "[setup_local] ClickHouse ready."
        break
    fi
    echo "  waiting… ($i/30)"
    sleep 3
done

# ── Step 5: Apply ClickHouse schema ───────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════"
echo " Step 5: Applying ClickHouse schema"
echo "══════════════════════════════════════════════"
docker exec -i peerdb_clickhouse clickhouse-client \
    --user "${CH_USER:-default}" --password "${CH_PASSWORD:-}" \
    --multiquery < "$PROJECT_DIR/sql/clickhouse_cdc_schema.sql"
docker exec -i peerdb_clickhouse clickhouse-client \
    --user "${CH_USER:-default}" --password "${CH_PASSWORD:-}" \
    --multiquery < "$PROJECT_DIR/sql/clickhouse_agg_schema.sql"
echo "[setup_local] ClickHouse schema applied."

# ── Step 6: Wait for PeerDB ───────────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════"
echo " Step 6: Waiting for PeerDB API…"
echo "══════════════════════════════════════════════"
for i in $(seq 1 20); do
    if curl -sf "http://localhost:8085/health" >/dev/null 2>&1; then
        echo "[setup_local] PeerDB API ready."
        break
    fi
    echo "  waiting… ($i/20)"
    sleep 3
done

# ── Step 7: Register peers & mirrors ─────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════"
echo " Step 7: Registering peers & mirrors"
echo "══════════════════════════════════════════════"
python "$PROJECT_DIR/python/peerdb_manager.py" apply "$PROJECT_DIR/mirrors/peers_local.yaml"
python "$PROJECT_DIR/python/peerdb_manager.py" apply "$PROJECT_DIR/mirrors/dims_cdc.yaml"
python "$PROJECT_DIR/python/peerdb_manager.py" apply "$PROJECT_DIR/mirrors/facts_cdc.yaml"

# ── Step 8: Start agg scheduler ───────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════"
echo " Step 8: Starting aggregation scheduler"
echo "══════════════════════════════════════════════"
nohup python "$PROJECT_DIR/python/agg_refresh.py" \
    >> /tmp/peerdb_agg_refresh.log 2>&1 &
echo "[setup_local] Agg scheduler started (PID $!)"

echo ""
echo "══════════════════════════════════════════════"
echo " ✅  Local PeerDB pipeline is running!"
echo "══════════════════════════════════════════════"
echo ""
echo "  PeerDB UI      → http://localhost:3000"
echo "  PeerDB API     → http://localhost:8085"
echo "  ClickHouse     → http://localhost:8123"
echo "  Agg log        → tail -f /tmp/peerdb_agg_refresh.log"
echo ""
echo "  Mirror status  → python python/peerdb_manager.py status"
echo ""
echo "  Test CDC (insert a row in local PG, watch it appear in CH):"
echo "    sudo -u postgres psql -p 5432 -d vipani_devpy_final_stg -c \\"
echo "      \"INSERT INTO public.rating (id, ...) VALUES (99999, ...)\""
