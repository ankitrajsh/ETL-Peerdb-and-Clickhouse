# PeerDB ETL Pipeline for Analytics Dashboard

> **PostgreSQL → ClickHouse** real-time CDC pipeline powered by **PeerDB**, replacing the Airflow batch ETL with sub-second replication latency.

---

## Table of Contents

1. [Overview & Comparison](#1-overview--comparison)
2. [Architecture](#2-architecture)
3. [Repository Structure](#3-repository-structure)
4. [Prerequisites](#4-prerequisites)
5. [Setup Guide](#5-setup-guide)
   - [5.1 Environment Variables](#51-environment-variables)
   - [5.2 PostgreSQL Replication Setup](#52-postgresql-replication-setup)
   - [5.3 Start Docker Services](#53-start-docker-services)
   - [5.4 Apply Schema & Create Mirrors](#54-apply-schema--create-mirrors)
6. [Mirror Reference](#6-mirror-reference)
7. [Aggregation Refresh](#7-aggregation-refresh)
8. [Operations](#8-operations)
9. [Troubleshooting](#9-troubleshooting)
10. [Comparison: PeerDB vs Airflow Batch](#10-comparison-peerdb-vs-airflow-batch)

---

## 1. Overview & Comparison

| Feature | Airflow Batch ETL | PeerDB CDC (this pipeline) |
|---|---|---|
| **Replication lag** | Up to 24 hours (nightly) | < 1 second (WAL streaming) |
| **Initial load** | Manual backfill script | Automatic snapshot on mirror creation |
| **DELETE handling** | Not handled | Native via `_peerdb_is_deleted` flag |
| **UPDATE handling** | Full table reload | ReplacingMergeTree deduplication |
| **Failure recovery** | Re-run DAG task | Watermark-based automatic retry |
| **Orchestration** | Apache Airflow 2.9 | PeerDB + Temporal |
| **Dashboard lag** | T+1 day | Near real-time |
| **Aggregations** | Part of ETL DAG | `agg_refresh.py` (every 10 min) |

---

## 2. Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│  PostgreSQL  (vipani_devpy_final_stg)                                   │
│                                                                         │
│  WAL Logical Replication                                                │
│  Publications:                                                          │
│    vipani_dims_pub   → userApis_organization, userApis_user,            │
│                        categories, master_products, userApis_warehouse  │
│    vipani_facts_pub  → po_details, po_item, bt_rfqs, bt_rfq_quotes,     │
│                        purchase_request_form, po_delivery, po_invoice,  │
│                        bt_contracts, inventory_stock, bt_ratings,       │
│                        user_activity_logs                               │
└───────────────────────────┬─────────────────────────────────────────────┘
                            │ WAL stream (pg_logical)
                            ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  PeerDB Server  (Docker — ghcr.io/peerdb-io/peerdb:stable)              │
│                                                                         │
│  Mirrors:                                                               │
│    vipani_dims_cdc    ── initial snapshot + continuous CDC              │
│    vipani_facts_cdc   ── initial snapshot + continuous CDC              │
│                                                                         │
│  Workflow engine: Temporal (embedded)                                   │
│  UI: http://localhost:3000   API: http://localhost:8085                 │
└───────────────────────────┬─────────────────────────────────────────────┘
                            │ batched micro-inserts (ReplacingMergeTree)
                            ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  ClickHouse  (vipaniv2 database)                                        │
│                                                                         │
│  Raw CDC landing tables (*_raw):                                        │
│    dim_organization_raw, dim_user_raw, dim_category_raw,               │
│    dim_product_raw, dim_warehouse_raw                                   │
│    fact_po_raw, fact_po_item_raw, fact_rfq_raw, fact_rfq_quote_raw,    │
│    fact_prf_raw, fact_delivery_raw, fact_invoice_raw,                  │
│    fact_contract_raw, fact_inventory_movement_raw,                     │
│    fact_rating_raw, fact_engagement_raw                                 │
│                                                                         │
│  Clean views (FINAL dedup):                                             │
│    dim_organization, dim_user, dim_category, dim_product,              │
│    dim_warehouse, fact_po, fact_rfq, …                                 │
│                                                                         │
│  Aggregation tables (rebuilt every 10 min by agg_refresh.py):          │
│    agg_daily_buyer, agg_daily_seller, agg_daily_category_spend,        │
│    agg_daily_supplier_spend, agg_daily_rfq_pipeline,                   │
│    agg_daily_revenue_territory, agg_daily_engagement                   │
│                                                                         │
│  Snapshots (rebuilt every 60 min):                                     │
│    snapshot_org_daily, snapshot_inventory_sku_daily                    │
└─────────────────────────────────────────────────────────────────────────┘
                            │
                            ▼
              Analytics Dashboards (Buyer B1–B6, Seller S1–S5)
```

---

## 3. Repository Structure

```
peerdb_etl/
├── docker-compose.yml          # PeerDB + Temporal + ClickHouse stack
├── .env.example                # Environment variable template
├── requirements.txt            # Python dependencies
│
├── config/
│   ├── clickhouse_config.xml   # ClickHouse server config
│   └── temporal_dynamic.yaml  # Temporal workflow limits
│
├── mirrors/
│   ├── peers.yaml              # PostgreSQL & ClickHouse peer definitions
│   ├── dims_cdc.yaml           # CDC mirror: dimension tables
│   └── facts_cdc.yaml          # CDC mirror: fact/transactional tables
│
├── sql/
│   ├── clickhouse_cdc_schema.sql   # Raw CDC landing tables + clean views
│   └── clickhouse_agg_schema.sql   # Aggregation & snapshot tables
│
├── scripts/
│   ├── setup.sh                # One-shot full setup script
│   ├── monitor.sh              # Mirror status + row count checker
│   └── pg_replication_setup.sql # PostgreSQL WAL + publication setup
│
└── python/
    ├── peerdb_manager.py       # Mirror CRUD via PeerDB REST API
    └── agg_refresh.py          # Post-CDC aggregation scheduler
```

---

## 4. Prerequisites

| Requirement | Version | Notes |
|---|---|---|
| Docker + Docker Compose | v24+ | For PeerDB, Temporal, ClickHouse |
| Python | 3.10+ | For `peerdb_manager.py` + `agg_refresh.py` |
| PostgreSQL | 14+ | Source DB — must have `wal_level=logical` |
| psql client | any | For running `pg_replication_setup.sql` |

**PostgreSQL WAL requirement:**
```sql
-- Check current wal_level:
SHOW wal_level;   -- must return 'logical'

-- If not 'logical', add to postgresql.conf:
--   wal_level = logical
-- then restart PostgreSQL.
```

---

## 5. Setup Guide

### 5.1 Environment Variables

```bash
cd peerdb_etl
cp .env.example .env
nano .env   # fill in PG_PASSWORD, CH_PASSWORD, etc.
```

### 5.2 PostgreSQL Replication Setup

Run as PostgreSQL superuser **once**:

```bash
source .env
PGPASSWORD="$PG_PASSWORD" psql \
    -h "$PG_HOST" -p "$PG_PORT" \
    -U "$PG_USER" -d "$PG_DATABASE" \
    -f scripts/pg_replication_setup.sql
```

This creates:
- `peerdb_replicator` role with `REPLICATION` privilege
- `vipani_dims_pub` and `vipani_facts_pub` publications
- SELECT grants on all source tables

### 5.3 Start Docker Services

```bash
docker compose up -d --wait
docker compose logs -f peerdb   # watch startup
```

### 5.4 Apply Schema & Create Mirrors

**Option A — Automated (recommended):**
```bash
chmod +x scripts/setup.sh
./scripts/setup.sh
```

**Option B — Manual step-by-step:**
```bash
# Install Python deps
pip install -r requirements.txt

# Apply ClickHouse schema
docker exec -i peerdb_clickhouse clickhouse-client \
    --multiquery < sql/clickhouse_cdc_schema.sql
docker exec -i peerdb_clickhouse clickhouse-client \
    --multiquery < sql/clickhouse_agg_schema.sql

# Register peers
python python/peerdb_manager.py apply mirrors/peers.yaml

# Validate connections
python python/peerdb_manager.py validate

# Create CDC mirrors (triggers initial snapshot automatically)
python python/peerdb_manager.py apply mirrors/dims_cdc.yaml
python python/peerdb_manager.py apply mirrors/facts_cdc.yaml

# Start aggregation scheduler
python python/agg_refresh.py &
```

---

## 6. Mirror Reference

| Mirror | Tables | Mode | Snapshot | Batch Size |
|---|---|---|---|---|
| `vipani_dims_cdc` | 5 dimension tables | CDC | Yes | 1,000 rows |
| `vipani_facts_cdc` | 11 fact tables | CDC | Yes | 500 rows |

**Mirror lifecycle:**
```bash
# Check status
python python/peerdb_manager.py status

# Pause for maintenance
python python/peerdb_manager.py pause vipani_facts_cdc

# Resume
python python/peerdb_manager.py resume vipani_facts_cdc

# Drop (irreversible — will prompt for confirmation)
python python/peerdb_manager.py drop vipani_dims_cdc
```

---

## 7. Aggregation Refresh

The `agg_refresh.py` scheduler runs independently of PeerDB and queries the raw CDC tables:

```bash
# Start continuous scheduler
python python/agg_refresh.py

# One-shot refresh (useful for testing / CI)
python python/agg_refresh.py --once

# Only aggregation tables (not snapshots)
python python/agg_refresh.py --group agg

# Check last-refresh timestamps
python python/agg_refresh.py --status
```

| Group | Tables | Default Interval |
|---|---|---|
| `agg` | `agg_daily_buyer`, `agg_daily_seller`, `agg_daily_category_spend`, `agg_daily_supplier_spend`, `agg_daily_rfq_pipeline`, `agg_daily_revenue_territory`, `agg_daily_engagement` | 10 min |
| `snapshot` | `snapshot_org_daily`, `snapshot_inventory_sku_daily` | 60 min |

Override intervals via env vars: `AGG_INTERVAL=300 SNAP_INTERVAL=1800 python python/agg_refresh.py`

---

## 8. Operations

### Monitor everything
```bash
./scripts/monitor.sh             # full report (mirrors + row counts + PG slots)
./scripts/monitor.sh --counts    # ClickHouse row counts only
./scripts/monitor.sh --lag       # replication lag only
```

### Open PeerDB UI
```
http://localhost:3000
```

### Query ClickHouse directly
```bash
# HTTP play UI
http://localhost:8123/play

# CLI
docker exec -it peerdb_clickhouse clickhouse-client
```

### Check CDC lag
```sql
-- In PostgreSQL — WAL bytes behind per slot
SELECT slot_name, active,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag
FROM pg_replication_slots;
```

### Stop everything
```bash
docker compose down        # keep volumes (CDC state preserved)
docker compose down -v     # wipe all volumes (full reset)
```

---

## 9. Troubleshooting

| Problem | Fix |
|---|---|
| `wal_level is not logical` | Set `wal_level=logical` in `postgresql.conf`, restart PG |
| `peerdb_replicator` can't connect | Check `pg_hba.conf` — add `host replication peerdb_replicator <ip>/32 md5` |
| PeerDB mirror stuck in `snapshot` | Check `docker compose logs peerdb` for errors; verify SELECT grants |
| ClickHouse `_raw` table empty | Mirror may still be in initial snapshot phase — check PeerDB UI |
| Duplicate rows in agg tables | `agg_refresh.py` drops today's partition before re-inserting — run `--once` |
| `publication "vipani_dims_pub" already exists` | Normal on re-run of `pg_replication_setup.sql` — the DO block skips it |
| Docker out of memory | ClickHouse needs ≥ 4 GB RAM; reduce `snapshot_num_tables_in_parallel` |

---

## 10. Comparison: PeerDB vs Airflow Batch

```
Airflow Batch (existing):                PeerDB CDC (this pipeline):
─────────────────────────                ───────────────────────────
Nightly @20:00 UTC                       Continuous streaming
24-hour data lag                         < 1 second lag
Full table scans                         WAL tail only
DROP PARTITION + re-insert               ReplacingMergeTree upsert
Manual backfill scripts                  Automatic initial snapshot
Airflow DAG + connections                PeerDB mirrors + Temporal
2 workers + scheduler                    Single PeerDB container
```

Both pipelines write to the **same ClickHouse tables** — you can run them in parallel (PeerDB for CDC, Airflow for historical backfill) and decommission Airflow once PeerDB's initial snapshot is complete.
