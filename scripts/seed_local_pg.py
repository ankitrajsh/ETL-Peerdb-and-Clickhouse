#!/usr/bin/env python3
"""
seed_local_pg.py
================
Copies data from the remote production replica into the local PostgreSQL
for CDC pipeline testing without touching the remote server.

Usage:
    python peerdb_etl/scripts/seed_local_pg.py

What it does:
  1. Connects to remote PG (34.93.93.62:5718) and local PG (localhost:5432)
  2. Truncates local CDC tables (order respects FK dependencies)
  3. Streams rows via cursor in batches, inserts locally with FK checks disabled
  4. Resets sequences to match remote max IDs
"""

import psycopg2
import psycopg2.extras
import sys
import os

# ── Connection config ─────────────────────────────────────────────────────────
REMOTE = dict(host="34.93.93.62", port=5718, dbname="bluet_devpy_final_stg_replica",
              user="postgres", password=os.getenv("PG_PASSWORD", ""), connect_timeout=10)

LOCAL  = dict(host="localhost", port=5432, dbname="vipani_devpy_final_stg",
              user="postgres", connect_timeout=5)

# Tables in FK-safe insertion order (parents before children)
# Each entry: (schema.table, row_limit)
TABLES = [
    # --- dimension / master (no FK parents in this set) ---
    ('"userApis_organization"',   2000),
    ('"userApis_user"',           2000),
    ('categories',                2000),   # self-referencing FK — handled separately
    ('master_products',           5000),
    ('vendor_products',           5000),
    ('user_address',              2000),
    ('organization_subscriptions',2000),
    # --- facts (depend on org/user/product) ---
    ('po_details',                2000),
    ('po_items',                  5000),
    ('bt_rfq',                    2000),
    ('bt_rfq_products',           5000),
    ('bt_rfq_quotes',             2000),
    ('bt_prf',                    2000),
    ('bt_prf_products',           5000),
    ('po_tax_invoice',            2000),
    ('po_asn',                    2000),
    ('contract_details',          2000),
    ('quote_details',             2000),
    ('transactions',              2000),
    ('preferred_partners',        2000),
    ('rating',                    2000),
    ('ratings_review',            2000),
]

BATCH = 500   # rows per INSERT


def copy_table(remote_cur, local_cur, table: str, limit: int) -> int:
    """Stream rows from remote → local for one table. Returns row count."""
    remote_cur.execute(f'SELECT * FROM public.{table} LIMIT %s', (limit,))
    cols = [d[0] for d in remote_cur.description]
    col_list = ", ".join(f'"{c}"' for c in cols)
    placeholders = ", ".join(["%s"] * len(cols))
    insert_sql = f'INSERT INTO public.{table} ({col_list}) VALUES ({placeholders}) ON CONFLICT DO NOTHING'

    total = 0
    while True:
        rows = remote_cur.fetchmany(BATCH)
        if not rows:
            break
        psycopg2.extras.execute_batch(local_cur, insert_sql, rows, page_size=BATCH)
        total += len(rows)
        print(f"  {table}: {total} rows...", end="\r", flush=True)
    return total


def main():
    print("=== Vipani Local PG Seed Script ===\n")

    if not REMOTE["password"]:
        print("  ✗ Missing remote password. Set PG_PASSWORD in environment before running.")
        sys.exit(1)

    try:
        print("Connecting to remote PG…")
        remote = psycopg2.connect(**REMOTE)
        remote.set_session(readonly=True, autocommit=True)
        print("  ✓ Remote connected")
    except Exception as e:
        print(f"  ✗ Remote connection failed: {e}")
        sys.exit(1)

    try:
        print("Connecting to local PG…")
        local = psycopg2.connect(**LOCAL)
        local.autocommit = False
        print("  ✓ Local connected\n")
    except Exception as e:
        print(f"  ✗ Local connection failed: {e}\n"
              f"    Make sure PostgreSQL is running: sudo systemctl start postgresql")
        sys.exit(1)

    local_cur = local.cursor()
    remote_cur = remote.cursor(name="seed_cursor", cursor_factory=psycopg2.extras.DictCursor)

    try:
        # Disable FK checks for the session
        local_cur.execute("SET session_replication_role = replica;")

        # Truncate all tables in REVERSE order (children first)
        print("Truncating local tables…")
        for table, _ in reversed(TABLES):
            local_cur.execute(f'TRUNCATE TABLE public.{table} CASCADE')
            print(f"  TRUNCATE {table}")
        local.commit()
        print()

        # Copy data
        print("Copying data from remote → local…")
        totals = {}
        for table, limit in TABLES:
            remote_cur2 = remote.cursor(cursor_factory=psycopg2.extras.DictCursor)
            remote_cur2.execute(f'SELECT * FROM public.{table} LIMIT %s', (limit,))
            cols = [d[0] for d in remote_cur2.description]
            col_list = ", ".join(f'"{c}"' for c in cols)
            placeholders = ", ".join(["%s"] * len(cols))
            insert_sql = (f'INSERT INTO public.{table} ({col_list}) '
                          f'VALUES ({placeholders}) ON CONFLICT DO NOTHING')

            total = 0
            while True:
                rows = remote_cur2.fetchmany(BATCH)
                if not rows:
                    break
                psycopg2.extras.execute_batch(local_cur, insert_sql, [list(r) for r in rows], page_size=BATCH)
                total += len(rows)
                print(f"  {table:40s}: {total:6d} rows", end="\r", flush=True)
            remote_cur2.close()
            local.commit()
            totals[table] = total
            print(f"  {table:40s}: {total:6d} rows ✓")

        # Re-enable FK checks
        local_cur.execute("RESET session_replication_role;")
        local.commit()

        print("\n=== Summary ===")
        grand = 0
        for table, count in totals.items():
            print(f"  {table:40s}: {count:6d}")
            grand += count
        print(f"\n  Total rows loaded: {grand}")
        print("\n✅ Local DB seeded successfully!")
        print("\nNext steps:")
        print("  1. Update peerdb_etl/.env → PG_HOST=localhost PG_PORT=5432 PG_DATABASE=vipani_devpy_final_stg")
        print("  2. Run: cd peerdb_etl && ./scripts/setup_local.sh")

    except Exception as e:
        local.rollback()
        print(f"\n✗ Error: {e}")
        import traceback; traceback.print_exc()
        sys.exit(1)
    finally:
        local_cur.close()
        remote_cur.close()
        local.close()
        remote.close()


if __name__ == "__main__":
    main()
