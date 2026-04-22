-- =============================================================================
-- PostgreSQL: Replication User & Publication Setup for PeerDB
-- =============================================================================
-- Source: 34.93.93.62:5718  bluet_devpy_final_stg_replica
-- Run as superuser BEFORE starting PeerDB.
--
-- Usage:
--   source .env
--   PGPASSWORD="$PG_PASSWORD" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" \
--       -d bluet_devpy_final_stg_replica -f scripts/pg_replication_setup.sql
-- =============================================================================

\set PEERDB_REPL_PASSWORD 'change_me_peerdb_repl_password'

-- 1. Create dedicated replication user
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'peerdb_replicator') THEN
    CREATE ROLE peerdb_replicator
      WITH LOGIN
           REPLICATION
           PASSWORD :'PEERDB_REPL_PASSWORD';
  END IF;
END $$;

-- 2. Grant SELECT on all tables used by mirrors
GRANT SELECT ON TABLE
    -- dim tables
    public."userApis_organization",
    public."userApis_user",
    public.categories,
    public.master_products,
    public.vendor_products,
    public.user_address,
    public.organization_subscriptions,
    -- fact tables
    public.po_details,
    public.po_items,
    public.po_tax_invoice,
    public.po_asn,
    public.bt_rfq,
    public.bt_rfq_quotes,
    public.bt_rfq_products,
    public.bt_prf,
    public.bt_prf_products,
    public.contract_details,
    public.rating,
    public.ratings_review,
    public.quote_details,
    public.transactions,
    public.preferred_partners
TO peerdb_replicator;

-- 3. Schema usage
GRANT USAGE ON SCHEMA public TO peerdb_replicator;

-- 4. Check wal_level
DO $$
BEGIN
  IF current_setting('wal_level') <> 'logical' THEN
    RAISE NOTICE 'wal_level is %. Set wal_level=logical in postgresql.conf and restart.',
                 current_setting('wal_level');
  ELSE
    RAISE NOTICE 'wal_level=logical ✓';
  END IF;
END $$;

-- 5. Create publications
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'vipani_dims_pub') THEN
    CREATE PUBLICATION vipani_dims_pub FOR TABLE
        public."userApis_organization",
        public."userApis_user",
        public.categories,
        public.master_products,
        public.vendor_products,
        public.user_address,
        public.organization_subscriptions;
  END IF;

  IF NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'vipani_facts_pub') THEN
    CREATE PUBLICATION vipani_facts_pub FOR TABLE
        public.po_details,
        public.po_items,
        public.po_tax_invoice,
        public.po_asn,
        public.bt_rfq,
        public.bt_rfq_quotes,
        public.bt_rfq_products,
        public.bt_prf,
        public.bt_prf_products,
        public.contract_details,
        public.rating,
        public.ratings_review,
        public.quote_details,
        public.transactions,
        public.preferred_partners;
  END IF;
END $$;

-- 6. Verify
SELECT pubname, puballtables FROM pg_publication;
SELECT slot_name, slot_type, active FROM pg_replication_slots;

-- 1. Create dedicated replication user
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'peerdb_replicator') THEN
    CREATE ROLE peerdb_replicator
      WITH LOGIN
           REPLICATION
           PASSWORD :'PEERDB_REPL_PASSWORD';
  END IF;
END $$;

-- 2. Grant SELECT on all relevant source tables
GRANT SELECT ON TABLE
    public."userApis_organization",
    public."userApis_user",
    public."userApis_warehouse",
    public.categories,
    public.master_products,
    public.po_details,
    public.po_item,
    public.po_delivery,
    public.po_invoice,
    public.bt_rfqs,
    public.bt_rfq_quotes,
    public.purchase_request_form,
    public.bt_contracts,
    public.inventory_stock,
    public.bt_ratings,
    public.user_activity_logs
TO peerdb_replicator;

-- 3. Allow peerdb_replicator to use the public schema
GRANT USAGE ON SCHEMA public TO peerdb_replicator;

-- 4. Set wal_level = logical (requires superuser + server restart)
--    Only run if not already set:
DO $$
BEGIN
  IF current_setting('wal_level') <> 'logical' THEN
    RAISE NOTICE 'wal_level is %. You must set wal_level=logical in postgresql.conf and restart.',
                 current_setting('wal_level');
  ELSE
    RAISE NOTICE 'wal_level=logical ✓';
  END IF;
END $$;

-- 5. Create publications (PeerDB will use these)
--    One publication per mirror group (dims + facts)
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'vipani_dims_pub') THEN
    CREATE PUBLICATION vipani_dims_pub FOR TABLE
        public."userApis_organization",
        public."userApis_user",
        public."userApis_warehouse",
        public.categories,
        public.master_products;
  END IF;

  IF NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'vipani_facts_pub') THEN
    CREATE PUBLICATION vipani_facts_pub FOR TABLE
        public.po_details,
        public.po_item,
        public.po_delivery,
        public.po_invoice,
        public.bt_rfqs,
        public.bt_rfq_quotes,
        public.purchase_request_form,
        public.bt_contracts,
        public.inventory_stock,
        public.bt_ratings,
        public.user_activity_logs;
  END IF;
END $$;

-- 6. Verify
SELECT pubname, puballtables FROM pg_publication;
SELECT slot_name, slot_type, active FROM pg_replication_slots;
