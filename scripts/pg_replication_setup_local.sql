-- =============================================================================
-- PostgreSQL LOCAL: Replication User & Publication Setup for PeerDB
-- =============================================================================
-- Run as postgres superuser on local PG18:
--   sudo -u postgres psql -p 5432 -d vipani_devpy_final_stg \
--       -f scripts/pg_replication_setup_local.sql
-- =============================================================================

\set PEERDB_REPL_PASSWORD 'change_me_peerdb_repl_password'

-- 1. Allow peerdb_replicator to connect via TCP (Docker → host)
--    PeerDB Docker container connects as peerdb_replicator over TCP.
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'peerdb_replicator') THEN
    CREATE ROLE peerdb_replicator
      WITH LOGIN
           REPLICATION
           PASSWORD :'PEERDB_REPL_PASSWORD';
    RAISE NOTICE 'Created role peerdb_replicator';
  ELSE
    RAISE NOTICE 'Role peerdb_replicator already exists';
  END IF;
END $$;

-- 2. Grant SELECT on all CDC tables
GRANT SELECT ON TABLE
    public."userApis_organization",
    public."userApis_user",
    public.categories,
    public.master_products,
    public.vendor_products,
    public.user_address,
    public.organization_subscriptions,
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

GRANT USAGE ON SCHEMA public TO peerdb_replicator;

-- 3. Verify wal_level (must be logical)
DO $$
BEGIN
  IF current_setting('wal_level') = 'logical' THEN
    RAISE NOTICE 'wal_level = logical ✓';
  ELSE
    RAISE EXCEPTION 'wal_level is %. Run: ALTER SYSTEM SET wal_level = logical; then restart PG.',
                    current_setting('wal_level');
  END IF;
END $$;

-- 4. Allow Docker network to connect as peerdb_replicator
--    Add to pg_hba.conf: host vipani_devpy_final_stg peerdb_replicator 172.16.0.0/12 md5
--    Or use the dynamic approach below (requires reload)
DO $$
DECLARE
  hba_line text := 'host vipani_devpy_final_stg peerdb_replicator 172.16.0.0/12 md5';
  hba_line2 text := 'host replication peerdb_replicator 172.16.0.0/12 md5';
  hba_file text;
  hba_contents text;
BEGIN
  SELECT setting INTO hba_file FROM pg_settings WHERE name = 'hba_file';
  SELECT pg_read_file(hba_file) INTO hba_contents;

  IF position(hba_line IN hba_contents) = 0 THEN
    PERFORM pg_file_write(hba_file, chr(10) || hba_line || chr(10), true);
    RAISE NOTICE 'Added HBA rule: %', hba_line;
  ELSE
    RAISE NOTICE 'HBA rule already exists';
  END IF;

  IF position(hba_line2 IN hba_contents) = 0 THEN
    PERFORM pg_file_write(hba_file, chr(10) || hba_line2 || chr(10), true);
    RAISE NOTICE 'Added HBA replication rule: %', hba_line2;
  END IF;

  PERFORM pg_reload_conf();
  RAISE NOTICE 'pg_hba.conf reloaded';
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
    RAISE NOTICE 'Created vipani_dims_pub';
  ELSE
    RAISE NOTICE 'vipani_dims_pub already exists';
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
    RAISE NOTICE 'Created vipani_facts_pub';
  ELSE
    RAISE NOTICE 'vipani_facts_pub already exists';
  END IF;
END $$;

-- 6. Verify
SELECT pubname, puballtables FROM pg_publication ORDER BY pubname;
SELECT slot_name, slot_type, active FROM pg_replication_slots;
SHOW wal_level;
