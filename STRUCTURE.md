peerdb_etl/
├── .env.example
├── docker-compose.yml
├── requirements.txt
├── README.md
│
├── config/
│   ├── clickhouse_config.xml
│   └── temporal_dynamic.yaml
│
├── mirrors/
│   ├── peers.yaml
│   ├── dims_cdc.yaml
│   └── facts_cdc.yaml
│
├── sql/
│   ├── clickhouse_cdc_schema.sql
│   └── clickhouse_agg_schema.sql
│
├── scripts/
│   ├── setup.sh
│   ├── monitor.sh
│   └── pg_replication_setup.sql
│
└── python/
    ├── peerdb_manager.py
    └── agg_refresh.py
