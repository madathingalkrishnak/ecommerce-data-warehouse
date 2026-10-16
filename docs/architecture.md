# Architecture Documentation

## System Architecture

```
                    ┌─────────────────────────────────────────────────────────┐
                    │                   DATA SOURCES                         │
                    │  Web App  │  Mobile App  │  Backend APIs  │  Batch     │
                    └─────┬─────┴──────┬───────┴───────┬────────┴─────┬──────┘
                          │            │               │              │
                    ┌─────▼────────────▼───────────────▼──────────────▼──────┐
                    │           AWS KINESIS DATA FIREHOSE                     │
                    │  ┌─────────────────┐  ┌─────────────────────┐          │
                    │  │ Clickstream     │  │ Orders Stream       │          │
                    │  │ Stream          │  │                     │          │
                    │  └────────┬────────┘  └──────────┬──────────┘          │
                    └───────────┼───────────────────────┼────────────────────┘
                               │                       │
                    ┌──────────▼───────────────────────▼────────────────────┐
                    │              AMAZON S3 (DATA LAKE)                     │
                    │                                                       │
                    │  raw/                    processed/                    │
                    │  ├── clickstream/         ├── clickstream/             │
                    │  │   └── year=.../        │   └── year=.../           │
                    │  │       └── month=../    │       └── month=../       │
                    │  │           └── day=../  │           └── day=../     │
                    │  ├── orders/              ├── orders/                  │
                    │  ├── order_line_items/    └── order_line_items/        │
                    │  └── dimensions/                                      │
                    │      ├── users/                                       │
                    │      └── products/                                    │
                    └──────────────────────┬────────────────────────────────┘
                                          │
                    ┌─────────────────────▼─────────────────────────────────┐
                    │              AWS GLUE (ETL)                            │
                    │                                                       │
                    │  ┌──────────────┐  ┌──────────────┐  ┌────────────┐  │
                    │  │ Crawler      │  │ ETL:         │  │ ETL:       │  │
                    │  │ (catalog)    │  │ Clickstream  │  │ Orders     │  │
                    │  └──────────────┘  └──────────────┘  └────────────┘  │
                    │                                                       │
                    │  ┌────────────────────────────────────────────────┐   │
                    │  │ ETL: Load Facts (staging → dimensional model)  │   │
                    │  └────────────────────────────────────────────────┘   │
                    └──────────────────────┬────────────────────────────────┘
                                          │
                    ┌─────────────────────▼─────────────────────────────────┐
                    │           AMAZON REDSHIFT (DATA WAREHOUSE)            │
                    │                                                       │
                    │  staging schema  →  dw schema  →  analytics schema   │
                    │  (raw landing)      (star schema)   (materialized     │
                    │                     Facts + Dims    views)            │
                    └──────────────────────┬────────────────────────────────┘
                                          │
                    ┌─────────────────────▼─────────────────────────────────┐
                    │         BI & REPORTING                                 │
                    │  ┌───────────────┐  ┌───────────┐  ┌──────────────┐  │
                    │  │ QuickSight    │  │ Tableau   │  │ Ad-hoc SQL   │  │
                    │  │ Dashboards    │  │           │  │ (Redshift    │  │
                    │  │               │  │           │  │  Query Editor)│  │
                    │  └───────────────┘  └───────────┘  └──────────────┘  │
                    └───────────────────────────────────────────────────────┘

                    ┌───────────────────────────────────────────────────────┐
                    │         APACHE AIRFLOW (MWAA) - ORCHESTRATION         │
                    │                                                       │
                    │  Daily DAG: S3 Sensor → Glue Crawler → ETL Jobs →    │
                    │  Load Dims → Load Facts → DQ Checks → Refresh MVs   │
                    └───────────────────────────────────────────────────────┘
```

## Data Flow

### 1. Data Ingestion (Real-time)
- **Kinesis Data Firehose** receives events from web/mobile/API sources
- Events buffered (64 MB or 5 minutes, whichever comes first)
- Compressed (GZIP) and written to S3 in partitioned layout
- Partition format: `year=YYYY/month=MM/day=DD/`

### 2. Data Storage (S3 Data Lake)
- **Raw zone**: Landing area for Firehose output (Parquet/JSON)
- **Processed zone**: Cleaned and enriched data from Glue ETL
- **Lifecycle policies**: Raw data 90 days, processed 365 days
- **Encryption**: S3-SSE with KMS key

### 3. Data Transformation (Glue ETL)
Three Glue jobs run in sequence:
1. **etl_clickstream**: Clean, dedupe, enrich clickstream → load staging
2. **etl_orders**: Validate orders + line items → load staging
3. **etl_load_facts**: Transform staging → dimensional model fact tables

### 4. Data Warehouse (Redshift)
Four-schema architecture:
- **staging**: Raw data landing (COPY from S3)
- **dw**: Star schema (facts + dimensions)
- **analytics**: Materialized views for BI
- **data_quality**: Audit logs and check results

### 5. Orchestration (Airflow/MWAA)
Single DAG runs daily at 3 AM UTC:
- S3 sensors wait for data availability
- Glue crawler updates data catalog
- ETL jobs run (clickstream || orders in parallel)
- Dimensions loaded, then fact tables
- Data quality checks validate results
- Materialized views refreshed

### 6. BI & Reporting (QuickSight)
Connects to Redshift `analytics` schema via JDBC/ODBC.
Pre-built dashboards: Executive, Funnel, Traffic, Product, Customer.

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Warehouse engine | Redshift RA3 | Managed storage, Spectrum for S3, columnar |
| Distribution | DISTKEY(user_key) | Most queries are user-centric |
| Sort keys | COMPOUND (date-first) | 90%+ queries filter by date range |
| Dimension loading | SCD Type 1 | Simple; full history not required |
| ETL engine | AWS Glue (PySpark) | Serverless, integrated with S3/Redshift |
| Orchestration | MWAA (managed Airflow) | Industry standard, rich operator library |
| Data format | Parquet | Columnar, compressed, schema evolution |
| Partitioning | Year/Month/Day | Matches daily pipeline cadence |

## Cost Estimation (Production)

| Service | Configuration | Est. Monthly Cost |
|---------|--------------|-------------------|
| Redshift | 2x ra3.xlplus | ~$650 |
| Glue ETL | ~3 DPU-hours/day | ~$130 |
| MWAA | mw1.medium | ~$350 |
| S3 | ~500 GB stored | ~$12 |
| Firehose | ~15 GB/day ingested | ~$15 |
| QuickSight | 5 author + 50 reader | ~$350 |
| **Total** | | **~$1,500/month** |
