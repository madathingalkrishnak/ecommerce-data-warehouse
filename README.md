# E-Commerce Data Warehouse on AWS

A production-grade data warehouse solution for e-commerce analytics, built with AWS Redshift, Apache Airflow, AWS Glue, and Kinesis Data Firehose. Implements a star schema dimensional model with automated ETL pipelines, data quality checks, and BI-ready materialized views.

## Architecture

```
Data Sources → Kinesis Firehose → S3 (Data Lake) → Glue ETL → Redshift → QuickSight
                                                         ↑
                                              Apache Airflow (orchestration)
```

**Pipeline flow:** Raw events land in S3 via Firehose → Glue crawlers catalog the data → PySpark ETL jobs clean, deduplicate, and enrich → Transformed data loads into a Redshift star schema → Materialized views power BI dashboards → Airflow orchestrates everything on a daily schedule with data quality gates.

## Key Features

- **Dimensional modeling**: Star schema with 4 fact tables, 8 dimension tables, and 4 materialized views
- **Scalable ETL**: PySpark jobs on AWS Glue with deduplication, validation, and enrichment
- **Pipeline orchestration**: Airflow DAG with S3 sensors, parallel Glue jobs, and quality checks
- **Query optimization**: Distribution keys, sort keys, compression encodings, and WLM configuration
- **Data quality**: Row count validation, null key checks, revenue anomaly detection, duplicate detection
- **Realistic synthetic data**: 50K users, 5K products, 500K events/day with seasonality and realistic distributions
- **BI-ready**: 12 KPI queries covering revenue, funnels, product analytics, RFM segmentation, and cohort analysis

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Ingestion | AWS Kinesis Data Firehose |
| Storage | Amazon S3 (Parquet, partitioned by date) |
| Catalog | AWS Glue Data Catalog |
| ETL | AWS Glue (PySpark) |
| Warehouse | Amazon Redshift (RA3) |
| Orchestration | Apache Airflow (MWAA) |
| BI/Visualization | Amazon QuickSight / Tableau |
| Data Quality | Custom Redshift SQL checks in Airflow |

## Project Structure

```
ecommerce-data-warehouse/
├── airflow/
│   └── dags/
│       └── ecommerce_dw_dag.py          # Main orchestration DAG
├── config/
│   └── aws_config.yaml                   # AWS infrastructure configuration
├── dashboards/
│   └── kpi_queries.sql                   # 12 BI dashboard queries
├── data_generator/
│   ├── __init__.py
│   ├── config.py                         # Data generation configuration
│   ├── generators.py                     # User, product, event, order generators
│   └── generate_all.py                   # Main generation script
├── docs/
│   ├── architecture.md                   # System architecture & design decisions
│   ├── data_dictionary.md                # Complete schema documentation
│   └── setup_guide.md                    # Deployment instructions
├── glue_jobs/
│   ├── etl_clickstream.py                # Clickstream transformation job
│   ├── etl_orders.py                     # Orders transformation job
│   └── etl_load_facts.py                 # Fact table loading job
├── redshift/
│   ├── schemas/
│   │   ├── 01_create_schemas.sql         # Schema setup
│   │   ├── 02_staging_tables.sql         # Raw data landing tables
│   │   ├── 03_dimension_tables.sql       # Star schema dimensions
│   │   ├── 04_fact_tables.sql            # Star schema facts
│   │   ├── 05_populate_dimensions.sql    # Static dimension data
│   │   └── 06_load_dimensions_from_staging.sql  # SCD Type 1 upserts
│   └── optimizations/
│       ├── 01_materialized_views.sql     # Pre-computed analytics views
│       └── 02_optimization_strategies.sql # Performance tuning guide
├── scripts/
│   └── setup_s3.py                       # S3 data upload utility
├── tests/
├── requirements.txt
└── README.md
```

## Quick Start

```bash
# 1. Install dependencies
pip install faker numpy pandas pyarrow

# 2. Generate sample data (7 days, 1% scale)
python -c "
import sys; sys.argv = ['gen', '--days', '7', '--scale', '0.01', '--output', './sample_data']
sys.path.insert(0, '.'); from data_generator.generate_all import main; main()
"

# 3. Upload to S3
python scripts/setup_s3.py --data-dir ./sample_data --bucket YOUR_BUCKET --dry-run

# 4. Set up Redshift schema (see docs/setup_guide.md)
# 5. Deploy Airflow DAG (see docs/setup_guide.md)
```

## Data Model

### Star Schema

**Fact Tables:**
- `fact_clickstream` — One row per event (~500K/day). Measures: view duration, scroll depth, quantities
- `fact_orders` — One row per order (~8K/day). Measures: subtotal, discounts, shipping, tax, total
- `fact_order_items` — One row per line item (~20K/day). Measures: quantity, unit price, net amount
- `fact_daily_user_sessions` — Periodic snapshot aggregating user behavior per day

**Dimension Tables:**
- `dim_date` — Calendar dimension (2020-2030) with holidays
- `dim_time` — Every minute of the day with time periods
- `dim_user` — Customer profiles with demographics and preferences
- `dim_product` — Product catalog with categories, pricing tiers, and ratings
- `dim_traffic_source` — Marketing channel attribution
- `dim_device` — Device type and browser combinations
- `dim_shipping` — Shipping method and tier
- `dim_payment` — Payment method and category

### Optimization Strategy

| Table | Distribution | Sort Key | Rationale |
|-------|-------------|----------|-----------|
| fact_clickstream | DISTKEY(user_key) | date_key, event_type, user_key | User-centric queries, date range scans |
| fact_orders | DISTKEY(user_key) | date_key, user_key, status | Collocated with clickstream for cross-fact joins |
| dim_user | DISTSTYLE ALL | user_id | Small table, replicated for zero-shuffle joins |
| dim_product | DISTSTYLE ALL | product_id | Small table, replicated |

## Dashboard KPIs

The `dashboards/kpi_queries.sql` file contains queries for:

1. **Revenue Overview** — Current vs previous month with YoY comparison
2. **Daily Revenue Trend** — 90-day trend with 7-day moving average
3. **Revenue by Category** — Category/subcategory breakdown with discount rates
4. **Conversion Funnel** — Visit → View → Cart → Checkout → Purchase rates
5. **Funnel by Segment** — Conversion rates by membership tier and demographics
6. **Traffic Sources** — Channel performance with bounce rates
7. **Device Breakdown** — Session share by device type
8. **Top Products** — Revenue, units sold, and view-to-purchase conversion
9. **Category Heatmap** — Revenue by category across months
10. **Cohort Analysis** — Customer retention by registration cohort
11. **RFM Segmentation** — Champions, Loyal, At Risk, Hibernating
12. **Geographic Distribution** — Revenue and customers by state

## License

MIT
