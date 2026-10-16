# Deployment & Setup Guide

## Prerequisites

- AWS Account with admin access
- AWS CLI v2 configured (`aws configure`)
- Python 3.10+
- Docker (for local Airflow testing)

## Step 1: Generate Sample Data

```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/ecommerce-data-warehouse.git
cd ecommerce-data-warehouse

# Install dependencies
pip install -r requirements.txt

# Generate 30 days of sample data at 1% scale (for testing)
python -c "
import sys
sys.argv = ['gen', '--days', '30', '--scale', '0.01', '--output', './sample_data']
sys.path.insert(0, '.')
from data_generator.generate_all import main
main()
"

# For full-scale data (production volumes):
# python -m data_generator.generate_all --days 365 --scale 1.0 --output ./full_data
```

## Step 2: Set Up AWS Infrastructure

### 2a. Create S3 Buckets
```bash
# Set your AWS account ID
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export BUCKET_NAME="ecommerce-dw-data-lake-${AWS_ACCOUNT_ID}"

# Create the data lake bucket
aws s3 mb s3://${BUCKET_NAME} --region us-east-1

# Enable versioning
aws s3api put-bucket-versioning \
  --bucket ${BUCKET_NAME} \
  --versioning-configuration Status=Enabled

# Block public access
aws s3api put-public-access-block \
  --bucket ${BUCKET_NAME} \
  --public-access-block-configuration \
    BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
```

### 2b. Upload Data to S3
```bash
python scripts/setup_s3.py \
  --data-dir ./sample_data \
  --bucket ${BUCKET_NAME} \
  --prefix raw/
```

### 2c. Create Redshift Cluster
```bash
# Create subnet group (use your VPC subnets)
aws redshift create-cluster-subnet-group \
  --cluster-subnet-group-name ecommerce-dw-subnet-group \
  --description "Subnet group for e-commerce DW" \
  --subnet-ids subnet-xxxxx subnet-yyyyy

# Create the cluster
aws redshift create-cluster \
  --cluster-identifier ecommerce-dw-cluster \
  --node-type ra3.xlplus \
  --number-of-nodes 2 \
  --master-username dw_admin \
  --master-user-password 'YOUR_SECURE_PASSWORD' \
  --db-name ecommerce_dw \
  --port 5439 \
  --encrypted \
  --iam-roles "arn:aws:iam::${AWS_ACCOUNT_ID}:role/ecommerce-dw-redshift-role"
```

### 2d. Create Redshift Schema
```bash
# Connect to Redshift and run the SQL scripts in order:
# Use psql, DBeaver, or Redshift Query Editor v2

psql -h ecommerce-dw-cluster.xxxxx.us-east-1.redshift.amazonaws.com \
     -p 5439 -U dw_admin -d ecommerce_dw

# In psql, run:
\i redshift/schemas/01_create_schemas.sql
\i redshift/schemas/02_staging_tables.sql
\i redshift/schemas/03_dimension_tables.sql
\i redshift/schemas/04_fact_tables.sql
\i redshift/schemas/05_populate_dimensions.sql
```

### 2e. Set Up Glue
```bash
# Create Glue database
aws glue create-database \
  --database-input '{"Name": "ecommerce_catalog"}'

# Upload ETL scripts to S3
aws s3 cp glue_jobs/ s3://${BUCKET_NAME}-scripts/glue/ --recursive

# Create Glue Crawler
aws glue create-crawler \
  --name ecommerce-raw-data-crawler \
  --role ecommerce-dw-glue-role \
  --database-name ecommerce_catalog \
  --targets '{"S3Targets": [{"Path": "s3://'"${BUCKET_NAME}"'/raw/"}]}'

# Create Glue connection to Redshift
aws glue create-connection \
  --connection-input '{
    "Name": "ecommerce-redshift-connection",
    "ConnectionType": "JDBC",
    "ConnectionProperties": {
      "JDBC_CONNECTION_URL": "jdbc:redshift://ecommerce-dw-cluster.xxxxx:5439/ecommerce_dw",
      "USERNAME": "dw_admin",
      "PASSWORD": "YOUR_SECURE_PASSWORD"
    },
    "PhysicalConnectionRequirements": {
      "SubnetId": "subnet-xxxxx",
      "SecurityGroupIdList": ["sg-xxxxx"]
    }
  }'
```

### 2f. Set Up MWAA (Airflow)
```bash
# Upload DAG and requirements
aws s3 cp airflow/dags/ s3://${BUCKET_NAME}-scripts/airflow/dags/ --recursive
aws s3 cp requirements.txt s3://${BUCKET_NAME}-scripts/airflow/requirements.txt

# Create MWAA environment (via console recommended for VPC setup)
# Or use AWS CLI:
aws mwaa create-environment \
  --name ecommerce-dw-airflow \
  --airflow-version 2.8.1 \
  --source-bucket-arn "arn:aws:s3:::${BUCKET_NAME}-scripts" \
  --dag-s3-path airflow/dags \
  --requirements-s3-path airflow/requirements.txt \
  --execution-role-arn "arn:aws:iam::${AWS_ACCOUNT_ID}:role/ecommerce-dw-airflow-role" \
  --environment-class mw1.medium \
  --max-workers 5

# Set Airflow Variables (via MWAA UI or CLI)
# ecommerce_s3_bucket = ecommerce-dw-data-lake-XXXXX
# ecommerce_redshift_conn = redshift_default
# ecommerce_glue_conn = ecommerce-redshift-connection
```

## Step 3: Run the Pipeline

```bash
# Option A: Trigger DAG via Airflow UI
# Navigate to MWAA console → Open Airflow UI → Enable DAG → Trigger

# Option B: Trigger via CLI
aws mwaa create-cli-token --name ecommerce-dw-airflow
# Use the token to call Airflow REST API

# Option C: Run Glue jobs manually for testing
aws glue start-job-run \
  --job-name etl-clickstream-transform \
  --arguments '{
    "--S3_INPUT_PATH": "s3://BUCKET/raw/clickstream/",
    "--S3_OUTPUT_PATH": "s3://BUCKET/processed/clickstream/",
    "--PROCESSING_DATE": "2024-01-01",
    "--REDSHIFT_CONNECTION": "ecommerce-redshift-connection",
    "--REDSHIFT_DATABASE": "ecommerce_dw",
    "--REDSHIFT_IAM_ROLE": "arn:aws:iam::ACCOUNT:role/redshift-role"
  }'
```

## Step 4: Connect BI Tool

### QuickSight Setup
1. Go to AWS QuickSight console
2. Manage Data → New Data Set → Redshift (Manual connect)
3. Enter cluster endpoint, port 5439, database `ecommerce_dw`
4. Create datasets from `analytics.mv_*` materialized views
5. Build dashboards using the queries in `dashboards/kpi_queries.sql`

### Tableau Setup
1. Connect → Amazon Redshift
2. Server: cluster endpoint, Port: 5439
3. Database: ecommerce_dw, Schema: analytics
4. Import the materialized views as data sources

## Local Development

### Run Airflow Locally (Docker Compose)
```bash
# Start local Airflow
docker compose -f airflow/docker-compose.yaml up -d

# Access Airflow UI at http://localhost:8080
# Username: airflow, Password: airflow
```

### Run Tests
```bash
pytest tests/ -v --cov=data_generator --cov=glue_jobs
```
