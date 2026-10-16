"""
E-Commerce Data Warehouse - Main ETL Orchestration DAG
======================================================

Orchestrates the daily ETL pipeline:
1. Check for new data in S3
2. Run Glue crawlers to update catalog
3. Execute Glue ETL jobs (clickstream + orders in parallel)
4. Load dimensions into Redshift
5. Load fact tables into Redshift
6. Run data quality checks
7. Refresh materialized views
8. Notify on success/failure

Schedule: Daily at 3:00 AM UTC (after Kinesis Firehose midnight flush)
Catchup:  Enabled for backfill support
"""

from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule


# ============================================================
# Configuration
# ============================================================
S3_BUCKET = Variable.get("ecommerce_s3_bucket", default_var="ecommerce-dw-data-lake")
REDSHIFT_CONN_ID = Variable.get("ecommerce_redshift_conn", default_var="redshift_default")
GLUE_CONNECTION = Variable.get("ecommerce_glue_conn", default_var="ecommerce-redshift-connection")
AWS_CONN_ID = "aws_default"
REDSHIFT_IAM_ROLE = Variable.get("ecommerce_redshift_iam_role", default_var="arn:aws:iam::role/ecommerce-dw-redshift-role")
GLUE_IAM_ROLE = Variable.get("ecommerce_glue_iam_role", default_var="arn:aws:iam::role/ecommerce-dw-glue-role")


# ============================================================
# Default arguments
# ============================================================
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["data-alerts@company.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=2),
}


# ============================================================
# DAG Definition
# ============================================================
with DAG(
    dag_id="ecommerce_data_warehouse_etl",
    default_args=default_args,
    description="Daily ETL pipeline for e-commerce data warehouse",
    schedule_interval="0 3 * * *",  # 3 AM UTC daily
    start_date=datetime(2024, 1, 1),
    catchup=True,
    max_active_runs=1,            # Prevent parallel runs
    tags=["ecommerce", "data-warehouse", "etl", "production"],
    doc_md=__doc__,
) as dag:

    # --------------------------------------------------------
    # Start & End markers
    # --------------------------------------------------------
    start = EmptyOperator(task_id="start")
    end_success = EmptyOperator(
        task_id="end_success",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # --------------------------------------------------------
    # Step 1: Sensor - Wait for raw data in S3
    # --------------------------------------------------------
    with TaskGroup("data_availability_checks") as data_checks:
        check_clickstream = S3KeySensor(
            task_id="check_clickstream_data",
            bucket_name=S3_BUCKET,
            bucket_key=(
                "raw/clickstream/"
                "year={{ ds_nodash[:4] }}/"
                "month={{ ds_nodash[4:6] }}/"
                "day={{ ds_nodash[6:8] }}/"
            ),
            wildcard_match=True,
            aws_conn_id=AWS_CONN_ID,
            timeout=3600,        # Wait up to 1 hour
            poke_interval=120,   # Check every 2 minutes
            mode="reschedule",   # Free up worker slot while waiting
        )

        check_orders = S3KeySensor(
            task_id="check_orders_data",
            bucket_name=S3_BUCKET,
            bucket_key=(
                "raw/orders/"
                "year={{ ds_nodash[:4] }}/"
                "month={{ ds_nodash[4:6] }}/"
                "day={{ ds_nodash[6:8] }}/"
            ),
            wildcard_match=True,
            aws_conn_id=AWS_CONN_ID,
            timeout=3600,
            poke_interval=120,
            mode="reschedule",
        )

    # --------------------------------------------------------
    # Step 2: Run Glue Crawler to update data catalog
    # --------------------------------------------------------
    run_crawler = GlueCrawlerOperator(
        task_id="run_glue_crawler",
        config={"Name": "ecommerce-raw-data-crawler"},
        aws_conn_id=AWS_CONN_ID,
        poll_interval=30,
        wait_for_completion=True,
    )

    # --------------------------------------------------------
    # Step 3: Glue ETL Jobs (parallel execution)
    # --------------------------------------------------------
    with TaskGroup("glue_etl_jobs") as etl_jobs:
        etl_clickstream = GlueJobOperator(
            task_id="etl_clickstream",
            job_name="etl-clickstream-transform",
            script_args={
                "--JOB_NAME": "etl-clickstream-transform",
                "--S3_INPUT_PATH": f"s3://{S3_BUCKET}/raw/clickstream/",
                "--S3_OUTPUT_PATH": f"s3://{S3_BUCKET}/processed/clickstream/",
                "--REDSHIFT_CONNECTION": GLUE_CONNECTION,
                "--REDSHIFT_DATABASE": "ecommerce_dw",
                "--PROCESSING_DATE": "{{ ds }}",
                "--REDSHIFT_IAM_ROLE": REDSHIFT_IAM_ROLE,
            },
            iam_role_name=GLUE_IAM_ROLE,
            aws_conn_id=AWS_CONN_ID,
            num_of_dpus=5,
            wait_for_completion=True,
            verbose=True,
        )

        etl_orders = GlueJobOperator(
            task_id="etl_orders",
            job_name="etl-orders-transform",
            script_args={
                "--JOB_NAME": "etl-orders-transform",
                "--S3_ORDERS_PATH": f"s3://{S3_BUCKET}/raw/orders/",
                "--S3_LINE_ITEMS_PATH": f"s3://{S3_BUCKET}/raw/order_line_items/",
                "--S3_OUTPUT_PATH": f"s3://{S3_BUCKET}/processed/",
                "--REDSHIFT_CONNECTION": GLUE_CONNECTION,
                "--REDSHIFT_DATABASE": "ecommerce_dw",
                "--PROCESSING_DATE": "{{ ds }}",
                "--REDSHIFT_IAM_ROLE": REDSHIFT_IAM_ROLE,
            },
            iam_role_name=GLUE_IAM_ROLE,
            aws_conn_id=AWS_CONN_ID,
            num_of_dpus=3,
            wait_for_completion=True,
            verbose=True,
        )

    # --------------------------------------------------------
    # Step 4: Load Dimensions (must run before facts)
    # --------------------------------------------------------
    with TaskGroup("load_dimensions") as load_dims:
        load_dim_users = RedshiftSQLOperator(
            task_id="load_dim_users",
            redshift_conn_id=REDSHIFT_CONN_ID,
            sql="redshift/schemas/06_load_dimensions_from_staging.sql",
        )

    # --------------------------------------------------------
    # Step 5: Load Fact Tables
    # --------------------------------------------------------
    load_facts = GlueJobOperator(
        task_id="load_fact_tables",
        job_name="etl-load-facts",
        script_args={
            "--JOB_NAME": "etl-load-facts",
            "--REDSHIFT_CONNECTION": GLUE_CONNECTION,
            "--REDSHIFT_DATABASE": "ecommerce_dw",
            "--PROCESSING_DATE": "{{ ds }}",
        },
        iam_role_name=GLUE_IAM_ROLE,
        aws_conn_id=AWS_CONN_ID,
        num_of_dpus=5,
        wait_for_completion=True,
        verbose=True,
    )

    # --------------------------------------------------------
    # Step 6: Data Quality Checks
    # --------------------------------------------------------
    with TaskGroup("data_quality_checks") as dq_checks:

        check_row_counts = RedshiftSQLOperator(
            task_id="check_row_counts",
            redshift_conn_id=REDSHIFT_CONN_ID,
            sql=dedent("""
                -- Verify fact tables have data for the processing date
                DO $$
                DECLARE
                    v_date_key INTEGER := {{ ds_nodash }}::INTEGER;
                    v_clickstream_count INTEGER;
                    v_orders_count INTEGER;
                BEGIN
                    SELECT COUNT(*) INTO v_clickstream_count
                    FROM dw.fact_clickstream WHERE date_key = v_date_key;

                    SELECT COUNT(*) INTO v_orders_count
                    FROM dw.fact_orders WHERE date_key = v_date_key;

                    IF v_clickstream_count = 0 THEN
                        RAISE EXCEPTION 'No clickstream records for date_key %', v_date_key;
                    END IF;

                    IF v_orders_count = 0 THEN
                        RAISE EXCEPTION 'No order records for date_key %', v_date_key;
                    END IF;

                    -- Log counts to audit table
                    INSERT INTO data_quality.audit_log (
                        check_date, check_name, table_name, row_count, status
                    ) VALUES
                        (CURRENT_DATE, 'row_count', 'fact_clickstream', v_clickstream_count, 'PASS'),
                        (CURRENT_DATE, 'row_count', 'fact_orders', v_orders_count, 'PASS');
                END $$;
            """),
        )

        check_null_keys = RedshiftSQLOperator(
            task_id="check_null_keys",
            redshift_conn_id=REDSHIFT_CONN_ID,
            sql=dedent("""
                -- Check for unresolved dimension keys (should be -1, not NULL)
                DO $$
                DECLARE
                    v_date_key INTEGER := {{ ds_nodash }}::INTEGER;
                    v_null_users INTEGER;
                BEGIN
                    SELECT COUNT(*) INTO v_null_users
                    FROM dw.fact_clickstream
                    WHERE date_key = v_date_key AND user_key IS NULL;

                    IF v_null_users > 0 THEN
                        RAISE EXCEPTION 'Found % records with NULL user_key in fact_clickstream', v_null_users;
                    END IF;

                    INSERT INTO data_quality.audit_log (
                        check_date, check_name, table_name, row_count, status
                    ) VALUES
                        (CURRENT_DATE, 'null_key_check', 'fact_clickstream', 0, 'PASS');
                END $$;
            """),
        )

        check_revenue_anomaly = RedshiftSQLOperator(
            task_id="check_revenue_anomaly",
            redshift_conn_id=REDSHIFT_CONN_ID,
            sql=dedent("""
                -- Alert if daily revenue deviates > 3 standard deviations from 30-day avg
                DO $$
                DECLARE
                    v_date_key INTEGER := {{ ds_nodash }}::INTEGER;
                    v_today_revenue DECIMAL(12,2);
                    v_avg_revenue DECIMAL(12,2);
                    v_std_revenue DECIMAL(12,2);
                BEGIN
                    SELECT COALESCE(SUM(order_total), 0) INTO v_today_revenue
                    FROM dw.fact_orders WHERE date_key = v_date_key;

                    SELECT AVG(daily_rev), STDDEV(daily_rev)
                    INTO v_avg_revenue, v_std_revenue
                    FROM (
                        SELECT date_key, SUM(order_total) AS daily_rev
                        FROM dw.fact_orders
                        WHERE date_key BETWEEN v_date_key - 30 AND v_date_key - 1
                        GROUP BY date_key
                    );

                    IF v_std_revenue IS NOT NULL AND v_std_revenue > 0
                       AND ABS(v_today_revenue - v_avg_revenue) > 3 * v_std_revenue THEN
                        RAISE WARNING 'Revenue anomaly detected: today=$%, avg=$%, stddev=$%',
                            v_today_revenue, v_avg_revenue, v_std_revenue;
                    END IF;

                    INSERT INTO data_quality.audit_log (
                        check_date, check_name, table_name, metric_value, status
                    ) VALUES
                        (CURRENT_DATE, 'revenue_anomaly', 'fact_orders', v_today_revenue, 'PASS');
                END $$;
            """),
        )

        check_duplicate_events = RedshiftSQLOperator(
            task_id="check_duplicate_events",
            redshift_conn_id=REDSHIFT_CONN_ID,
            sql=dedent("""
                DO $$
                DECLARE
                    v_date_key INTEGER := {{ ds_nodash }}::INTEGER;
                    v_dupe_count INTEGER;
                BEGIN
                    SELECT COUNT(*) INTO v_dupe_count
                    FROM (
                        SELECT event_id, COUNT(*) AS cnt
                        FROM dw.fact_clickstream
                        WHERE date_key = v_date_key
                        GROUP BY event_id
                        HAVING COUNT(*) > 1
                    );

                    IF v_dupe_count > 0 THEN
                        RAISE EXCEPTION 'Found % duplicate event_ids in fact_clickstream', v_dupe_count;
                    END IF;
                END $$;
            """),
        )

    # --------------------------------------------------------
    # Step 7: Refresh Materialized Views
    # --------------------------------------------------------
    refresh_mv = RedshiftSQLOperator(
        task_id="refresh_materialized_views",
        redshift_conn_id=REDSHIFT_CONN_ID,
        sql=dedent("""
            REFRESH MATERIALIZED VIEW analytics.mv_daily_revenue;
            REFRESH MATERIALIZED VIEW analytics.mv_product_performance;
            REFRESH MATERIALIZED VIEW analytics.mv_user_funnel;
            REFRESH MATERIALIZED VIEW analytics.mv_traffic_sources;
        """),
    )

    # --------------------------------------------------------
    # Step 8: Create audit log table if not exists
    # --------------------------------------------------------
    create_audit_table = RedshiftSQLOperator(
        task_id="ensure_audit_table",
        redshift_conn_id=REDSHIFT_CONN_ID,
        sql=dedent("""
            CREATE TABLE IF NOT EXISTS data_quality.audit_log (
                id              INTEGER         IDENTITY(1,1),
                check_date      DATE            DEFAULT CURRENT_DATE,
                check_name      VARCHAR(100),
                table_name      VARCHAR(100),
                row_count       BIGINT,
                metric_value    DECIMAL(18,2),
                status          VARCHAR(20),
                details         VARCHAR(1000),
                created_at      TIMESTAMP       DEFAULT GETDATE()
            );
        """),
    )

    # --------------------------------------------------------
    # DAG Dependencies (execution order)
    # --------------------------------------------------------
    start >> data_checks >> run_crawler >> etl_jobs
    etl_jobs >> create_audit_table >> load_dims >> load_facts
    load_facts >> dq_checks >> refresh_mv >> end_success
