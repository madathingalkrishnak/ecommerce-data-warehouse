"""
AWS Glue ETL Job: Load Fact Tables
-----------------------------------
Transforms staging data into dimensional model fact tables in Redshift.
Performs surrogate key lookups against dimension tables and inserts
into fact tables.

This runs AFTER the clickstream and orders ETL jobs have loaded
staging tables, and AFTER dimension tables have been updated.

Glue Job Parameters:
    --JOB_NAME: Glue job name
    --REDSHIFT_CONNECTION: Glue connection name for Redshift
    --REDSHIFT_DATABASE: ecommerce_dw
    --PROCESSING_DATE: YYYY-MM-DD
"""

import sys
from datetime import datetime

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext


# SQL templates for loading fact tables
LOAD_FACT_CLICKSTREAM_SQL = """
INSERT INTO dw.fact_clickstream (
    event_id, date_key, time_key, user_key, product_key,
    device_key, traffic_source_key, event_type, session_id,
    page_url, view_duration_sec, scroll_depth_pct, quantity,
    search_results_count, cart_total, item_count, search_query,
    page_name, coupon_code, event_timestamp
)
SELECT
    s.event_id,
    CAST(TO_CHAR(s.event_timestamp, 'YYYYMMDD') AS INTEGER)    AS date_key,
    CAST(EXTRACT(HOUR FROM s.event_timestamp) * 100 +
         EXTRACT(MINUTE FROM s.event_timestamp) AS INTEGER)     AS time_key,
    COALESCE(du.user_key, -1)                                   AS user_key,
    dp.product_key,                                             -- NULL ok for non-product events
    COALESCE(dd.device_key, -1)                                 AS device_key,
    COALESCE(dts.traffic_source_key, -1)                        AS traffic_source_key,
    s.event_type,
    s.session_id,
    s.page_url,
    s.view_duration_sec,
    s.scroll_depth_pct,
    s.quantity,
    s.results_count,
    s.cart_total,
    s.item_count,
    s.search_query,
    s.page_name,
    s.coupon_code,
    s.event_timestamp
FROM staging.clickstream_events s
LEFT JOIN dw.dim_user du           ON s.user_id = du.user_id AND du.is_current = TRUE
LEFT JOIN dw.dim_product dp        ON s.product_id = dp.product_id AND dp.is_current = TRUE
LEFT JOIN dw.dim_device dd         ON s.device_type = dd.device_type AND s.browser = dd.browser
LEFT JOIN dw.dim_traffic_source dts ON s.traffic_source = dts.traffic_source
WHERE s.year = {year} AND s.month = {month} AND s.day = {day}
  AND NOT EXISTS (
    SELECT 1 FROM dw.fact_clickstream f WHERE f.event_id = s.event_id
  );
"""

LOAD_FACT_ORDERS_SQL = """
INSERT INTO dw.fact_orders (
    order_id, date_key, time_key, user_key, shipping_key,
    payment_key, order_status, coupon_code, subtotal,
    discount_total, shipping_cost, tax_rate, tax_amount,
    order_total, num_items, order_timestamp
)
SELECT
    s.order_id,
    CAST(TO_CHAR(s.order_timestamp, 'YYYYMMDD') AS INTEGER)    AS date_key,
    CAST(EXTRACT(HOUR FROM s.order_timestamp) * 100 +
         EXTRACT(MINUTE FROM s.order_timestamp) AS INTEGER)     AS time_key,
    COALESCE(du.user_key, -1)                                   AS user_key,
    COALESCE(ds.shipping_key, -1)                               AS shipping_key,
    COALESCE(dpay.payment_key, -1)                              AS payment_key,
    s.status,
    s.coupon_code,
    s.subtotal,
    s.discount_total,
    s.shipping_cost,
    s.tax_rate,
    s.tax_amount,
    s.order_total,
    s.num_items,
    s.order_timestamp
FROM staging.orders s
LEFT JOIN dw.dim_user du           ON s.user_id = du.user_id AND du.is_current = TRUE
LEFT JOIN dw.dim_shipping ds       ON s.shipping_method = ds.shipping_method
LEFT JOIN dw.dim_payment dpay      ON s.payment_method = dpay.payment_method
WHERE s.year = {year} AND s.month = {month} AND s.day = {day}
  AND NOT EXISTS (
    SELECT 1 FROM dw.fact_orders f WHERE f.order_id = s.order_id
  );
"""

LOAD_FACT_ORDER_ITEMS_SQL = """
INSERT INTO dw.fact_order_items (
    line_item_id, order_id, date_key, user_key, product_key,
    quantity, unit_price, line_total, discount_amount, net_amount
)
SELECT
    sli.line_item_id,
    sli.order_id,
    fo.date_key,
    fo.user_key,
    COALESCE(dp.product_key, -1)    AS product_key,
    sli.quantity,
    sli.unit_price,
    sli.line_total,
    sli.discount_amount,
    (sli.line_total - sli.discount_amount) AS net_amount
FROM staging.order_line_items sli
INNER JOIN dw.fact_orders fo       ON sli.order_id = fo.order_id
LEFT JOIN dw.dim_product dp        ON sli.product_id = dp.product_id AND dp.is_current = TRUE
WHERE NOT EXISTS (
    SELECT 1 FROM dw.fact_order_items f WHERE f.line_item_id = sli.line_item_id
);
"""

LOAD_DAILY_SESSIONS_SQL = """
INSERT INTO dw.fact_daily_user_sessions (
    date_key, user_key, device_key, traffic_source_key,
    session_count, total_page_views, total_product_views,
    total_add_to_cart, total_searches, total_events,
    avg_view_duration, max_scroll_depth,
    did_add_to_cart, did_begin_checkout, did_purchase
)
SELECT
    fc.date_key,
    fc.user_key,
    -- Use most frequent device/source for the day
    MODE() WITHIN GROUP (ORDER BY fc.device_key)               AS device_key,
    MODE() WITHIN GROUP (ORDER BY fc.traffic_source_key)       AS traffic_source_key,
    COUNT(DISTINCT fc.session_id)                               AS session_count,
    SUM(CASE WHEN fc.event_type = 'page_view' THEN 1 ELSE 0 END) AS total_page_views,
    SUM(CASE WHEN fc.event_type = 'product_view' THEN 1 ELSE 0 END) AS total_product_views,
    SUM(CASE WHEN fc.event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS total_add_to_cart,
    SUM(CASE WHEN fc.event_type = 'search' THEN 1 ELSE 0 END) AS total_searches,
    COUNT(*)                                                    AS total_events,
    AVG(fc.view_duration_sec)                                   AS avg_view_duration,
    MAX(fc.scroll_depth_pct)                                    AS max_scroll_depth,
    MAX(CASE WHEN fc.event_type = 'add_to_cart' THEN TRUE ELSE FALSE END) AS did_add_to_cart,
    MAX(CASE WHEN fc.event_type = 'begin_checkout' THEN TRUE ELSE FALSE END) AS did_begin_checkout,
    CASE WHEN EXISTS (
        SELECT 1 FROM dw.fact_orders fo
        WHERE fo.user_key = fc.user_key AND fo.date_key = fc.date_key
    ) THEN TRUE ELSE FALSE END                                  AS did_purchase
FROM dw.fact_clickstream fc
WHERE fc.date_key = {date_key}
GROUP BY fc.date_key, fc.user_key;
"""


def main():
    args = getResolvedOptions(sys.argv, [
        "JOB_NAME",
        "REDSHIFT_CONNECTION",
        "REDSHIFT_DATABASE",
        "PROCESSING_DATE",
    ])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    logger = glueContext.get_logger()
    processing_date = args["PROCESSING_DATE"]
    year, month, day = processing_date.split("-")
    date_key = int(f"{year}{month}{day}")

    # Get JDBC connection URL from Glue connection
    connection = glueContext.extract_jdbc_conf(args["REDSHIFT_CONNECTION"])
    jdbc_url = connection["url"]
    props = {
        "user": connection["user"],
        "password": connection["password"],
        "driver": "com.amazon.redshift.jdbc42.Driver",
    }

    def execute_sql(sql: str, description: str):
        """Execute SQL against Redshift via JDBC."""
        logger.info(f"Executing: {description}")
        try:
            # Use spark JDBC to execute
            spark.read.jdbc(
                url=jdbc_url,
                table=f"({sql.rstrip(';')}) AS tmp",
                properties=props,
            )
        except Exception:
            # For INSERT/UPDATE statements, use a different approach
            from py4j.java_gateway import java_import
            java_import(spark._jvm, "java.sql.DriverManager")
            conn = spark._jvm.DriverManager.getConnection(
                jdbc_url, props["user"], props["password"]
            )
            try:
                stmt = conn.createStatement()
                stmt.execute(sql)
                stmt.close()
            finally:
                conn.close()
        logger.info(f"Completed: {description}")

    # -----------------------------------------------------------
    # Execute fact table loads in dependency order
    # -----------------------------------------------------------

    # 1. Delete existing data for the processing date (idempotent reload)
    execute_sql(
        f"DELETE FROM dw.fact_daily_user_sessions WHERE date_key = {date_key};",
        "Clear daily sessions"
    )
    execute_sql(
        f"DELETE FROM dw.fact_order_items WHERE date_key = {date_key};",
        "Clear order items"
    )
    execute_sql(
        f"DELETE FROM dw.fact_orders WHERE date_key = {date_key};",
        "Clear orders"
    )
    execute_sql(
        f"DELETE FROM dw.fact_clickstream WHERE date_key = {date_key};",
        "Clear clickstream"
    )

    # 2. Load fact tables
    execute_sql(
        LOAD_FACT_CLICKSTREAM_SQL.format(year=year, month=int(month), day=int(day)),
        "Load fact_clickstream"
    )
    execute_sql(
        LOAD_FACT_ORDERS_SQL.format(year=year, month=int(month), day=int(day)),
        "Load fact_orders"
    )
    execute_sql(
        LOAD_FACT_ORDER_ITEMS_SQL,
        "Load fact_order_items"
    )

    # 3. Load aggregated daily sessions
    execute_sql(
        LOAD_DAILY_SESSIONS_SQL.format(date_key=date_key),
        "Load fact_daily_user_sessions"
    )

    # 4. Run ANALYZE on loaded tables for query optimizer
    for table in ["fact_clickstream", "fact_orders", "fact_order_items", "fact_daily_user_sessions"]:
        execute_sql(f"ANALYZE dw.{table};", f"Analyze {table}")

    logger.info(f"All fact tables loaded for {processing_date}")
    job.commit()


if __name__ == "__main__":
    main()
