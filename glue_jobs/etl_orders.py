"""
AWS Glue ETL Job: Orders & Line Items Transformation
-----------------------------------------------------
Reads raw orders and line items from S3, performs validation,
enrichment, and loads into Redshift staging tables.

Glue Job Parameters:
    --JOB_NAME: Glue job name
    --S3_ORDERS_PATH: s3://bucket/raw/orders/
    --S3_LINE_ITEMS_PATH: s3://bucket/raw/order_line_items/
    --S3_OUTPUT_PATH: s3://bucket/processed/orders/
    --REDSHIFT_CONNECTION: Glue connection name for Redshift
    --REDSHIFT_DATABASE: ecommerce_dw
    --PROCESSING_DATE: YYYY-MM-DD
    --REDSHIFT_IAM_ROLE: ARN of role for COPY command
"""

import sys
from datetime import datetime

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DecimalType
from pyspark.sql.window import Window


def main():
    args = getResolvedOptions(sys.argv, [
        "JOB_NAME",
        "S3_ORDERS_PATH",
        "S3_LINE_ITEMS_PATH",
        "S3_OUTPUT_PATH",
        "REDSHIFT_CONNECTION",
        "REDSHIFT_DATABASE",
        "PROCESSING_DATE",
        "REDSHIFT_IAM_ROLE",
    ])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    logger = glueContext.get_logger()
    processing_date = args["PROCESSING_DATE"]
    year, month, day = processing_date.split("-")

    logger.info(f"Processing orders data for {processing_date}")

    # -----------------------------------------------------------
    # Step 1: Read raw data
    # -----------------------------------------------------------
    orders_path = f"{args['S3_ORDERS_PATH']}year={year}/month={month}/day={day}/"
    line_items_path = f"{args['S3_LINE_ITEMS_PATH']}year={year}/month={month}/day={day}/"

    orders_df = spark.read.parquet(orders_path)
    line_items_df = spark.read.parquet(line_items_path)

    raw_orders_count = orders_df.count()
    raw_items_count = line_items_df.count()
    logger.info(f"Raw orders: {raw_orders_count}, Raw line items: {raw_items_count}")

    if raw_orders_count == 0:
        logger.warn(f"No orders for {processing_date}. Exiting.")
        job.commit()
        return

    # -----------------------------------------------------------
    # Step 2: Clean & validate orders
    # -----------------------------------------------------------
    cleaned_orders = (
        orders_df
        .filter(
            F.col("order_id").isNotNull() &
            F.col("user_id").isNotNull() &
            F.col("order_timestamp").isNotNull() &
            (F.col("order_total") > 0)
        )
        .withColumn("order_timestamp", F.to_timestamp(F.col("order_timestamp")))
        .withColumn("status", F.lower(F.trim(F.col("status"))))
        # Ensure financial consistency
        .withColumn("computed_total",
            F.round(
                F.col("subtotal") - F.col("discount_total") +
                F.col("shipping_cost") + F.col("tax_amount"), 2
            )
        )
        .withColumn("total_discrepancy",
            F.abs(F.col("order_total") - F.col("computed_total"))
        )
        # Flag orders with large discrepancies (> $1) for review
        .withColumn("is_valid_total",
            F.col("total_discrepancy") < 1.0
        )
    )

    # Log problematic orders
    invalid_count = cleaned_orders.filter(~F.col("is_valid_total")).count()
    if invalid_count > 0:
        logger.warn(f"Found {invalid_count} orders with total discrepancies > $1")

    # -----------------------------------------------------------
    # Step 3: Deduplicate orders
    # -----------------------------------------------------------
    window_spec = Window.partitionBy("order_id").orderBy(F.col("order_timestamp").desc())
    deduped_orders = (
        cleaned_orders
        .withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num", "computed_total", "total_discrepancy", "is_valid_total")
    )

    # -----------------------------------------------------------
    # Step 4: Enrich orders
    # -----------------------------------------------------------
    enriched_orders = (
        deduped_orders
        .withColumn("date_key",
            F.date_format(F.col("order_timestamp"), "yyyyMMdd").cast(IntegerType())
        )
        .withColumn("time_key",
            (F.hour(F.col("order_timestamp")) * 100 +
             F.minute(F.col("order_timestamp"))).cast(IntegerType())
        )
        .withColumn("etl_processed_at", F.current_timestamp())
    )

    # -----------------------------------------------------------
    # Step 5: Clean & validate line items
    # -----------------------------------------------------------
    cleaned_items = (
        line_items_df
        .filter(
            F.col("order_id").isNotNull() &
            F.col("line_item_id").isNotNull() &
            F.col("product_id").isNotNull() &
            (F.col("quantity") > 0) &
            (F.col("unit_price") >= 0)
        )
        # Compute net amount
        .withColumn("net_amount",
            F.round(F.col("line_total") - F.col("discount_amount"), 2)
        )
        .withColumn("etl_processed_at", F.current_timestamp())
    )

    # Deduplicate line items
    item_window = Window.partitionBy("line_item_id").orderBy(F.col("line_total").desc())
    deduped_items = (
        cleaned_items
        .withColumn("row_num", F.row_number().over(item_window))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    # Validate referential integrity: all line items should belong to a valid order
    orphan_items = deduped_items.join(
        deduped_orders.select("order_id"),
        on="order_id",
        how="left_anti"
    )
    orphan_count = orphan_items.count()
    if orphan_count > 0:
        logger.warn(f"Found {orphan_count} orphan line items (no matching order)")

    # Keep only items with valid orders
    valid_items = deduped_items.join(
        deduped_orders.select("order_id"),
        on="order_id",
        how="inner"
    )

    # -----------------------------------------------------------
    # Step 6: Write processed data to S3
    # -----------------------------------------------------------
    orders_output = f"{args['S3_OUTPUT_PATH']}orders/year={year}/month={month}/day={day}/"
    items_output = f"{args['S3_OUTPUT_PATH']}order_line_items/year={year}/month={month}/day={day}/"

    enriched_orders.repartition(2).write.mode("overwrite").parquet(orders_output)
    valid_items.repartition(2).write.mode("overwrite").parquet(items_output)

    # -----------------------------------------------------------
    # Step 7: Load into Redshift staging tables
    # -----------------------------------------------------------
    logger.info("Loading orders into Redshift staging...")

    # Orders
    orders_staging = enriched_orders.select(
        "order_id", "user_id", "order_timestamp", "status",
        "subtotal", "discount_total", "shipping_cost", "shipping_method",
        "tax_rate", "tax_amount", "order_total", "payment_method",
        "coupon_code", "num_items", "year", "month", "day",
    )

    orders_dyf = DynamicFrame.fromDF(orders_staging, glueContext, "orders_output")
    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=orders_dyf,
        catalog_connection=args["REDSHIFT_CONNECTION"],
        connection_options={
            "dbtable": "staging.orders",
            "database": args["REDSHIFT_DATABASE"],
            "preactions": f"""
                DELETE FROM staging.orders
                WHERE year = {year} AND month = {int(month)} AND day = {int(day)};
            """,
        },
        redshift_tmp_dir=f"{args['S3_OUTPUT_PATH']}tmp/",
    )

    # Line Items
    items_staging = valid_items.select(
        "order_id", "line_item_id", "product_id",
        "quantity", "unit_price", "line_total", "discount_amount",
    )

    items_dyf = DynamicFrame.fromDF(items_staging, glueContext, "items_output")
    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=items_dyf,
        catalog_connection=args["REDSHIFT_CONNECTION"],
        connection_options={
            "dbtable": "staging.order_line_items",
            "database": args["REDSHIFT_DATABASE"],
            "preactions": "TRUNCATE TABLE staging.order_line_items;",
        },
        redshift_tmp_dir=f"{args['S3_OUTPUT_PATH']}tmp/",
    )

    final_orders = enriched_orders.count()
    final_items = valid_items.count()
    logger.info(f"ETL complete. Orders: {final_orders}, Line Items: {final_items}")

    job.commit()


if __name__ == "__main__":
    main()
