"""
AWS Glue ETL Job: Clickstream Event Transformation
---------------------------------------------------
Reads raw clickstream data from S3, applies cleaning, deduplication,
enrichment, and writes processed Parquet to the processed zone.
Then loads into Redshift fact table via COPY.

Glue Job Parameters:
    --JOB_NAME: Glue job name
    --S3_INPUT_PATH: s3://bucket/raw/clickstream/
    --S3_OUTPUT_PATH: s3://bucket/processed/clickstream/
    --REDSHIFT_CONNECTION: Glue connection name for Redshift
    --REDSHIFT_DATABASE: ecommerce_dw
    --PROCESSING_DATE: YYYY-MM-DD (date to process)
    --REDSHIFT_IAM_ROLE: ARN of role for COPY command
"""

import sys
from datetime import datetime

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, BooleanType,
)
from pyspark.sql.window import Window


def main():
    # Parse job arguments
    args = getResolvedOptions(sys.argv, [
        "JOB_NAME",
        "S3_INPUT_PATH",
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

    logger.info(f"Processing clickstream data for {processing_date}")

    # -----------------------------------------------------------
    # Step 1: Read raw data from S3 (date-partitioned)
    # -----------------------------------------------------------
    input_path = f"{args['S3_INPUT_PATH']}year={year}/month={month}/day={day}/"
    logger.info(f"Reading from: {input_path}")

    raw_df = spark.read.parquet(input_path)
    raw_count = raw_df.count()
    logger.info(f"Raw records read: {raw_count}")

    if raw_count == 0:
        logger.warn(f"No data found for {processing_date}. Exiting.")
        job.commit()
        return

    # -----------------------------------------------------------
    # Step 2: Data Cleaning
    # -----------------------------------------------------------
    cleaned_df = (
        raw_df
        # Drop records with null required fields
        .filter(
            F.col("event_id").isNotNull() &
            F.col("event_type").isNotNull() &
            F.col("event_timestamp").isNotNull()
        )
        # Standardize event types
        .withColumn("event_type", F.lower(F.trim(F.col("event_type"))))
        # Parse timestamp if string
        .withColumn(
            "event_timestamp",
            F.to_timestamp(F.col("event_timestamp"))
        )
        # Clamp numeric values to valid ranges
        .withColumn(
            "view_duration_sec",
            F.when(F.col("view_duration_sec") < 0, 0)
             .when(F.col("view_duration_sec") > 3600, 3600)
             .otherwise(F.col("view_duration_sec"))
        )
        .withColumn(
            "scroll_depth_pct",
            F.when(F.col("scroll_depth_pct") < 0, 0)
             .when(F.col("scroll_depth_pct") > 100, 100)
             .otherwise(F.col("scroll_depth_pct"))
        )
        # Trim string fields
        .withColumn("search_query", F.trim(F.col("search_query")))
        .withColumn("page_url", F.trim(F.col("page_url")))
    )

    # -----------------------------------------------------------
    # Step 3: Deduplication
    # -----------------------------------------------------------
    # Use window function to keep latest version of each event
    window_spec = Window.partitionBy("event_id").orderBy(F.col("event_timestamp").desc())
    deduped_df = (
        cleaned_df
        .withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )
    dedup_count = deduped_df.count()
    logger.info(f"After dedup: {dedup_count} (removed {raw_count - dedup_count} dupes)")

    # -----------------------------------------------------------
    # Step 4: Enrichment
    # -----------------------------------------------------------
    enriched_df = (
        deduped_df
        # Extract date/time keys for dimensional model joins
        .withColumn("date_key",
            F.date_format(F.col("event_timestamp"), "yyyyMMdd").cast(IntegerType())
        )
        .withColumn("time_key",
            (F.hour(F.col("event_timestamp")) * 100 + F.minute(F.col("event_timestamp"))).cast(IntegerType())
        )
        # Compute session duration rank (for session-level analytics)
        .withColumn("is_bounce",
            F.when(
                F.count("event_id").over(
                    Window.partitionBy("session_id")
                ) == 1,
                F.lit(True)
            ).otherwise(F.lit(False))
        )
        # Add processing metadata
        .withColumn("etl_processed_at", F.current_timestamp())
        .withColumn("processing_date", F.lit(processing_date))
    )

    # -----------------------------------------------------------
    # Step 5: Write processed data to S3
    # -----------------------------------------------------------
    output_path = f"{args['S3_OUTPUT_PATH']}year={year}/month={month}/day={day}/"
    logger.info(f"Writing processed data to: {output_path}")

    (
        enriched_df
        .repartition(4)  # Control number of output files
        .write
        .mode("overwrite")
        .parquet(output_path)
    )

    # -----------------------------------------------------------
    # Step 6: Load into Redshift staging table via COPY
    # -----------------------------------------------------------
    logger.info("Loading into Redshift staging table...")

    # Select columns matching staging table schema
    redshift_df = enriched_df.select(
        "event_id", "event_type", "event_timestamp", "user_id",
        "session_id", "device_type", "browser", "traffic_source",
        "page_url", "referrer_url", "ip_address", "user_agent",
        "page_name", "product_id", "quantity", "view_duration_sec",
        "scroll_depth_pct", "search_query", "results_count",
        "cart_total", "item_count", "coupon_code", "discount_pct",
        "filter_type", "filter_value", "year", "month", "day",
    )

    dynamic_frame = DynamicFrame.fromDF(redshift_df, glueContext, "clickstream_output")

    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dynamic_frame,
        catalog_connection=args["REDSHIFT_CONNECTION"],
        connection_options={
            "dbtable": "staging.clickstream_events",
            "database": args["REDSHIFT_DATABASE"],
            "preactions": f"""
                DELETE FROM staging.clickstream_events
                WHERE year = {year} AND month = {int(month)} AND day = {int(day)};
            """,
        },
        redshift_tmp_dir=f"{args['S3_OUTPUT_PATH']}tmp/",
    )

    final_count = enriched_df.count()
    logger.info(f"ETL complete. Loaded {final_count} records for {processing_date}")

    # -----------------------------------------------------------
    # Step 7: Record metrics for data quality checks
    # -----------------------------------------------------------
    metrics = {
        "processing_date": processing_date,
        "raw_count": raw_count,
        "dedup_count": dedup_count,
        "final_count": final_count,
        "null_user_pct": round(
            enriched_df.filter(F.col("user_id").isNull()).count() / max(final_count, 1) * 100, 2
        ),
    }
    logger.info(f"ETL Metrics: {metrics}")

    job.commit()


if __name__ == "__main__":
    main()
