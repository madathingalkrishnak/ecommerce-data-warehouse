#!/usr/bin/env python3
"""
Main script to generate all synthetic e-commerce data.
Outputs Parquet files partitioned by date, mimicking how data
would land in S3 from Kinesis Firehose.

Usage:
    python -m data_generator.generate_all --days 30 --output ./sample_data
    python -m data_generator.generate_all --start-date 2024-01-01 --end-date 2024-01-31
"""

import argparse
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from .config import (
    NUM_USERS, NUM_PRODUCTS,
    NUM_CLICKSTREAM_EVENTS_PER_DAY, NUM_PURCHASES_PER_DAY,
    OUTPUT_DIR, BATCH_SIZE,
)
from .generators import (
    generate_users, generate_products,
    generate_clickstream_events, generate_purchases,
)


def parse_args():
    parser = argparse.ArgumentParser(description="Generate synthetic e-commerce data")
    parser.add_argument("--output", "-o", default=OUTPUT_DIR, help="Output directory")
    parser.add_argument("--days", "-d", type=int, default=7, help="Number of days to generate")
    parser.add_argument("--start-date", type=str, default="2024-01-01", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, default=None, help="End date (YYYY-MM-DD). Overrides --days")
    parser.add_argument("--num-users", type=int, default=NUM_USERS, help="Number of users")
    parser.add_argument("--num-products", type=int, default=NUM_PRODUCTS, help="Number of products")
    parser.add_argument("--events-per-day", type=int, default=NUM_CLICKSTREAM_EVENTS_PER_DAY)
    parser.add_argument("--purchases-per-day", type=int, default=NUM_PURCHASES_PER_DAY)
    parser.add_argument("--scale", type=float, default=0.01,
                        help="Scale factor (0.01 = 1%% of full volume, good for local dev)")
    parser.add_argument("--format", choices=["parquet", "json"], default="parquet")
    return parser.parse_args()


def save_dimension_table(data: list[dict], name: str, output_dir: str, fmt: str):
    """Save a dimension table (users, products) as a single file."""
    path = Path(output_dir) / "dimensions" / name
    path.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(data)
    if fmt == "parquet":
        filepath = path / f"{name}.parquet"
        df.to_parquet(filepath, index=False, engine="pyarrow")
    else:
        filepath = path / f"{name}.json"
        df.to_json(filepath, orient="records", lines=True)

    print(f"  Saved {len(df):,} {name} records -> {filepath}")
    return df


def save_partitioned_data(
    events: list[dict],
    name: str,
    output_dir: str,
    target_date: datetime,
    fmt: str,
):
    """Save event data partitioned by year/month/day (S3-style layout)."""
    partition_path = (
        Path(output_dir) / name
        / f"year={target_date.year}"
        / f"month={target_date.month:02d}"
        / f"day={target_date.day:02d}"
    )
    partition_path.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(events)

    if fmt == "parquet":
        filepath = partition_path / f"{name}.parquet"
        df.to_parquet(filepath, index=False, engine="pyarrow")
    else:
        filepath = partition_path / f"{name}.json"
        df.to_json(filepath, orient="records", lines=True)

    print(f"  {target_date.date()} | {len(df):>8,} {name} records -> {filepath}")
    return len(df)


def main():
    args = parse_args()
    output_dir = args.output
    fmt = args.format

    # Apply scale factor
    scaled_events = max(100, int(args.events_per_day * args.scale))
    scaled_purchases = max(10, int(args.purchases_per_day * args.scale))
    scaled_users = max(100, int(args.num_users * args.scale))
    scaled_products = max(50, int(args.num_products * args.scale))

    start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    if args.end_date:
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
    else:
        end_date = start_date + timedelta(days=args.days - 1)

    print("=" * 60)
    print("E-Commerce Data Warehouse - Synthetic Data Generator")
    print("=" * 60)
    print(f"  Output:     {output_dir}")
    print(f"  Format:     {fmt}")
    print(f"  Date range: {start_date.date()} to {end_date.date()}")
    print(f"  Scale:      {args.scale:.0%}")
    print(f"  Users:      {scaled_users:,}")
    print(f"  Products:   {scaled_products:,}")
    print(f"  Events/day: ~{scaled_events:,}")
    print(f"  Orders/day: ~{scaled_purchases:,}")
    print("=" * 60)

    # 1. Generate dimension tables
    print("\n[1/3] Generating dimension tables...")
    users = generate_users(scaled_users)
    save_dimension_table(users, "users", output_dir, fmt)

    products = generate_products(scaled_products)
    save_dimension_table(products, "products", output_dir, fmt)

    # 2. Generate clickstream events per day
    print("\n[2/3] Generating clickstream events...")
    total_events = 0
    current_date = start_date
    while current_date <= end_date:
        events = list(generate_clickstream_events(users, products, current_date, scaled_events))
        total_events += save_partitioned_data(events, "clickstream", output_dir, current_date, fmt)
        current_date += timedelta(days=1)

    # 3. Generate purchase transactions per day
    print("\n[3/3] Generating purchase transactions...")
    total_orders = 0
    total_line_items = 0
    current_date = start_date
    while current_date <= end_date:
        orders = list(generate_purchases(users, products, current_date, scaled_purchases))

        # Flatten line items into separate dataset
        line_items = []
        orders_flat = []
        for order in orders:
            items = order.pop("line_items")
            line_items.extend(items)
            orders_flat.append(order)

        total_orders += save_partitioned_data(orders_flat, "orders", output_dir, current_date, fmt)
        total_line_items += save_partitioned_data(line_items, "order_line_items", output_dir, current_date, fmt)
        current_date += timedelta(days=1)

    # Summary
    print("\n" + "=" * 60)
    print("Generation complete!")
    print(f"  Total clickstream events: {total_events:,}")
    print(f"  Total orders:             {total_orders:,}")
    print(f"  Total line items:         {total_line_items:,}")
    print(f"  Total users:              {scaled_users:,}")
    print(f"  Total products:           {scaled_products:,}")
    print("=" * 60)


if __name__ == "__main__":
    main()
