#!/usr/bin/env python3
"""
Upload synthetic data to S3 in the partitioned layout
expected by Glue crawlers and Redshift COPY commands.

Usage:
    python scripts/setup_s3.py --data-dir ./sample_data --bucket my-bucket

    # Dry run (no upload):
    python scripts/setup_s3.py --data-dir ./sample_data --bucket my-bucket --dry-run
"""

import argparse
import os
import sys
from pathlib import Path

import boto3
from botocore.exceptions import ClientError


def upload_directory_to_s3(local_dir: str, bucket: str, s3_prefix: str, dry_run: bool = False):
    """Recursively upload a directory to S3, preserving folder structure."""
    s3_client = boto3.client("s3")
    local_path = Path(local_dir)
    uploaded = 0
    skipped = 0

    for file_path in sorted(local_path.rglob("*")):
        if file_path.is_dir():
            continue

        relative_path = file_path.relative_to(local_path)
        s3_key = f"{s3_prefix}{relative_path}"

        if dry_run:
            print(f"  [DRY RUN] {file_path} -> s3://{bucket}/{s3_key}")
            uploaded += 1
            continue

        try:
            # Check if file already exists and is same size
            try:
                response = s3_client.head_object(Bucket=bucket, Key=s3_key)
                s3_size = response["ContentLength"]
                local_size = file_path.stat().st_size
                if s3_size == local_size:
                    skipped += 1
                    continue
            except ClientError:
                pass  # File doesn't exist yet

            content_type = "application/octet-stream"
            if file_path.suffix == ".parquet":
                content_type = "application/x-parquet"
            elif file_path.suffix == ".json":
                content_type = "application/json"

            s3_client.upload_file(
                str(file_path),
                bucket,
                s3_key,
                ExtraArgs={"ContentType": content_type},
            )
            uploaded += 1
            print(f"  Uploaded: s3://{bucket}/{s3_key}")

        except Exception as e:
            print(f"  ERROR uploading {file_path}: {e}", file=sys.stderr)

    return uploaded, skipped


def ensure_bucket_exists(bucket: str, region: str = "us-east-1"):
    """Create S3 bucket if it doesn't exist."""
    s3_client = boto3.client("s3", region_name=region)
    try:
        s3_client.head_bucket(Bucket=bucket)
        print(f"  Bucket exists: {bucket}")
    except ClientError as e:
        error_code = int(e.response["Error"]["Code"])
        if error_code == 404:
            print(f"  Creating bucket: {bucket}")
            if region == "us-east-1":
                s3_client.create_bucket(Bucket=bucket)
            else:
                s3_client.create_bucket(
                    Bucket=bucket,
                    CreateBucketConfiguration={"LocationConstraint": region},
                )
            # Enable versioning
            s3_client.put_bucket_versioning(
                Bucket=bucket,
                VersioningConfiguration={"Status": "Enabled"},
            )
            # Block public access
            s3_client.put_public_access_block(
                Bucket=bucket,
                PublicAccessBlockConfiguration={
                    "BlockPublicAcls": True,
                    "IgnorePublicAcls": True,
                    "BlockPublicPolicy": True,
                    "RestrictPublicBuckets": True,
                },
            )
        else:
            raise


def main():
    parser = argparse.ArgumentParser(description="Upload data to S3")
    parser.add_argument("--data-dir", required=True, help="Local data directory")
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument("--prefix", default="raw/", help="S3 key prefix")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be uploaded")
    args = parser.parse_args()

    print("=" * 60)
    print("Uploading data to S3")
    print("=" * 60)
    print(f"  Source:  {args.data_dir}")
    print(f"  Target:  s3://{args.bucket}/{args.prefix}")
    print(f"  Dry run: {args.dry_run}")
    print()

    if not args.dry_run:
        ensure_bucket_exists(args.bucket, args.region)

    # Upload each data category
    categories = ["dimensions", "clickstream", "orders", "order_line_items"]
    total_uploaded = 0
    total_skipped = 0

    for category in categories:
        local_dir = os.path.join(args.data_dir, category)
        if not os.path.exists(local_dir):
            print(f"  Skipping {category} (not found)")
            continue

        print(f"\n  Uploading {category}...")
        uploaded, skipped = upload_directory_to_s3(
            local_dir, args.bucket, f"{args.prefix}{category}/", args.dry_run
        )
        total_uploaded += uploaded
        total_skipped += skipped

    print(f"\n  Done! Uploaded: {total_uploaded}, Skipped (unchanged): {total_skipped}")


if __name__ == "__main__":
    main()
