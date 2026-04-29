#!/usr/bin/env python3
"""
Fetch JSON from the public GDELT Doc API and upload it to S3.

GDELT Doc API (no API key): https://api.gdeltproject.org/api/v2/doc/doc

Requires AWS credentials (env, profile, or EC2 instance role) with s3:PutObject
on the target bucket.

Example:
  python scripts/gdelt_fetch_to_s3.py
  python scripts/gdelt_fetch_to_s3.py --query "ukraine" --maxrecords 100
  S3_BUCKET=my-bucket python scripts/gdelt_fetch_to_s3.py
"""

from __future__ import annotations

import argparse
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3

GDELT_DOC_BASE = "https://api.gdeltproject.org/api/v2/doc/doc"
DEFAULT_BUCKET = "visorbacket"
DEFAULT_PREFIX = "gdelt/"


def build_gdelt_url(query: str, maxrecords: int, timespan: str) -> str:
    params = {
        "query": query,
        "mode": "ArtList",
        "format": "json",
        "maxrecords": str(maxrecords),
        "timespan": timespan,
        "sort": "datedesc",
    }
    return f"{GDELT_DOC_BASE}?{urllib.parse.urlencode(params)}"


def fetch_gdelt(url: str, timeout: int = 120) -> bytes:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "gdelt-fetch-to-s3/1.0 (learning script)"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def upload_to_s3(bucket: str, key: str, body: bytes, content_type: str = "application/json") -> None:
    client = boto3.client("s3")
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType=content_type,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="GDELT Doc API → S3")
    parser.add_argument(
        "--bucket",
        default=os.environ.get("S3_BUCKET", DEFAULT_BUCKET),
        help=f"S3 bucket name (default: env S3_BUCKET or {DEFAULT_BUCKET})",
    )
    parser.add_argument(
        "--prefix",
        default=os.environ.get("S3_PREFIX", DEFAULT_PREFIX).rstrip("/") + "/",
        help=f"S3 key prefix (default: env S3_PREFIX or {DEFAULT_PREFIX})",
    )
    parser.add_argument(
        "--query",
        default=os.environ.get("GDELT_QUERY", "global climate"),
        help='GDELT search query (default: env GDELT_QUERY or "global climate")',
    )
    parser.add_argument(
        "--maxrecords",
        type=int,
        default=int(os.environ.get("GDELT_MAXRECORDS", "50")),
        help="Max records from GDELT (default: 50)",
    )
    parser.add_argument(
        "--timespan",
        default=os.environ.get("GDELT_TIMESPAN", "24h"),
        help="GDELT timespan e.g. 24h, 7d (default: 24h)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch only, print size and URL; do not upload to S3",
    )
    args = parser.parse_args()

    url = build_gdelt_url(args.query, args.maxrecords, args.timespan)
    print(f"Fetching: {url}")

    try:
        body = fetch_gdelt(url)
    except urllib.error.HTTPError as e:
        print(f"GDELT HTTP error: {e.code} {e.reason}", file=sys.stderr)
        return 1
    except urllib.error.URLError as e:
        print(f"GDELT network error: {e.reason}", file=sys.stderr)
        return 1

    print(f"Downloaded {len(body)} bytes")

    if args.dry_run:
        print("Dry run — skipping S3 upload")
        return 0

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe_query = "".join(c if c.isalnum() or c in "-_" else "_" for c in args.query)[:40]
    key = f"{args.prefix}{ts}_{safe_query}.json"

    print(f"Uploading s3://{args.bucket}/{key}")
    try:
        upload_to_s3(args.bucket, key, body)
    except Exception as e:
        print(f"S3 upload failed: {e}", file=sys.stderr)
        return 1

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
