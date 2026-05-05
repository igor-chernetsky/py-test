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

Rate limits: GDELT may return HTTP 429. This script retries with backoff (429/502/503).
Tune with GDELT_MAX_ATTEMPTS, GDELT_RETRY_BASE_SEC, GDELT_RETRY_CAP_SEC, or CLI flags.
Use `flock` in cron so two fetchers never overlap. If logs still show `query=global+climate`,
update the server repo or unset legacy GDELT_QUERY so the default positive-tone query is used.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3

GDELT_DOC_BASE = "https://api.gdeltproject.org/api/v2/doc/doc"
DEFAULT_BUCKET = "visorbacket"
DEFAULT_PREFIX = "gdelt/"
DEFAULT_QUERY = (
    '(tone>0) AND (nature OR environment OR "world news" OR international OR science OR family)'
)


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


RETRYABLE_HTTP = {429, 502, 503}


def _retry_after_seconds(err: urllib.error.HTTPError, cap: float) -> float | None:
    raw = err.headers.get("Retry-After")
    if not raw:
        return None
    try:
        return min(float(raw), cap)
    except ValueError:
        return None


def _backoff_seconds(attempt: int, base: float, cap: float) -> float:
    """attempt is 0-based index of the next sleep (after failure)."""
    return min(base * (2**attempt), cap)


def fetch_gdelt(
    url: str,
    *,
    timeout: int = 120,
    max_attempts: int = 6,
    retry_base_sec: float = 60.0,
    retry_cap_sec: float = 900.0,
) -> bytes:
    """
    Fetch with retries on rate limit / transient errors.
    GDELT often returns 429; use env GDELT_MAX_ATTEMPTS, GDELT_RETRY_BASE_SEC, GDELT_RETRY_CAP_SEC to tune.
    """
    ua = os.environ.get(
        "GDELT_USER_AGENT",
        "gdelt-fetch-to-s3/1.1 (+https://github.com/)",
    )
    for attempt in range(max_attempts):
        req = urllib.request.Request(url, headers={"User-Agent": ua})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except urllib.error.HTTPError as e:
            if e.code not in RETRYABLE_HTTP or attempt >= max_attempts - 1:
                raise
            delay = _retry_after_seconds(e, retry_cap_sec)
            if delay is None:
                delay = _backoff_seconds(attempt, retry_base_sec, retry_cap_sec)
            delay += random.uniform(5.0, 35.0)
            print(
                f"GDELT HTTP {e.code}: retry in {delay:.0f}s (attempt {attempt + 2}/{max_attempts})",
                file=sys.stderr,
                flush=True,
            )
            time.sleep(delay)
        except urllib.error.URLError as e:
            if attempt >= max_attempts - 1:
                raise
            delay = _backoff_seconds(attempt, retry_base_sec, retry_cap_sec) + random.uniform(5.0, 25.0)
            print(
                f"GDELT network error ({e.reason!s}): retry in {delay:.0f}s",
                file=sys.stderr,
                flush=True,
            )
            time.sleep(delay)
    raise AssertionError("fetch_gdelt: exhausted retries without return")  # pragma: no cover


def upload_to_s3(bucket: str, key: str, body: bytes, content_type: str = "application/json") -> None:
    client = boto3.client("s3")
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType=content_type,
    )


def validate_gdelt_payload(body: bytes) -> tuple[bool, str]:
    """Basic shape validation so we never upload HTML/error pages as .json."""
    try:
        data = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        return False, f"invalid json: {e}"
    if not isinstance(data, dict):
        return False, "top-level JSON is not an object"
    articles = data.get("articles")
    if articles is None:
        return False, "missing top-level 'articles' key"
    if not isinstance(articles, list):
        return False, f"'articles' is not a list ({type(articles).__name__})"
    return True, ""


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
        default=os.environ.get("GDELT_QUERY", DEFAULT_QUERY),
        help=(
            "GDELT search query (default: env GDELT_QUERY or positive-tone "
            "nature/world/science/family query)"
        ),
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
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=int(os.environ.get("GDELT_MAX_ATTEMPTS", "6")),
        help="HTTP retries for 429/502/503 (default: env GDELT_MAX_ATTEMPTS or 6)",
    )
    parser.add_argument(
        "--retry-base-sec",
        type=float,
        default=float(os.environ.get("GDELT_RETRY_BASE_SEC", "60")),
        help="Base backoff seconds before jitter (default: 60)",
    )
    parser.add_argument(
        "--retry-cap-sec",
        type=float,
        default=float(os.environ.get("GDELT_RETRY_CAP_SEC", "900")),
        help="Max wait per retry (default: 900)",
    )
    parser.add_argument(
        "--save-invalid-to-s3",
        action="store_true",
        help="Upload invalid API payloads under <prefix>invalid/ for debugging",
    )
    args = parser.parse_args()

    url = build_gdelt_url(args.query, args.maxrecords, args.timespan)
    print(f"Fetching: {url}")

    try:
        body = fetch_gdelt(
            url,
            max_attempts=max(1, args.max_attempts),
            retry_base_sec=args.retry_base_sec,
            retry_cap_sec=args.retry_cap_sec,
        )
    except urllib.error.HTTPError as e:
        print(f"GDELT HTTP error: {e.code} {e.reason}", file=sys.stderr)
        return 1
    except urllib.error.URLError as e:
        print(f"GDELT network error: {e.reason}", file=sys.stderr)
        return 1

    print(f"Downloaded {len(body)} bytes")
    ok, err = validate_gdelt_payload(body)
    if not ok:
        print(f"GDELT payload rejected: {err}", file=sys.stderr)
        if args.save_invalid_to_s3:
            ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            bad_key = f"{args.prefix}invalid/{ts}_gdelt_invalid_payload.txt"
            print(f"Uploading rejected payload for debug: s3://{args.bucket}/{bad_key}", file=sys.stderr)
            try:
                upload_to_s3(args.bucket, bad_key, body, content_type="text/plain")
            except Exception as e:
                print(f"Failed to upload rejected payload: {e}", file=sys.stderr)
        return 1

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
