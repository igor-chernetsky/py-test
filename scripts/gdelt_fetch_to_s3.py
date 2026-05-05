#!/usr/bin/env python3
"""
Fetch JSON from Actually Relevant API, convert to GDELT-like schema, upload to S3.

Actually Relevant public API: /api/stories (no auth expected)

Requires AWS credentials (env, profile, or EC2 instance role) with s3:PutObject
on the target bucket.

Example:
  python scripts/gdelt_fetch_to_s3.py
  python scripts/gdelt_fetch_to_s3.py --query "ukraine" --maxrecords 100
  S3_BUCKET=my-bucket python scripts/gdelt_fetch_to_s3.py

Rate limits/transient upstream failures may happen; this script retries with backoff
(HTTP 429/502/503 + short anti-bot body heuristics) and only uploads valid JSON payloads.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3

AR_API_BASE = "https://actually-relevant-api.onrender.com/api"
DEFAULT_BUCKET = "visorbacket"
DEFAULT_PREFIX = "gdelt/"
# NOTE: This is plain-text search for Actually Relevant, not GDELT boolean syntax.
DEFAULT_QUERY = "nature world science family"


def build_source_url(query: str, maxrecords: int, api_base: str) -> str:
    params = {
        "page": "1",
        "pageSize": str(maxrecords),
    }
    q = (query or "").strip()
    if q:
        params["search"] = q
    return f"{api_base.rstrip('/')}/stories?{urllib.parse.urlencode(params)}"


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


def _looks_like_rate_limited_body(body: bytes) -> bool:
    """
    GDELT may occasionally return short plain-text/html anti-abuse pages with HTTP 200.
    Treat obvious rate-limit bodies as transient and retry.
    """
    if len(body) > 4096:
        return False
    text = body.decode("utf-8", errors="ignore").lower()
    signals = (
        "too many requests",
        "rate limit",
        "429",
        "access denied",
        "temporarily unavailable",
        "service unavailable",
        "<html",
    )
    return any(s in text for s in signals)


def _iso_to_seendate(value: object) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y%m%d%H%M%S")


def _fetch_og_image(url: str, timeout: int = 10) -> str | None:
    """
    Best-effort extraction of og:image/twitter:image from article HTML.
    Keeps payload quality high when upstream API doesn't provide image fields.
    """
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "news-image-enricher/1.0",
            "Accept": "text/html,application/xhtml+xml",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            ct = (resp.headers.get("Content-Type") or "").lower()
            if "text/html" not in ct and "application/xhtml" not in ct:
                return None
            raw = resp.read(300_000)
    except Exception:
        return None

    html = raw.decode("utf-8", errors="ignore")
    patterns = [
        r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:image["\']',
        r'<meta[^>]+name=["\']twitter:image["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+name=["\']twitter:image["\']',
    ]
    for p in patterns:
        m = re.search(p, html, flags=re.IGNORECASE)
        if not m:
            continue
        candidate = m.group(1).strip()
        if candidate:
            return urllib.parse.urljoin(url, candidate)
    return None


def _story_to_article(story: dict[str, object], *, enrich_images: bool) -> dict[str, object] | None:
    source_url = story.get("sourceUrl")
    if not isinstance(source_url, str) or not source_url.strip():
        return None
    source_url = source_url.strip()
    title = story.get("title") if isinstance(story.get("title"), str) else story.get("sourceTitle")
    seendate = _iso_to_seendate(story.get("datePublished")) or _iso_to_seendate(story.get("dateCrawled"))
    hostname = urllib.parse.urlparse(source_url).hostname
    feed = story.get("feed")
    feed_title = feed.get("title") if isinstance(feed, dict) and isinstance(feed.get("title"), str) else None
    issue_name = None
    if isinstance(story.get("issue"), dict) and isinstance(story["issue"].get("name"), str):
        issue_name = story["issue"]["name"]
    elif isinstance(feed, dict) and isinstance(feed.get("issue"), dict) and isinstance(feed["issue"].get("name"), str):
        issue_name = feed["issue"]["name"]

    social_image = story.get("socialImage") or story.get("imageUrl") or story.get("thumbnail")
    if (not social_image or not str(social_image).strip()) and enrich_images:
        social_image = _fetch_og_image(source_url)

    out: dict[str, object] = {
        "url": source_url,
        "title": title.strip() if isinstance(title, str) and title.strip() else None,
        "seendate": seendate,
        "domain": hostname,
        "language": "English",
        "sourcecountry": None,
        "socialimage": social_image,
        "summary": story.get("summary"),
        "quote": story.get("quote"),
        "emotionTag": story.get("emotionTag"),
        "relevance": story.get("relevance"),
        "sourceTitle": story.get("sourceTitle"),
        "issue": issue_name,
        "feedTitle": feed_title,
    }
    return out


def transform_source_payload(raw_body: bytes, *, enrich_images: bool) -> tuple[dict[str, object] | None, str]:
    """
    Convert Actually Relevant `/api/stories` response into expected `{\"articles\": [...]}`.
    """
    text = raw_body.decode("utf-8", errors="replace")
    if "<!doctype html" in text.lower() or "<html" in text.lower():
        return (
            None,
            "received HTML instead of JSON (likely frontend host). "
            "Use --api-base https://actually-relevant-api.onrender.com/api",
        )
    try:
        src = json.loads(text)
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        return None, f"invalid json: {e}"

    rows: list[object]
    if isinstance(src, dict) and isinstance(src.get("data"), list):
        rows = src["data"]
    elif isinstance(src, list):
        rows = src
    else:
        return None, "unexpected source payload shape (expected object with data[] or array)"

    articles: list[dict[str, object]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        article = _story_to_article(row, enrich_images=enrich_images)
        if article is not None:
            articles.append(article)

    return {
        "provider": "actually_relevant",
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "articles": articles,
    }, ""


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
                body = resp.read()
                if _looks_like_rate_limited_body(body):
                    if attempt >= max_attempts - 1:
                        return body
                    delay = _backoff_seconds(attempt, retry_base_sec, retry_cap_sec) + random.uniform(5.0, 35.0)
                    print(
                        f"GDELT returned non-JSON rate-limit body: retry in {delay:.0f}s "
                        f"(attempt {attempt + 2}/{max_attempts})",
                        file=sys.stderr,
                        flush=True,
                    )
                    time.sleep(delay)
                    continue
                return body
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
            "Plain-text search query for Actually Relevant stories endpoint "
            "(default: env GDELT_QUERY or 'nature world science family'). "
            "Do not use GDELT boolean syntax here."
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
        help="Deprecated (kept for backwards-compatible cron args); ignored for Actually Relevant API",
    )
    parser.add_argument(
        "--api-base",
        default=os.environ.get("AR_API_BASE", AR_API_BASE),
        help=(
            "Actually Relevant API base URL "
            "(default: env AR_API_BASE or https://actually-relevant-api.onrender.com/api)"
        ),
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
    parser.add_argument(
        "--enrich-images",
        action="store_true",
        help="When source API has no image, try extracting og:image from article pages (slower)",
    )
    args = parser.parse_args()

    url = build_source_url(args.query, args.maxrecords, args.api_base)
    print(f"Fetching: {url}")

    try:
        raw_body = fetch_gdelt(
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

    print(f"Downloaded {len(raw_body)} bytes")
    converted, convert_err = transform_source_payload(raw_body, enrich_images=args.enrich_images)
    if converted is None:
        print(f"Source payload rejected: {convert_err}", file=sys.stderr)
        if args.save_invalid_to_s3:
            ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            bad_key = f"{args.prefix}invalid/{ts}_source_invalid_payload.txt"
            print(f"Uploading rejected payload for debug: s3://{args.bucket}/{bad_key}", file=sys.stderr)
            try:
                upload_to_s3(args.bucket, bad_key, raw_body, content_type="text/plain")
            except Exception as e:
                print(f"Failed to upload rejected payload: {e}", file=sys.stderr)
        return 1

    body = json.dumps(converted, ensure_ascii=False).encode("utf-8")
    ok, err = validate_gdelt_payload(body)
    if not ok:
        print(f"Converted payload rejected: {err}", file=sys.stderr)
        if args.save_invalid_to_s3:
            ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            bad_key = f"{args.prefix}invalid/{ts}_converted_invalid_payload.json"
            print(f"Uploading rejected payload for debug: s3://{args.bucket}/{bad_key}", file=sys.stderr)
            try:
                upload_to_s3(args.bucket, bad_key, body, content_type="application/json")
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
