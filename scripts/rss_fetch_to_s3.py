#!/usr/bin/env python3
"""
Fetch RSS/Atom feeds, normalize into {"articles": [...]} JSON, upload to S3.

Compatible with normalize_news_from_s3.py expected article shape.

Each article may include ``rss_label`` (short slug, e.g. science, nature) and
``rss_feed_url``; both are stored inside ``gdelt_snippet`` in PostgreSQL. Filter
via GET /api/news?rss_label=science on the API.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

import boto3

DEFAULT_BUCKET = "visorbacket"
DEFAULT_PREFIX = "gdelt/"
DEFAULT_TIMEOUT = 30
# (feed_url, rss_label) — label is stored in gdelt_snippet.rss_label (see normalize_news_from_s3).
RSS_FEEDS: tuple[tuple[str, str], ...] = (
    # Science
    ("https://www.sciencedaily.com/rss/all.xml", "science"),
    ("https://feeds.bbci.co.uk/news/science_and_environment/rss.xml", "science"),
    ("https://www.theguardian.com/science/rss", "science"),
    ("https://www.phys.org/rss-feed/", "science"),
    ("https://www.space.com/feeds/tag/science", "science"),
    # Nature / environment / climate
    ("https://www.theguardian.com/environment/rss", "nature"),
    ("https://www.smithsonianmag.com/rss/science-nature/", "nature"),
    # Health / society-adjacent (often positive-leaning institutional news)
    ("https://www.cdc.gov/media/rss/rss.xml", "health"),
    ("https://www.who.int/rss-feeds/news-english.xml", "health"),
    # Culture & film & arts
    ("https://www.theguardian.com/culture/rss", "culture"),
    ("https://www.theguardian.com/film/rss", "film"),
    ("https://feeds.bbci.co.uk/news/entertainment_and_arts/rss.xml", "culture"),
    ("https://www.theguardian.com/artanddesign/rss", "culture"),
    # World (broader wire; may hit positive_only filter more often)
    ("https://feeds.bbci.co.uk/news/world/rss.xml", "world"),
    ("https://www.theguardian.com/world/rss", "world"),
    # Uplifting / solutions-oriented
    ("https://www.goodnewsnetwork.org/feed/", "positive"),
    ("https://www.positive.news/feed/", "positive"),
)


def _iso_to_seendate(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y%m%d%H%M%S")


def _text(node: ET.Element | None) -> str | None:
    if node is None or node.text is None:
        return None
    s = node.text.strip()
    return s or None


def _strip_html(s: str) -> str:
    return re.sub(r"<[^>]+>", " ", s).strip()


def _extract_image_from_entry(entry: ET.Element) -> str | None:
    # RSS enclosure
    enc = entry.find("enclosure")
    if enc is not None:
        url = (enc.attrib.get("url") or "").strip()
        ctype = (enc.attrib.get("type") or "").lower()
        if url and ("image/" in ctype or not ctype):
            return url

    # media namespace candidates
    for tag in ("{*}content", "{*}thumbnail"):
        for node in entry.findall(tag):
            url = (node.attrib.get("url") or "").strip()
            medium = (node.attrib.get("medium") or "").lower()
            if url and (medium in ("image", "") or tag.endswith("thumbnail")):
                return url

    # Sometimes image is embedded in summary/description HTML.
    for tag in ("description", "{*}summary", "{*}content"):
        txt = _text(entry.find(tag))
        if not txt:
            continue
        m = re.search(r'<img[^>]+src=["\']([^"\']+)["\']', txt, flags=re.IGNORECASE)
        if m and m.group(1).strip():
            return m.group(1).strip()
    return None


def _fetch_og_image(url: str, timeout: int = 10) -> str | None:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "rss-image-enricher/1.0",
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
        if m and m.group(1).strip():
            return urllib.parse.urljoin(url, m.group(1).strip())
    return None


def _first_link_from_entry(entry: ET.Element) -> str | None:
    # RSS: <link>https://...</link>
    rss_link = _text(entry.find("link"))
    if rss_link:
        return rss_link
    # Atom: <link href="..."/>
    for link in entry.findall("{*}link"):
        href = (link.attrib.get("href") or "").strip()
        if href:
            return href
    return None


def _source_country_from_host(host: str | None) -> str | None:
    if not host:
        return None
    parts = host.lower().split(".")
    if not parts:
        return None
    tld = parts[-1]
    if len(tld) == 2:
        return tld.upper()
    return None


def parse_feed(
    xml_bytes: bytes,
    feed_url: str,
    rss_label: str,
    *,
    enrich_images: bool,
) -> list[dict[str, object]]:
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return []

    items: list[ET.Element] = []
    # RSS items
    items.extend(root.findall(".//item"))
    # Atom entries
    items.extend(root.findall(".//{*}entry"))

    out: list[dict[str, object]] = []
    feed_host = urllib.parse.urlparse(feed_url).hostname
    feed_title = feed_host or feed_url
    for entry in items:
        title = _text(entry.find("title")) or _text(entry.find("{*}title"))
        link = _first_link_from_entry(entry)
        if not link:
            continue
        summary = (
            _text(entry.find("description"))
            or _text(entry.find("{*}summary"))
            or _text(entry.find("{*}content"))
        )
        social_image = _extract_image_from_entry(entry)
        if (not social_image or not str(social_image).strip()) and enrich_images:
            social_image = _fetch_og_image(link)
        if summary:
            summary = _strip_html(summary)[:1200]
        entry_host = urllib.parse.urlparse(link).hostname
        source_country = _source_country_from_host(entry_host or feed_host)
        out.append(
            {
                "url": link,
                "title": title,
                "seendate": _iso_to_seendate(datetime.now(timezone.utc)),
                "domain": entry_host,
                "language": "English",
                "sourcecountry": source_country,
                "socialimage": social_image,
                "summary": summary,
                "quote": None,
                "emotionTag": None,
                "relevance": None,
                "sourceTitle": feed_title,
                "issue": None,
                "feedTitle": feed_title,
                "rss_label": rss_label.strip().lower(),
                "rss_feed_url": feed_url,
            }
        )
    return out


def fetch_url(url: str, timeout: int) -> bytes:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "rss-fetch-to-s3/1.0 (compatible; +https://github.com/) Mozilla/5.0",
            "Accept": "application/rss+xml, application/atom+xml, text/xml, application/xml, */*",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def upload_to_s3(bucket: str, key: str, body: bytes, content_type: str = "application/json") -> None:
    client = boto3.client("s3")
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)


def main() -> int:
    parser = argparse.ArgumentParser(description="RSS/Atom feeds -> S3 JSON")
    parser.add_argument("--bucket", default=os.environ.get("S3_BUCKET", DEFAULT_BUCKET))
    parser.add_argument(
        "--prefix",
        default=os.environ.get("S3_PREFIX", DEFAULT_PREFIX).rstrip("/") + "/",
        help="S3 key prefix",
    )
    parser.add_argument(
        "--feed",
        action="append",
        default=[],
        help="Feed URL (repeatable). Label defaults to 'custom'. If omitted, built-in RSS_FEEDS is used.",
    )
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help="HTTP timeout seconds")
    parser.add_argument(
        "--max-per-feed",
        type=int,
        default=15,
        help="Max entries per feed (after parse, before global dedupe)",
    )
    parser.add_argument(
        "--enrich-images",
        action="store_true",
        help="If feed has no image field, try to fetch og:image from article page (slower)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Fetch/parse only, do not upload")
    args = parser.parse_args()

    if args.feed:
        feed_jobs = [(f.strip(), "custom") for f in args.feed if f.strip()]
    else:
        feed_jobs = list(RSS_FEEDS)

    all_articles: list[dict[str, object]] = []

    for feed_url, rss_label in feed_jobs:
        print(f"Fetching feed [{rss_label}]: {feed_url}")
        try:
            body = fetch_url(feed_url, args.timeout)
        except Exception as e:
            print(f"Feed fetch failed: {feed_url}: {e}", file=sys.stderr)
            continue
        parsed = parse_feed(body, feed_url, rss_label, enrich_images=args.enrich_images)
        if args.max_per_feed > 0:
            parsed = parsed[: args.max_per_feed]
        print(f"Parsed {len(parsed)} items from {feed_url}")
        all_articles.extend(parsed)

    # Deduplicate by URL in-memory
    unique: list[dict[str, object]] = []
    seen_urls: set[str] = set()
    for a in all_articles:
        u = str(a.get("url") or "").strip()
        if not u or u in seen_urls:
            continue
        seen_urls.add(u)
        unique.append(a)

    payload = {
        "provider": "rss",
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "articles": unique,
    }
    out = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    print(f"Prepared payload with {len(unique)} unique articles ({len(out)} bytes)")

    if args.dry_run:
        print("Dry run — skipping S3 upload")
        return 0

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    key = f"{args.prefix}{ts}_rss_feeds.json"
    print(f"Uploading s3://{args.bucket}/{key}")
    try:
        upload_to_s3(args.bucket, key, out)
    except Exception as e:
        print(f"S3 upload failed: {e}", file=sys.stderr)
        return 1
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
