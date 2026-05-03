#!/usr/bin/env python3
"""
List new GDELT JSON objects in S3, normalize each article, upsert into PostgreSQL.

Expects files produced by gdelt_fetch_to_s3.py (JSON with an "articles" array).

Env:
  Same DB vars as main.py: RDSHOST, RDSPORT, RDSDB, RDSUSER, RDSPASSWORD, SSLMODE, SSLROOTCERT
  S3_BUCKET (default: visorbacket), S3_PREFIX (default: gdelt/)
  EMBEDDING_MODEL (default: all-MiniLM-L6-v2) — sentence-transformers; output dim must match DB (384).
  EMBEDDING_DIM (default: 384) — must match column `embedding vector(384)` and the model output size.

Requires: CREATE EXTENSION vector; column news_articles.embedding (see create_news_tables.py).

Run after create_news_tables.py:
  python scripts/create_news_tables.py
  python scripts/normalize_news_from_s3.py
  python scripts/normalize_news_from_s3.py --limit 10
  python scripts/normalize_news_from_s3.py --no-embed   # skip sentence-transformers (no vector fill)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any

import boto3
from psycopg2.extras import Json

from pg_env import connect_pg

DEFAULT_BUCKET = "visorbacket"
DEFAULT_PREFIX = "gdelt/"

_EMBED_MODEL = None


def get_embed_model():
    """Lazy-load SentenceTransformer (heavy); only when embeddings enabled."""
    global _EMBED_MODEL
    if _EMBED_MODEL is None:
        from sentence_transformers import SentenceTransformer

        name = os.environ.get("EMBEDDING_MODEL", "all-MiniLM-L6-v2")
        _EMBED_MODEL = SentenceTransformer(name)
    return _EMBED_MODEL


def embedding_dim_expected() -> int:
    return int(os.environ.get("EMBEDDING_DIM", "384"))


def build_embedding_text(title: str | None, snippet: dict[str, Any] | None) -> str:
    parts: list[str] = []
    if title and str(title).strip():
        parts.append(str(title).strip())
    if snippet:
        for key in (
            "description",
            "Description",
            "snippet",
            "Snippet",
            "excerpt",
            "Excerpt",
            "summary",
            "Summary",
            "quote",
            "Quote",
            "context",
            "Context",
        ):
            v = snippet.get(key)
            if isinstance(v, str) and v.strip():
                parts.append(v.strip()[:800])
                break
    text = " ".join(parts).strip()
    return text if text else "news article"


def vector_literal_from_text(text: str, no_embed: bool) -> str | None:
    if no_embed:
        return None
    model = get_embed_model()
    vec = model.encode(text[:5000], normalize=True, show_progress_bar=False)
    dim = int(vec.shape[0]) if hasattr(vec, "shape") else len(vec)
    expected = embedding_dim_expected()
    if dim != expected:
        raise RuntimeError(
            f"Model embedding dim is {dim} but EMBEDDING_DIM / DB column expect {expected}. "
            "Set EMBEDDING_MODEL / EMBEDDING_DIM to match create_news_tables.py vector(N)."
        )
    return "[" + ",".join(str(float(x)) for x in vec.tolist()) + "]"


def title_taken_by_other_url(cur, title: str | None, url: str) -> bool:
    """True if another row already has this title (btrim, case-insensitive)."""
    t = (title or "").strip()
    if not t:
        return False
    u = url.strip()
    cur.execute(
        """
        SELECT 1 FROM news_articles
        WHERE lower(btrim(COALESCE(title, ''))) = lower(btrim(COALESCE(%s, '')))
          AND length(btrim(COALESCE(%s, ''))) > 0
          AND url <> %s
        LIMIT 1
        """,
        (t, t, u),
    )
    return cur.fetchone() is not None


def _pick(d: dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return None


def parse_gdelt_seendate(value: Any) -> datetime | None:
    if value is None:
        return None
    s = str(value).strip()
    if len(s) < 14:
        return None
    digits = s[:14]
    if not digits.isdigit():
        return None
    try:
        return datetime.strptime(digits, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def normalize_article(row: dict[str, Any], bucket: str, key: str) -> tuple[Any, ...]:
    url = _pick(row, "url", "URL")
    if not url:
        return tuple()
    title = _pick(row, "title", "Title")
    seen_at = parse_gdelt_seendate(_pick(row, "seendate", "seenDate", "seen_date"))
    domain = _pick(row, "domain", "Domain")
    language = _pick(row, "language", "Language")
    country = _pick(row, "sourcecountry", "sourceCountry", "SourceCountry")
    image = _pick(row, "socialimage", "socialImage", "SocialImage")
    snippet = {k: v for k, v in row.items() if k not in ("url", "URL")} or None
    return (
        str(url).strip(),
        str(title).strip() if title else None,
        seen_at,
        str(domain).strip() if domain else None,
        str(language).strip() if language else None,
        str(country).strip() if country else None,
        str(image).strip() if image else None,
        bucket,
        key,
        Json(snippet) if snippet else None,
    )


UPSERT_ARTICLE = """
INSERT INTO news_articles (
    url, title, seen_at, domain, language, source_country, social_image_url,
    s3_bucket, s3_object_key, gdelt_snippet, embedding
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::vector
)
ON CONFLICT (url) DO UPDATE SET
    title = EXCLUDED.title,
    seen_at = EXCLUDED.seen_at,
    domain = EXCLUDED.domain,
    language = EXCLUDED.language,
    source_country = EXCLUDED.source_country,
    social_image_url = EXCLUDED.social_image_url,
    s3_bucket = EXCLUDED.s3_bucket,
    s3_object_key = EXCLUDED.s3_object_key,
    gdelt_snippet = EXCLUDED.gdelt_snippet,
    embedding = COALESCE(EXCLUDED.embedding, news_articles.embedding),
    updated_at = now();
"""


def upsert_article_sql(no_embed: bool) -> str:
    if no_embed:
        return """
INSERT INTO news_articles (
    url, title, seen_at, domain, language, source_country, social_image_url,
    s3_bucket, s3_object_key, gdelt_snippet
) VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
ON CONFLICT (url) DO UPDATE SET
    title = EXCLUDED.title,
    seen_at = EXCLUDED.seen_at,
    domain = EXCLUDED.domain,
    language = EXCLUDED.language,
    source_country = EXCLUDED.source_country,
    social_image_url = EXCLUDED.social_image_url,
    s3_bucket = EXCLUDED.s3_bucket,
    s3_object_key = EXCLUDED.s3_object_key,
    gdelt_snippet = EXCLUDED.gdelt_snippet,
    updated_at = now();
"""
    return UPSERT_ARTICLE


UPSERT_INGEST = """
INSERT INTO news_s3_ingest (
    s3_bucket, s3_key, etag, article_count, status, error_message, completed_at
) VALUES (%s, %s, %s, %s, %s, %s, now())
ON CONFLICT (s3_bucket, s3_key) DO UPDATE SET
    etag = EXCLUDED.etag,
    article_count = EXCLUDED.article_count,
    status = EXCLUDED.status,
    error_message = EXCLUDED.error_message,
    completed_at = now();
"""


def list_json_keys(s3, bucket: str, prefix: str) -> list[dict[str, Any]]:
    keys: list[dict[str, Any]] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents") or []:
            k = obj.get("Key") or ""
            if k.endswith(".json"):
                keys.append({"Key": k, "ETag": obj.get("ETag")})
    return keys


def load_completed_keys(conn, bucket: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT s3_key FROM news_s3_ingest
            WHERE s3_bucket = %s AND status = 'completed'
            """,
            (bucket,),
        )
        return {row[0] for row in cur.fetchall()}


def process_object(
    s3,
    conn,
    bucket: str,
    key: str,
    etag: str | None,
    *,
    no_embed: bool,
) -> tuple[int, str | None]:
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    try:
        data = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        return 0, f"invalid json: {e}"

    articles = data.get("articles")
    if articles is None:
        return 0, "missing top-level 'articles' key (not GDELT ArtList JSON?)"
    if not isinstance(articles, list):
        return 0, f"'articles' must be a list, got {type(articles).__name__}"

    sql = upsert_article_sql(no_embed)
    count = 0
    with conn.cursor() as cur:
        for row in articles:
            if not isinstance(row, dict):
                continue
            tup = normalize_article(row, bucket, key)
            if not tup:
                continue
            url = tup[0]
            title = tup[1]
            snippet_dict: dict[str, Any] | None = (
                {k: v for k, v in row.items() if k not in ("url", "URL")} or None
            )

            if title_taken_by_other_url(cur, title, url):
                continue

            if no_embed:
                cur.execute(sql, tup)
            else:
                emb_text = build_embedding_text(title, snippet_dict)
                vec_lit = vector_literal_from_text(emb_text, no_embed=False)
                cur.execute(sql, (*tup, vec_lit))
            count += 1
        cur.execute(
            UPSERT_INGEST,
            (bucket, key, etag, count, "completed", None),
        )
    return count, None


def mark_failed(conn, bucket: str, key: str, etag: str | None, err: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            UPSERT_INGEST,
            (bucket, key, etag, 0, "failed", err[:2000]),
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Normalize GDELT JSON from S3 into PostgreSQL")
    parser.add_argument("--bucket", default=os.environ.get("S3_BUCKET", DEFAULT_BUCKET))
    parser.add_argument(
        "--prefix",
        default=os.environ.get("S3_PREFIX", DEFAULT_PREFIX),
        help="S3 prefix for gdelt json files",
    )
    parser.add_argument("--limit", type=int, default=0, help="Max new objects to process (0 = all)")
    parser.add_argument("--dry-run", action="store_true", help="List objects only, no DB/S3 body reads")
    parser.add_argument(
        "--no-embed",
        action="store_true",
        help="Do not load sentence-transformers or write embedding (column must allow NULL or omit in SQL)",
    )
    args = parser.parse_args()

    prefix = args.prefix.rstrip("/") + "/"
    bucket = args.bucket

    s3 = boto3.client("s3")
    keys = list_json_keys(s3, bucket, prefix)
    print(f"Found {len(keys)} .json objects under s3://{bucket}/{prefix}")

    if args.dry_run:
        for item in keys[:20]:
            print(" ", item["Key"])
        if len(keys) > 20:
            print(f"  ... and {len(keys) - 20} more")
        return 0

    try:
        conn = connect_pg()
    except Exception as e:
        print(f"Database connection failed: {e}", file=sys.stderr)
        return 1

    completed = load_completed_keys(conn, bucket)
    conn.commit()

    pending = [item for item in keys if item["Key"] not in completed]
    if args.limit:
        pending = pending[: args.limit]

    processed = 0
    try:
        for item in pending:
            key = item["Key"]
            etag = item.get("ETag")
            if etag and isinstance(etag, str):
                etag = etag.strip('"')

            try:
                n, err = process_object(s3, conn, bucket, key, etag, no_embed=args.no_embed)
                if err:
                    conn.rollback()
                    mark_failed(conn, bucket, key, etag, err)
                conn.commit()
                print(f"OK s3://{bucket}/{key} -> {n} articles" if not err else f"SKIP s3://{bucket}/{key}: {err}")
                processed += 1
            except Exception as e:
                conn.rollback()
                try:
                    mark_failed(conn, bucket, key, etag, str(e))
                    conn.commit()
                except Exception:
                    conn.rollback()
                print(f"FAIL s3://{bucket}/{key}: {e}", file=sys.stderr)
                processed += 1
    finally:
        conn.close()

    print(f"Finished. New files processed this run: {processed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
