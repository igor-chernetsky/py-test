#!/usr/bin/env python3
"""
Create PostgreSQL tables for normalized GDELT news and S3 ingest tracking.

Uses the same env vars as main.py:
  RDSHOST, RDSPORT, RDSDB, RDSUSER, RDSPASSWORD, SSLMODE, SSLROOTCERT (optional)

Creates pgvector extension and `news_articles.embedding vector(384)` for
`sentence-transformers` default model `all-MiniLM-L6-v2` (override with EMBEDDING_MODEL / EMBEDDING_DIM).

On RDS, `CREATE EXTENSION vector` may require an allowed extension list / admin;
if it fails, enable `vector` in the parameter group or run the extension SQL as admin.

Run:
  python scripts/create_news_tables.py
  python scripts/create_news_tables.py --dry-run
"""

from __future__ import annotations

import argparse
import sys

DDL_STATEMENTS = [
    """
    CREATE EXTENSION IF NOT EXISTS vector;
    """,
    """
    CREATE TABLE IF NOT EXISTS news_s3_ingest (
        id BIGSERIAL PRIMARY KEY,
        s3_bucket TEXT NOT NULL,
        s3_key TEXT NOT NULL,
        etag TEXT,
        article_count INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'completed',
        error_message TEXT,
        started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        completed_at TIMESTAMPTZ,
        UNIQUE (s3_bucket, s3_key)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS news_articles (
        id BIGSERIAL PRIMARY KEY,
        url TEXT NOT NULL UNIQUE,
        title TEXT,
        seen_at TIMESTAMPTZ,
        domain TEXT,
        language TEXT,
        source_country TEXT,
        social_image_url TEXT,
        s3_bucket TEXT NOT NULL,
        s3_object_key TEXT NOT NULL,
        gdelt_snippet JSONB,
        embedding vector(384),
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """,
    """
    ALTER TABLE news_articles
        ADD COLUMN IF NOT EXISTS embedding vector(384);
    """,
    """
    CREATE INDEX IF NOT EXISTS news_articles_seen_at_idx
        ON news_articles (seen_at DESC NULLS LAST);
    """,
    """
    CREATE INDEX IF NOT EXISTS news_articles_domain_idx
        ON news_articles (domain);
    """,
    """
    CREATE INDEX IF NOT EXISTS news_articles_s3_key_idx
        ON news_articles (s3_bucket, s3_object_key);
    """,
    """
    CREATE INDEX IF NOT EXISTS news_articles_title_lower_btrim_idx
        ON news_articles (lower(btrim(COALESCE(title, ''))));
    """,
    # Remove duplicate rows (same normalized title + language + domain) before unique index can apply.
    """
    DELETE FROM news_articles
    WHERE id IN (
        SELECT id FROM (
            SELECT id,
                   ROW_NUMBER() OVER (
                       PARTITION BY lower(btrim(COALESCE(title, ''))),
                                    COALESCE(language, ''),
                                    COALESCE(domain, '')
                       ORDER BY created_at DESC NULLS LAST, id DESC
                   ) AS rn
            FROM news_articles
            WHERE length(btrim(COALESCE(title, ''))) > 0
        ) sub
        WHERE rn > 1
    );
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS news_articles_title_lang_domain_uidx
        ON news_articles (
            lower(btrim(COALESCE(title, ''))),
            COALESCE(language, ''),
            COALESCE(domain, '')
        )
        WHERE length(btrim(COALESCE(title, ''))) > 0;
    """,
]


def main() -> int:
    parser = argparse.ArgumentParser(description="Create news + ingest tables in PostgreSQL")
    parser.add_argument("--dry-run", action="store_true", help="Print SQL only, do not connect")
    args = parser.parse_args()

    if args.dry_run:
        for stmt in DDL_STATEMENTS:
            print(stmt.strip())
        return 0

    try:
        from pg_env import connect_pg
    except ImportError:
        print("Run from project root: python scripts/create_news_tables.py", file=sys.stderr)
        return 1

    conn = connect_pg()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            for stmt in DDL_STATEMENTS:
                cur.execute(stmt)
        print("Tables created or already exist (news_s3_ingest, news_articles).")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
