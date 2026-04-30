#!/usr/bin/env python3
"""
Create PostgreSQL tables for normalized GDELT news and S3 ingest tracking.

Uses the same env vars as main.py:
  RDSHOST, RDSPORT, RDSDB, RDSUSER, RDSPASSWORD, SSLMODE, SSLROOTCERT (optional)

Run:
  python scripts/create_news_tables.py
  python scripts/create_news_tables.py --dry-run
"""

from __future__ import annotations

import argparse
import sys

DDL_STATEMENTS = [
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
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
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
