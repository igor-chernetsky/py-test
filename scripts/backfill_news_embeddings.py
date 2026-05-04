#!/usr/bin/env python3
"""
Set news_articles.embedding for rows where it is NULL (same model/text rules as normalize_news_from_s3).

Uses title + first snippet field from gdelt_snippet JSONB via build_embedding_text().
Requires sentence-transformers in this venv (not needed for the FastAPI app) plus DB env
(RDSHOST, RDSPASSWORD, etc.):  python -m pip install sentence-transformers

Run from project root:
  python scripts/backfill_news_embeddings.py
  python scripts/backfill_news_embeddings.py --limit 100
  python scripts/backfill_news_embeddings.py --dry-run
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

import numpy as np
import psycopg2
from psycopg2.extras import RealDictCursor

# Same directory as this file (scripts/)
_scripts_dir = Path(__file__).resolve().parent
if str(_scripts_dir) not in sys.path:
    sys.path.insert(0, str(_scripts_dir))

from normalize_news_from_s3 import (  # noqa: E402
    build_embedding_text,
    embedding_dim_expected,
    get_embed_model,
)
from pg_env import connect_pg  # noqa: E402


def _require_sentence_transformers() -> None:
    try:
        import sentence_transformers  # noqa: F401
    except ModuleNotFoundError:
        raise SystemExit(
            "sentence-transformers is not installed. This script encodes text; the API does not need it.\n"
            "  python -m pip install sentence-transformers\n"
            "or:  python -m pip install -r requirements.txt\n"
            "Then re-run this script."
        ) from None


def texts_to_vector_literals(texts: list[str], model) -> list[str]:
    """L2-normalized vectors as pgvector literals; matches export_topic_embeddings style (ST encode without normalize=)."""
    expected = embedding_dim_expected()
    raw = model.encode(texts, show_progress_bar=False)
    arr = np.asarray(raw, dtype=np.float64)
    if arr.ndim == 1:
        arr = arr.reshape(1, -1)
    out: list[str] = []
    for i in range(arr.shape[0]):
        row = arr[i]
        s = float(np.sqrt(np.dot(row, row)))
        if s > 0:
            row = row / s
        if row.shape[0] != expected:
            raise RuntimeError(
                f"Embedding dim {row.shape[0]} != {expected} (set EMBEDDING_DIM / recreate vector column)"
            )
        out.append("[" + ",".join(str(float(x)) for x in row.tolist()) + "]")
    return out


def fetch_null_batch(cur, *, after_id: int, batch_size: int) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT id, title, gdelt_snippet
        FROM news_articles
        WHERE embedding IS NULL AND id > %s
        ORDER BY id ASC
        LIMIT %s
        """,
        (after_id, batch_size),
    )
    return list(cur.fetchall())


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill news_articles.embedding where NULL")
    parser.add_argument("--batch-size", type=int, default=64, help="Rows per encode + UPDATE batch")
    parser.add_argument("--limit", type=int, default=0, help="Max rows to update (0 = all NULL rows)")
    parser.add_argument("--dry-run", action="store_true", help="Show counts / first batch only, no writes")
    args = parser.parse_args()

    try:
        conn = connect_pg()
    except Exception as e:
        print(f"Database connection failed: {e}", file=sys.stderr)
        return 1

    with conn.cursor() as count_cur:
        count_cur.execute("SELECT count(*) FROM news_articles WHERE embedding IS NULL")
        total_null = count_cur.fetchone()[0]
    print(f"Rows with embedding IS NULL: {total_null}")

    if total_null == 0:
        conn.close()
        return 0

    if args.dry_run:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            rows = fetch_null_batch(cur, after_id=0, batch_size=min(5, args.batch_size))
        for r in rows:
            snippet = r.get("gdelt_snippet")
            text = build_embedding_text(r.get("title"), snippet if isinstance(snippet, dict) else None)
            print(f"  id={r['id']} text_len={len(text)} preview={text[:120]!r}...")
        conn.close()
        print("Dry run: no updates.")
        return 0

    _require_sentence_transformers()
    print(
        "Loading embedding model (downloads ~100MB on first run; CPU encode can take a few minutes). "
        "Wait until you see 'Updated …' lines…",
        flush=True,
    )
    model = get_embed_model()
    print("Model ready. Encoding and updating…", flush=True)
    updated = 0
    last_id = 0
    target_cap = args.limit if args.limit > 0 else None

    try:
        while True:
            if target_cap is not None and updated >= target_cap:
                break
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                take = args.batch_size
                if target_cap is not None:
                    take = min(take, target_cap - updated)
                if take <= 0:
                    break
                rows = fetch_null_batch(cur, after_id=last_id, batch_size=take)
            if not rows:
                break

            texts: list[str] = []
            ids: list[int] = []
            for r in rows:
                snippet = r.get("gdelt_snippet")
                sn = snippet if isinstance(snippet, dict) else None
                t = build_embedding_text(r.get("title"), sn)
                texts.append(t[:5000] if t else "news article")
                ids.append(int(r["id"]))

            literals = texts_to_vector_literals(texts, model)
            last_id = ids[-1]

            with conn.cursor() as ucur:
                for row_id, lit in zip(ids, literals, strict=True):
                    ucur.execute(
                        """
                        UPDATE news_articles
                        SET embedding = %s::vector, updated_at = now()
                        WHERE id = %s AND embedding IS NULL
                        """,
                        (lit, row_id),
                    )
            conn.commit()
            updated += len(ids)
            print(f"Updated {updated} row(s) (last id={last_id})...")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print(f"Done. Backfilled {updated} row(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
