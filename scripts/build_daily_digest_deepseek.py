#!/usr/bin/env python3
"""
Build one Markdown digest article from recent news_articles rows using DeepSeek.

Env (add to project .env — same file you load for cron):
  DEEPSEEK_API_KEY   — required
  DEEPSEEK_API_URL   — optional, default https://api.deepseek.com/chat/completions
  DEEPSEEK_MODEL     — optional, default deepseek-chat

NewsAPI.org fetch is separate: use gdelt_fetch_to_s3.py --source newsapi (NEWSAPI_KEY in .env).

Run after ingest + normalize so the DB has fresh rows:
  python scripts/create_news_tables.py   # once, creates daily_digests
  python scripts/build_daily_digest_deepseek.py --digest-date 2026-05-09

Output: upserts row in daily_digests (body_markdown includes [DIGEST_IMAGE_1]…[DIGEST_IMAGE_4] placeholders).
meta.image_slots maps slot -> article id + image URL for your frontend.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone
from typing import Any

from psycopg2.extras import Json

from pg_env import connect_pg


def _snippet_line(snippet: object) -> str:
    if not isinstance(snippet, dict):
        return ""
    for key in ("summary", "description", "content", "quote", "snippet"):
        v = snippet.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip().replace("\n", " ")[:500]
    return ""


def deepseek_chat(
    api_url: str,
    api_key: str,
    model: str,
    system: str,
    user: str,
    *,
    timeout: int = 180,
) -> str:
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": 0.6,
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        api_url,
        data=data,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"DeepSeek HTTP {e.code}: {body}") from e

    body = json.loads(raw)
    choices = body.get("choices")
    if not isinstance(choices, list) or not choices:
        raise RuntimeError(f"DeepSeek: no choices in response: {raw[:2000]}")
    msg = choices[0].get("message") if isinstance(choices[0], dict) else None
    content = msg.get("content") if isinstance(msg, dict) else None
    if not isinstance(content, str) or not content.strip():
        raise RuntimeError(f"DeepSeek: empty content: {raw[:2000]}")
    return content.strip()


def fetch_articles_for_day(
    conn,
    start: datetime,
    end: datetime,
    limit: int,
) -> list[tuple[Any, ...]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, url, title, social_image_url, gdelt_snippet, created_at
            FROM news_articles
            WHERE created_at >= %s AND created_at < %s
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (start, end, limit),
        )
        return list(cur.fetchall())


def main() -> int:
    parser = argparse.ArgumentParser(description="Daily digest via DeepSeek from news_articles")
    parser.add_argument(
        "--digest-date",
        default="",
        help="UTC calendar date YYYY-MM-DD (default: today UTC)",
    )
    parser.add_argument("--article-limit", type=int, default=40, help="Max rows to send to the model")
    parser.add_argument("--min-articles", type=int, default=4, help="Skip if fewer rows in window")
    parser.add_argument("--dry-run", action="store_true", help="Print prompt only, no API/DB write")
    args = parser.parse_args()

    api_key = (os.environ.get("DEEPSEEK_API_KEY") or "").strip()
    if not api_key and not args.dry_run:
        print("Set DEEPSEEK_API_KEY in .env (or export in shell).", file=sys.stderr)
        return 1

    api_url = (os.environ.get("DEEPSEEK_API_URL") or "https://api.deepseek.com/chat/completions").strip()
    model = (os.environ.get("DEEPSEEK_MODEL") or "deepseek-chat").strip()

    if args.digest_date.strip():
        d = date.fromisoformat(args.digest_date.strip())
    else:
        d = datetime.now(timezone.utc).date()
    start = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)

    try:
        conn = connect_pg()
    except Exception as e:
        print(f"Database connection failed: {e}", file=sys.stderr)
        return 1

    rows = fetch_articles_for_day(conn, start, end, args.article_limit)
    conn.close()

    if len(rows) < args.min_articles:
        print(f"Only {len(rows)} article(s) in window {d} UTC; need >= {args.min_articles}. Skipping.")
        return 0

    bullets: list[str] = []
    slim_meta: list[dict[str, Any]] = []
    image_candidates: list[dict[str, Any]] = []

    for rid, url, title, image_url, snippet, _created in rows:
        t = (title or "").strip() or "(no title)"
        u = (url or "").strip()
        sn = _snippet_line(snippet)
        line = f"- **{t}** — {sn}" if sn else f"- **{t}**"
        if u:
            line += f" | {u}"
        bullets.append(line)
        slim_meta.append({"id": rid, "url": u, "title": t})
        img = (image_url or "").strip()
        if img and len(image_candidates) < 4:
            image_candidates.append({"slot": len(image_candidates) + 1, "article_id": rid, "url": img})

    while len(image_candidates) < 4:
        image_candidates.append({"slot": len(image_candidates) + 1, "article_id": None, "url": None})

    system = (
        "You write concise, accurate English news digests. "
        "Use ONLY facts present in the bullet list the user provides. "
        "Do not invent events, names, numbers, or quotes. "
        "If something is unclear, omit it. "
        "Tone: calm, readable, suitable for a general audience."
    )

    placeholders = "\n".join(
        [
            "[DIGEST_IMAGE_1]",
            "[DIGEST_IMAGE_2]",
            "[DIGEST_IMAGE_3]",
            "[DIGEST_IMAGE_4]",
        ]
    )

    user_prompt = f"""Write a cohesive Markdown article for **{d.isoformat()}** (UTC) summarizing these items.

Rules:
- Start with a single H1 title line: `# ...`
- Then 3–6 short sections with `##` subheadings where helpful.
- Weave related items together; you may group by theme.
- After each major idea, readers should understand it came from the linked sources (you may mention outlet or topic, but no fabricated detail).
- Place each of the following placeholder lines **once**, on its **own paragraph** (blank line before and after), **spread through** the article (not all at the end), in this order:

{placeholders}

Source bullets:
{chr(10).join(bullets)}
"""

    if args.dry_run:
        print(user_prompt)
        print("\n--- image_slots ---\n", json.dumps(image_candidates, indent=2))
        return 0

    digest_body = deepseek_chat(api_url, api_key, model, system, user_prompt)
    title_line = f"Daily digest — {d.isoformat()}"
    first_nl = digest_body.find("\n")
    if digest_body.startswith("#"):
        title_line = digest_body[1:first_nl].strip() if first_nl != -1 else digest_body[1:].strip()

    meta: dict[str, Any] = {
        "digest_date": d.isoformat(),
        "article_count": len(rows),
        "articles": slim_meta[:200],
        "image_slots": image_candidates,
        "model": model,
    }

    conn = connect_pg()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO daily_digests (digest_date, title, body_markdown, meta)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (digest_date) DO UPDATE SET
                    title = EXCLUDED.title,
                    body_markdown = EXCLUDED.body_markdown,
                    meta = EXCLUDED.meta
                """,
                (d, title_line, digest_body, Json(meta)),
            )
        conn.commit()
    finally:
        conn.close()

    print(f"OK: saved digest for {d} ({len(digest_body)} chars).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
