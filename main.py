"""
FastAPI application: HTTP routes and in-memory "database" for learning.

Run locally:
    uvicorn main:app --reload
Interactive docs: /api/docs (and /api/redoc). Example routes: /api/health, /api/news.
"""

import logging
import os
from datetime import date
from typing import Literal

import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import APIRouter, FastAPI, HTTPException, Query
from fastapi.responses import RedirectResponse

app = FastAPI(
    title="Learning API",
    description="Simple API with health check and news list from PostgreSQL.",
    version="0.2.0",
    # Same prefix as APIRouter — works when nginx only proxies /api/* to this app.
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
)
api_router = APIRouter(prefix="/api")


@app.get("/docs", include_in_schema=False)
def redirect_legacy_swagger() -> RedirectResponse:
    """Default FastAPI path /docs → /api/docs (matches public nginx /api prefix)."""
    return RedirectResponse(url="/api/docs")

logger = logging.getLogger("healthcheck.db")


def get_db_status() -> str:
    """
    Check PostgreSQL connectivity using password auth.
    Returns a short status string for the health endpoint.
    """
    host = os.getenv("RDSHOST")
    if not host:
        logger.warning("DB health check skipped: RDSHOST is not set")
        return "not_configured"

    raw_port = os.getenv("RDSPORT", "5432")
    dbname = os.getenv("RDSDB", "postgres")
    user = os.getenv("RDSUSER", "postgres")
    password = os.getenv("RDSPASSWORD")
    sslmode = os.getenv("SSLMODE", "require")
    sslrootcert = os.getenv("SSLROOTCERT")
    try:
        port = int(raw_port)
    except ValueError:
        logger.exception("DB health check failed: RDSPORT is not a valid integer", extra={"RDSPORT": raw_port})
        return "down"

    logger.info(
        "DB health check started",
        extra={
            "RDSHOST": host,
            "RDSPORT": port,
            "RDSDB": dbname,
            "RDSUSER": user,
            "RDSPASSWORD_SET": bool(password),
            "SSLMODE": sslmode,
            "SSLROOTCERT_SET": bool(sslrootcert),
        },
    )

    try:
        if not password:
            logger.warning("DB health check failed: RDSPASSWORD is not set")
            return "down"
        logger.info("Using password auth from RDSPASSWORD")

        logger.info("Opening PostgreSQL connection with SSL")
        connect_kwargs = {
            "host": host,
            "port": port,
            "dbname": dbname,
            "user": user,
            "password": password,
            "sslmode": sslmode,
            "connect_timeout": 5,
        }
        if sslrootcert:
            connect_kwargs["sslrootcert"] = sslrootcert

        conn = psycopg2.connect(
            **connect_kwargs,
        )
        try:
            logger.info("Running DB probe query: SELECT 1")
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
        finally:
            conn.close()
            logger.info("PostgreSQL connection closed")
    except Exception:
        logger.exception("DB health check failed during connection/probe")
        return "down"
    logger.info("DB health check succeeded")
    return "up"


@api_router.get("/health")
def health() -> dict[str, str]:
    """Often used by load balancers or monitoring to check the service is up."""
    return {"status": "ok", "db_status": get_db_status()}


def get_db_connection():
    connect_kwargs = {
        "host": os.getenv("RDSHOST"),
        "port": int(os.getenv("RDSPORT", "5432")),
        "dbname": os.getenv("RDSDB", "postgres"),
        "user": os.getenv("RDSUSER", "postgres"),
        "password": os.getenv("RDSPASSWORD"),
        "sslmode": os.getenv("SSLMODE", "require"),
        "connect_timeout": 10,
    }
    sslrootcert = os.getenv("SSLROOTCERT")
    if sslrootcert:
        connect_kwargs["sslrootcert"] = sslrootcert
    return psycopg2.connect(**connect_kwargs)


@api_router.get("/news/languages")
def list_news_languages() -> dict[str, list[str]]:
    """Distinct non-empty language values for filter dropdowns."""
    sql = """
        SELECT DISTINCT language FROM news_articles
        WHERE language IS NOT NULL AND TRIM(language) != ''
        ORDER BY language ASC
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    return {"languages": [row[0] for row in rows]}


@api_router.get("/news/rss-labels")
def list_rss_labels() -> dict[str, list[str]]:
    """Distinct rss_label values stored by rss_fetch_to_s3 (see gdelt_snippet.rss_label)."""
    sql = """
        SELECT DISTINCT BTRIM(gdelt_snippet->>'rss_label') AS label
        FROM news_articles
        WHERE gdelt_snippet IS NOT NULL
          AND COALESCE(BTRIM(gdelt_snippet->>'rss_label'), '') <> ''
        ORDER BY 1 ASC
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    return {"labels": [row[0] for row in rows if row[0]]}


@api_router.get("/news")
def list_news(
    q: str | None = None,
    domain: str | None = None,
    language: str | None = None,
    source_country: str | None = None,
    rss_label: str | None = Query(
        default=None,
        description=(
            "Filter rows where gdelt_snippet.rss_label equals this slug (case-insensitive). "
            "Set by RSS fetcher for ingested feeds; other sources usually have no label."
        ),
    ),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    order_by: Literal["created_at", "seen_at"] = Query(
        default="created_at",
        description=(
            "created_at: newest ingested rows first. "
            "seen_at: source 'seen' time first (when present)."
        ),
    ),
    mix_within_hour: bool = Query(
        default=True,
        description=(
            "When true, rows are grouped by calendar hour of the sort key, newest hours first; "
            "within each hour order is a stable pseudo-shuffle (same order for all clients until "
            "data changes). When false, strict chronological order by order_by then id."
        ),
    ),
) -> dict[str, object]:
    """
    List normalized news from PostgreSQL with optional filters.
    """
    where_parts: list[str] = []
    values: list[object] = []

    if q:
        where_parts.append("(title ILIKE %s OR url ILIKE %s)")
        pattern = f"%{q}%"
        values.extend([pattern, pattern])
    if domain:
        where_parts.append("domain = %s")
        values.append(domain)
    if language:
        where_parts.append("language = %s")
        values.append(language)
    if source_country:
        where_parts.append("source_country = %s")
        values.append(source_country)
    if rss_label and rss_label.strip():
        where_parts.append("lower(btrim(COALESCE(gdelt_snippet->>'rss_label',''))) = %s")
        values.append(rss_label.strip().lower())

    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

    if mix_within_hour:
        if order_by == "created_at":
            bucket_sql = "date_trunc('hour', COALESCE(deduped.created_at, deduped.seen_at))"
        else:
            bucket_sql = "date_trunc('hour', COALESCE(deduped.seen_at, deduped.created_at))"
        order_sql = (
            f"{bucket_sql} DESC NULLS LAST, "
            f"md5(COALESCE(deduped.url, '') || '|' || ({bucket_sql})::text) ASC, "
            f"deduped.id ASC"
        )
    else:
        order_sql = (
            "created_at DESC NULLS LAST, id DESC"
            if order_by == "created_at"
            else "seen_at DESC NULLS LAST, id DESC"
        )
    # Pick one row per URL (latest by the same sort key) so duplicates never reach the client.
    inner_order_tail = (
        "created_at DESC NULLS LAST, id DESC"
        if order_by == "created_at"
        else "seen_at DESC NULLS LAST, id DESC"
    )

    sql = f"""
        SELECT
            id,
            url,
            title,
            seen_at,
            created_at,
            domain,
            language,
            source_country,
            social_image_url,
            s3_bucket,
            s3_object_key,
            gdelt_snippet
        FROM (
            SELECT DISTINCT ON (url)
                id,
                url,
                title,
                seen_at,
                created_at,
                domain,
                language,
                source_country,
                social_image_url,
                s3_bucket,
                s3_object_key,
                gdelt_snippet
            FROM news_articles
            {where_sql}
            ORDER BY url ASC, {inner_order_tail}
        ) AS deduped
        ORDER BY {order_sql}
        LIMIT %s OFFSET %s
    """
    values.extend([limit, offset])

    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, values)
            rows = cur.fetchall()

    return {"count": len(rows), "items": rows}


@api_router.get("/news/detail")
def get_news_detail(url: str = Query(..., min_length=1)) -> dict[str, object]:
    """One article by exact URL, including GDELT JSON snippet (may hold an excerpt)."""
    sql = """
        SELECT
            url,
            title,
            seen_at,
            created_at,
            domain,
            language,
            source_country,
            social_image_url,
            s3_bucket,
            s3_object_key,
            gdelt_snippet
        FROM news_articles
        WHERE url = %s
        LIMIT 1
    """
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (url,))
            row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Article not found")
    return dict(row)


@api_router.get("/digests")
def list_digest_dates() -> dict[str, object]:
    """ISO dates (YYYY-MM-DD) that have a saved digest, newest first."""
    sql = """
        SELECT digest_date
        FROM daily_digests
        ORDER BY digest_date DESC
    """
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    dates: list[str] = []
    for row in rows:
        d = row.get("digest_date")
        if hasattr(d, "isoformat"):
            dates.append(d.isoformat())
        else:
            dates.append(str(d))
    return {"dates": dates}


@api_router.get("/digests/by-date")
def get_digest_by_date(
    digest_date: str = Query(
        ...,
        alias="date",
        description="UTC calendar date YYYY-MM-DD (query name: date)",
    ),
) -> dict[str, object]:
    """One daily digest (markdown body + meta including image slot mapping)."""
    try:
        d = date.fromisoformat(digest_date.strip())
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid digest_date; use YYYY-MM-DD") from None

    sql = """
        SELECT digest_date, title, body_markdown, meta, created_at
        FROM daily_digests
        WHERE digest_date = %s
        LIMIT 1
    """
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (d,))
            row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Digest not found for that date")

    out = dict(row)
    dd = out.get("digest_date")
    if hasattr(dd, "isoformat"):
        out["digest_date"] = dd.isoformat()
    ca = out.get("created_at")
    if ca is not None and hasattr(ca, "isoformat"):
        out["created_at"] = ca.isoformat()
    return out


app.include_router(api_router)
