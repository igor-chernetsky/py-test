"""
FastAPI application: HTTP routes and in-memory "database" for learning.

Run locally:
    uvicorn main:app --reload
Then open http://127.0.0.1:8000/docs for interactive API docs.
Routes are under /api (e.g. /api/health, /api/news).
"""

import json
import logging
import os
from pathlib import Path
from typing import Literal

import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import APIRouter, FastAPI, HTTPException, Query

app = FastAPI(
    title="Learning API",
    description="Simple API with health check and news list from PostgreSQL.",
    version="0.2.0",
)
api_router = APIRouter(prefix="/api")

logger = logging.getLogger("healthcheck.db")


def _topic_embeddings_path() -> Path:
    """
    JSON path: TOPIC_EMBEDDINGS_PATH env (absolute or relative to cwd), else next to this file.
    """
    override = os.getenv("TOPIC_EMBEDDINGS_PATH", "").strip()
    if override:
        return Path(override).expanduser().resolve()
    return (Path(__file__).resolve().parent / "topic_embeddings.json").resolve()


def _read_topic_vector_literals(path: Path) -> dict[str, str]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    out: dict[str, str] = {}
    for slug, arr in raw.items():
        if not isinstance(arr, list):
            continue
        out[str(slug)] = "[" + ",".join(str(float(x)) for x in arr) + "]"
    return out


_topic_file_mtime: float | None = None
_topic_vector_literals_cache: dict[str, str] = {}


def get_topic_vector_literals() -> dict[str, str]:
    """
    Cached read of topic_embeddings.json; reloads when the file's mtime changes.
    Avoids empty topic maps when the file is added after the process starts or the service cwd differs.
    """
    global _topic_file_mtime, _topic_vector_literals_cache
    path = _topic_embeddings_path()
    if not path.is_file():
        if _topic_vector_literals_cache:
            logger.warning("topic embeddings file disappeared, clearing cache: %s", path)
            _topic_vector_literals_cache = {}
        elif _topic_file_mtime is None:
            logger.warning(
                "topic_embeddings.json not found at %s (set TOPIC_EMBEDDINGS_PATH if it lives elsewhere)",
                path,
            )
        _topic_file_mtime = -1.0
        return {}
    mtime = path.stat().st_mtime
    if _topic_vector_literals_cache and mtime == _topic_file_mtime:
        return _topic_vector_literals_cache
    try:
        _topic_vector_literals_cache = _read_topic_vector_literals(path)
        _topic_file_mtime = mtime
        logger.info(
            "Loaded topic embeddings: %s (%d topic(s))",
            path,
            len(_topic_vector_literals_cache),
        )
    except Exception:
        logger.exception("Failed to parse topic embeddings: %s", path)
        _topic_vector_literals_cache = {}
    return _topic_vector_literals_cache


# Phrases the older visor sent as `topic=` before switching to slugs.
_LEGACY_TOPIC_PHRASE_TO_SLUG: dict[str, str] = {
    "climate change environment energy sustainability": "climate",
    "technology software artificial intelligence computing": "technology",
    "health medicine public health disease healthcare": "health",
}


def _resolve_topic_vector_literal(topic_param: str) -> str | None:
    literals = get_topic_vector_literals()
    if not literals:
        return None
    key = " ".join(topic_param.strip().lower().replace("+", " ").split())
    if not key:
        return None
    if key in literals:
        return literals[key]
    slug = _LEGACY_TOPIC_PHRASE_TO_SLUG.get(key)
    if slug and slug in literals:
        return literals[slug]
    return None


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


@api_router.get("/news")
def list_news(
    q: str | None = None,
    domain: str | None = None,
    language: str | None = None,
    source_country: str | None = None,
    topic: str | None = Query(
        default=None,
        description=(
            "Topic slug (climate, technology, health): rank by pgvector similarity using "
            "precomputed embeddings from topic_embeddings.json. Rows need non-null embedding."
        ),
    ),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    order_by: Literal["created_at", "seen_at"] = Query(
        default="created_at",
        description=(
            "created_at: newest ingested rows first (matches typical SQL ORDER BY created_at). "
            "seen_at: GDELT 'seen' time first. "
            "Ignored for final ordering when `topic` is set (results ordered by vector similarity)."
        ),
    ),
) -> dict[str, object]:
    """
    List normalized news from PostgreSQL with optional filters.
    """
    topic_clean = (topic or "").strip()
    use_vector = bool(topic_clean)
    vec_lit: str | None = None
    if use_vector:
        vec_lit = _resolve_topic_vector_literal(topic_clean)
        if vec_lit is None:
            if not get_topic_vector_literals():
                p = _topic_embeddings_path()
                raise HTTPException(
                    status_code=503,
                    detail=(
                        "Topic search is not configured: no topic embeddings loaded. "
                        f"Expected file at {p} (override with TOPIC_EMBEDDINGS_PATH). "
                        "Restart the API after adding the file."
                    ),
                )
            raise HTTPException(
                status_code=400,
                detail=(
                    "Unknown topic. Use one of: climate, technology, health "
                    "(or redeploy with matching topic_embeddings.json)."
                ),
            )

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
    if use_vector:
        where_parts.append("embedding IS NOT NULL")

    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

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

    if use_vector and vec_lit is not None:
        sql = f"""
            SELECT
                d.id,
                d.url,
                d.title,
                d.seen_at,
                d.created_at,
                d.domain,
                d.language,
                d.source_country,
                d.social_image_url,
                d.s3_bucket,
                d.s3_object_key,
                d.gdelt_snippet
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
                    gdelt_snippet,
                    embedding
                FROM news_articles
                {where_sql}
                ORDER BY url ASC, {inner_order_tail}
            ) AS d
            CROSS JOIN (SELECT %s::vector AS qv) AS qvec
            WHERE d.embedding IS NOT NULL
            ORDER BY d.embedding <=> qvec.qv ASC NULLS LAST
            LIMIT %s OFFSET %s
        """
        values.extend([vec_lit, limit, offset])
    else:
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


app.include_router(api_router)
