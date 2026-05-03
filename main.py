"""
FastAPI application: HTTP routes and in-memory "database" for learning.

Run locally:
    uvicorn main:app --reload
Then open http://127.0.0.1:8000/docs for interactive API docs.
Routes are under /api (e.g. /api/health, /api/news).
"""

import logging
import os
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


_embed_model = None


def embed_text_to_vector_literal(text: str) -> str:
    """
    Encode text with the same model / dim as scripts/normalize_news_from_s3.py
    for pgvector similarity against news_articles.embedding.
    """
    global _embed_model
    if _embed_model is None:
        from sentence_transformers import SentenceTransformer

        name = os.getenv("EMBEDDING_MODEL", "all-MiniLM-L6-v2")
        _embed_model = SentenceTransformer(name)
    vec = _embed_model.encode(text[:5000], normalize=True, show_progress_bar=False)
    dim = int(vec.shape[0]) if hasattr(vec, "shape") else len(vec)
    expected = int(os.getenv("EMBEDDING_DIM", "384"))
    if dim != expected:
        raise RuntimeError(
            f"Model embedding dim is {dim} but EMBEDDING_DIM / DB column expect {expected}."
        )
    return "[" + ",".join(str(float(x)) for x in vec.tolist()) + "]"


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
            "If set, rank deduplicated rows by embedding cosine distance to this phrase "
            "(requires sentence-transformers and non-null embedding rows)."
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
        try:
            vec_lit = embed_text_to_vector_literal(topic_clean)
        except Exception as e:
            logger.exception("topic embedding failed")
            raise HTTPException(
                status_code=503,
                detail=f"Embedding unavailable: {e!s}",
            ) from e

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
