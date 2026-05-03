"""
FastAPI application: HTTP routes and in-memory "database" for learning.

Run locally:
    uvicorn main:app --reload
Then open http://127.0.0.1:8000/docs for interactive API docs.
Routes are under /api (e.g. /api/health, /api/news).
"""

import os
import logging

import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import APIRouter, FastAPI, Query

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


@api_router.get("/news")
def list_news(
    q: str | None = None,
    domain: str | None = None,
    language: str | None = None,
    source_country: str | None = None,
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
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

    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""

    sql = f"""
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
            s3_object_key
        FROM news_articles
        {where_sql}
        ORDER BY seen_at DESC NULLS LAST, id DESC
        LIMIT %s OFFSET %s
    """
    values.extend([limit, offset])

    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, values)
            rows = cur.fetchall()

    return {"count": len(rows), "items": rows}


app.include_router(api_router)
