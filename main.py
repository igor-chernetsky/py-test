"""
FastAPI application: HTTP routes and in-memory "database" for learning.

Run locally:
    uvicorn main:app --reload
Then open http://127.0.0.1:8000/docs for interactive API docs.
"""

import os
import logging

import psycopg2
from fastapi import FastAPI, HTTPException

from schemas import Item, ItemCreate

app = FastAPI(
    title="Learning API",
    description="A tiny FastAPI example for learning Python and REST basics.",
    version="0.1.0",
)

# In-memory store (resets when the server restarts — fine for practice).
_items: list[Item] = []
_next_id: int = 1
logger = logging.getLogger("healthcheck.db")


def get_db_status() -> str:
    """
    Check PostgreSQL connectivity using password auth.
    Returns a short status string for the health endpoint.
    """
    print(f"=== DEBUG: RDSHOST from os.getenv = {os.getenv('RDSHOST')} ===")
    print(f"=== DEBUG: RDSHOST from os.environ = {os.environ.get('RDSHOST')} ===")
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


@app.get("/")
def read_root() -> dict[str, str]:
    """Simple GET — no path parameters or body."""
    return {"message": "Hello — try GET /items or open /docs"}


@app.get("/health")
def health() -> dict[str, str]:
    """Often used by load balancers or monitoring to check the service is up."""
    return {"status": "ok", "db_status": get_db_status()}

@app.get("/info")
def info() -> list[str]:
    """Return information about the items."""
    return [f"Item {item.name}: {item.description}" for item in _items]

@app.get("/items", response_model=list[Item])
def list_items() -> list[Item]:
    """Return all items. `response_model` tells FastAPI how to serialize the JSON."""
    return _items


@app.get("/items/{item_id}", response_model=Item)
def get_item(item_id: int) -> Item:
    """Path parameter `item_id` is parsed as int; 404 if not found."""
    for item in _items:
        if item.id == item_id:
            return item
    raise HTTPException(status_code=404, detail="Item not found")


@app.post("/items", response_model=Item, status_code=201)
def create_item(body: ItemCreate) -> Item:
    """
    Request body is validated against ItemCreate automatically.
    Returns 201 Created with the new item (including assigned id).
    """
    global _next_id
    new_item = Item(id=_next_id, name=body.name, description=body.description)
    _next_id += 1
    _items.append(new_item)
    return new_item
