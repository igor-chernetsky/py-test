"""Shared PostgreSQL connection from the same env vars as the FastAPI app."""

from __future__ import annotations

import os
from typing import Any

import psycopg2


def pg_connect_kwargs() -> dict[str, Any]:
    host = os.getenv("RDSHOST")
    if not host:
        raise RuntimeError("RDSHOST is not set")

    password = os.getenv("RDSPASSWORD")
    if not password:
        raise RuntimeError("RDSPASSWORD is not set")

    raw_port = os.getenv("RDSPORT", "5432")
    port = int(raw_port)

    kwargs: dict[str, Any] = {
        "host": host,
        "port": port,
        "dbname": os.getenv("RDSDB", "postgres"),
        "user": os.getenv("RDSUSER", "postgres"),
        "password": password,
        "sslmode": os.getenv("SSLMODE", "require"),
        "connect_timeout": 30,
    }
    sslrootcert = os.getenv("SSLROOTCERT")
    if sslrootcert:
        kwargs["sslrootcert"] = sslrootcert
    return kwargs


def connect_pg():
    return psycopg2.connect(**pg_connect_kwargs())
