"""Pydantic models: define the shape of JSON request/response data."""

from pydantic import BaseModel, Field


class ItemCreate(BaseModel):
    """Body for POST /items — only fields the client may send."""

    name: str = Field(..., min_length=1, max_length=100)
    description: str | None = None


class Item(ItemCreate):
    """Item as returned by the API — includes server-assigned id."""

    id: int
