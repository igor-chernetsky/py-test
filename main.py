"""
FastAPI application: HTTP routes and in-memory "database" for learning.

Run locally:
    uvicorn main:app --reload
Then open http://127.0.0.1:8000/docs for interactive API docs.
"""

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


@app.get("/")
def read_root() -> dict[str, str]:
    """Simple GET — no path parameters or body."""
    return {"message": "Hello — try GET /items or open /docs"}


@app.get("/health")
def health() -> dict[str, str]:
    """Often used by load balancers or monitoring to check the service is up."""
    return {"status": "ok"}

@app.get("/info")
def info() -> dict[str, str]:
    """Return information about the items."""
    return list(map(lambda x: f"Item {x.name}: {x.description}", _items))

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
