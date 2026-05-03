#!/usr/bin/env python3
"""
Emit topic_embeddings.json for main.py (same model/dim as normalize_news_from_s3.py).

Run locally after pip install -r requirements.txt (needs sentence-transformers once).
Writes JSON next to main.py by default.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

# Phrases must match visor TOPIC_FILTERS; slugs are the `topic=` API values.
TOPICS: dict[str, str] = {
    "climate": "climate change environment energy sustainability",
    "technology": "technology software artificial intelligence computing",
    "health": "health medicine public health disease healthcare",
}


def main() -> None:
    import numpy as np
    from sentence_transformers import SentenceTransformer

    root = Path(__file__).resolve().parent.parent
    out = root / "topic_embeddings.json"
    name = os.environ.get("EMBEDDING_MODEL", "all-MiniLM-L6-v2")
    model = SentenceTransformer(name)
    expected = int(os.environ.get("EMBEDDING_DIM", "384"))

    payload: dict[str, list[float]] = {}
    for slug, phrase in TOPICS.items():
        raw = model.encode(phrase[:5000], show_progress_bar=False)
        v = np.asarray(raw, dtype=np.float64).reshape(-1)
        n = float(np.linalg.norm(v))
        if n > 0:
            v = v / n
        if v.size != expected:
            raise SystemExit(f"{slug}: dim {v.size} != {expected}")
        payload[slug] = [float(x) for x in v.tolist()]

    out.write_text(json.dumps(payload, indent=0) + "\n", encoding="utf-8")
    print(f"Wrote {out} ({len(payload)} topics, dim {expected})")


if __name__ == "__main__":
    main()
