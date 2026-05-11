#!/usr/bin/env python3
"""
Emit topic_embeddings.json for main.py (same model/dim as normalize_news_from_s3.py).

Each topic is represented by several short phrases; we L2-normalize per phrase,
average the vectors, then L2-normalize the mean. One long keyword blob tends to
dilute the embedding and *reduces* recall in topic-filtered search.

Run locally after pip install -r requirements.txt (needs sentence-transformers once).
Writes JSON next to main.py by default. The API only reads that JSON — you do not need
this script on the server if topic_embeddings.json is already deployed.
"""

from __future__ import annotations

import json
import math
import os
from pathlib import Path

import numpy as np

# Slugs must match visor TOPIC_FILTERS; values are the `topic=` API values.
# Several focused sentences per topic work better than one huge bag of words.
TOPICS: dict[str, list[str]] = {
    "nature": [
        "Wild animals wildlife ecology biodiversity conservation and protected habitats.",
        "Forests wetlands rivers lakes oceans coasts marine life and seabird ecology.",
        "Marine mammals seals whales dolphins field biology telemetry and wildlife surveys.",
        "Plants fungi insects pollinators botany zoology ethology and national parks.",
        "Climate adaptation rewilding nature reserves and outdoor environmental stewardship.",
    ],
    "world": [
        "International news geopolitics diplomacy conflicts borders and global security.",
        "Countries governments elections policy economy trade sanctions and migration.",
        "Cities society humanitarian crises refugees United Nations and regional summits.",
        "Culture sports religion tourism travel justice crime courts and human interest.",
    ],
    "science": [
        "Scientific research discoveries peer-reviewed studies laboratory experiments and data.",
        "Biology medicine neuroscience physiology genetics immunology and drug discovery.",
        "Brain circuits neurons sensory systems itch pain skin and neuroscience breakthroughs.",
        "Climate science ocean circulation atmosphere earth system geophysics and sea level.",
        "Volcanoes atmospheric chemistry methane ozone aerosols and planetary atmospheres.",
        "Physics chemistry astronomy space telescopes materials nanotechnology and engineering.",
        "Microbiome gut bacteria aging liver metabolism biotechnology CRISPR and genomics.",
        "Statistics machine learning robotics AI microscopy imaging and STEM innovation.",
    ],
}


def _l2_normalize(vec: np.ndarray) -> np.ndarray:
    n = float(np.linalg.norm(vec))
    if n <= 0:
        return vec
    return vec / n


def main() -> None:
    try:
        from sentence_transformers import SentenceTransformer
    except ModuleNotFoundError:
        root = Path(__file__).resolve().parent.parent
        existing = root / "topic_embeddings.json"
        extra = ""
        if existing.is_file():
            extra = (
                f"\n\nYou already have {existing} — the FastAPI app uses that file and does "
                "NOT need sentence-transformers. You can stop here unless you are regenerating "
                "vectors after changing topic phrases or the embedding model.\n"
            )
        raise SystemExit(
            "sentence-transformers is not installed in this environment.\n"
            "  python -m pip install sentence-transformers\n"
            "or:  python -m pip install -r requirements.txt\n"
            "Or run this script on your laptop and copy topic_embeddings.json here.\n"
            f"{extra}"
        ) from None

    root = Path(__file__).resolve().parent.parent
    out = root / "topic_embeddings.json"
    name = os.environ.get("EMBEDDING_MODEL", "all-MiniLM-L6-v2")
    model = SentenceTransformer(name)
    expected = int(os.environ.get("EMBEDDING_DIM", "384"))

    payload: dict[str, list[float]] = {}
    for slug, phrases in TOPICS.items():
        texts = [p.strip()[:5000] for p in phrases if p and p.strip()]
        if not texts:
            raise SystemExit(f"{slug}: no phrases configured")

        mat = model.encode(
            texts,
            show_progress_bar=False,
            normalize_embeddings=True,
        )
        arr = np.asarray(mat, dtype=np.float64)
        if arr.ndim == 1:
            arr = arr.reshape(1, -1)
        if arr.shape[0] != len(texts):
            raise SystemExit(f"{slug}: encode rows {arr.shape[0]} != phrases {len(texts)}")
        if arr.shape[1] != expected:
            raise SystemExit(f"{slug}: dim {arr.shape[1]} != {expected}")

        centroid = _l2_normalize(arr.mean(axis=0))
        flat = [float(x) for x in centroid.tolist()]
        payload[slug] = flat

    out.write_text(json.dumps(payload, indent=0) + "\n", encoding="utf-8")
    print(f"Wrote {out} ({len(payload)} topics, dim {expected}, multi-phrase centroids)")


if __name__ == "__main__":
    main()
