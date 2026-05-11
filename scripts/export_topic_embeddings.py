#!/usr/bin/env python3
"""
Emit topic_embeddings.json for main.py (same model/dim as normalize_news_from_s3.py).

Run locally after pip install -r requirements.txt (needs sentence-transformers once).
Writes JSON next to main.py by default. The API only reads that JSON — you do not need
this script on the server if topic_embeddings.json is already deployed.
"""

from __future__ import annotations

import json
import math
import os
from pathlib import Path

# Phrases must match visor TOPIC_FILTERS; slugs are the `topic=` API values.
TOPICS: dict[str, str] = {
    "nature": (
        "nature wildlife wild animals biodiversity conservation ecology ecosystems habitats "
        "forests woodlands grasslands wetlands rivers lakes oceans coasts marine shoreline "
        "marine biology ocean life seals sea lions walruses whales dolphins porpoises otters "
        "birds ornithology reptiles amphibians insects pollinators plants botany trees fungi "
        "national parks reserves field research tracking telemetry tagging collars diving "
        "underwater surveys veterinary wildlife animal physiology heart rate metabolism "
        "migration breeding nesting endangered species rewilding climate nature reserves "
        "environmental science earth natural world outdoor wilderness zoology ethology"
    ),
    "world": (
        "world international global countries geopolitics diplomacy conflict economy "
        "policy migration society country tourism travel cities urban rural development "
        "humanitarian aid trade sanctions elections government law courts crime justice "
        "culture religion sports Olympics refugees borders United Nations summits"
    ),
    "science": (
        "science research discovery scientific study experiment peer reviewed journal "
        "laboratory medicine biology physics chemistry astronomy space oceanography "
        "geophysics climate science earth system atmospheric science innovation "
        "genetics neuroscience immunology epidemiology statistics data modeling hypothesis "
        "engineering materials nanotechnology robotics AI machine learning publication"
    ),
}


def _flatten_encode_output(raw: object) -> list[float]:
    """Turn SentenceTransformer.encode() output into a 1-D float list (no numpy)."""
    if hasattr(raw, "tolist"):
        raw = raw.tolist()
    if not isinstance(raw, list):
        raise SystemExit(f"Unexpected encode output type: {type(raw)}")
    if not raw:
        return []
    if isinstance(raw[0], (list, tuple)):
        if len(raw) != 1:
            raise SystemExit(f"Expected one embedding row, got {len(raw)}")
        return [float(x) for x in raw[0]]
    return [float(x) for x in raw]


def _l2_normalize(vec: list[float]) -> list[float]:
    s = sum(x * x for x in vec)
    n = math.sqrt(s) if s > 0 else 0.0
    if n <= 0:
        return vec
    return [x / n for x in vec]


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
    for slug, phrase in TOPICS.items():
        raw = model.encode(phrase[:5000], show_progress_bar=False)
        flat = _l2_normalize(_flatten_encode_output(raw))
        if len(flat) != expected:
            raise SystemExit(f"{slug}: dim {len(flat)} != {expected}")
        payload[slug] = flat

    out.write_text(json.dumps(payload, indent=0) + "\n", encoding="utf-8")
    print(f"Wrote {out} ({len(payload)} topics, dim {expected})")


if __name__ == "__main__":
    main()
