"""
Manages the OUTPUT glossary: a growing, deduplicated record of historically
specific terms discovered by Stage 2b across the corpus.

IMPORTANT — direction: the model does not consult a glossary as input.
Stage 2 stays purely visual, with no glossary injected into its prompt.
This file is built automatically as a byproduct of the pipeline run: every
time Stage 2b identifies a specific term (e.g., "huipil," "redingote,"
"segbureh") that is supported by both the article corpus and the image,
that term is appended here — with which source backed it, a short note on
why, and which images it has been seen in. No manual curation required.

SCOPE — same boundary as before: only naming/material vocabulary belongs
here (what an object or garment IS). Racial/casta classification labels
should never end up in this file, since Stage 2b is instructed not to look
those up — if one somehow appears, treat it as a bug to investigate, not a
valid discovery.
"""

import json
import os
from datetime import datetime, timezone

from config import GLOSSARY_OUTPUT_PATH


def append_discovered_terms(image_id: str, identified_elements: list) -> None:
    """Takes Stage 2b's identified_elements for one image and merges any
    successfully identified terms (specific_term non-empty) into the
    cumulative output glossary, deduplicating by the term itself."""
    if not identified_elements:
        return

    glossary = _load_existing()

    for element in identified_elements:
        term = (element.get("specific_term") or "").strip()
        if not term:
            continue  # Stage 2b found no supported match for this item — nothing to record

        key = term.lower()
        if key in glossary:
            if image_id not in glossary[key]["seen_in_images"]:
                glossary[key]["seen_in_images"].append(image_id)
        else:
            glossary[key] = {
                "term": term,
                "language": element.get("term_language", ""),
                "generic_description": element.get("generic_description", ""),
                "supporting_source": element.get("supporting_source", ""),
                "confidence_note": element.get("confidence_note", ""),
                "seen_in_images": [image_id],
                "first_added": datetime.now(timezone.utc).isoformat(),
            }

    _save(glossary)


def _load_existing() -> dict:
    if not os.path.exists(GLOSSARY_OUTPUT_PATH):
        return {}
    with open(GLOSSARY_OUTPUT_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def _save(glossary: dict) -> None:
    os.makedirs(os.path.dirname(GLOSSARY_OUTPUT_PATH), exist_ok=True)
    with open(GLOSSARY_OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(glossary, f, ensure_ascii=False, indent=2)