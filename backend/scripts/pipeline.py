"""
Orchestrates the full analysis of one artwork across Stages 1-4 and the
closing metacognition, for a given served VLM.

- Stage 1 stays purely visual: no external resources at all.
- Stage 2 also stays purely visual: generic descriptions only, no glossary
  consulted.
- Stage 2b looks up Stage 2's generic descriptions against the article
  corpus, and any successfully identified term is appended to a growing
  OUTPUT glossary (data/discovered_glossary.json) — built automatically as
  a byproduct, never hand-curated.
- Stages 3 and 4 retrieve supporting material from the document corpus,
  informed by Stage 2b's specific terms. Stage 4's two opposing-reading
  calls (4a and 4b) are grounded in that material but are NOT restricted
  to "visual evidence only" — the model is asked to interpret, combining
  the retrieved material with its own historical/iconographic judgment,
  so that whatever bias or default reading it carries becomes visible
  instead of staying hidden behind a literalness requirement.
"""

import json

from prompts import (
    GENERAL_INSTRUCTIONS,
    STAGE1_PROMPT_TEMPLATE, STAGE1_SCHEMA,
    STAGE2_PROMPT_TEMPLATE, STAGE2_SCHEMA,
    STAGE2_CSV_CONFLICTS_PROMPT_TEMPLATE, STAGE2_CSV_CONFLICTS_SCHEMA,
    STAGE2B_PROMPT_TEMPLATE, STAGE2B_SCHEMA,
    STAGE3_PROMPT_TEMPLATE, STAGE3_SCHEMA,
    STAGE4A_CALL1_TEMPLATE, STAGE4A_CALL2_TEMPLATE, STAGE4A_CALL3_TEMPLATE,
    STAGE4B_BRANCH_A_CALL1, STAGE4B_BRANCH_A_CALL2,
    STAGE4B_BRANCH_B_CALL1, STAGE4B_BRANCH_B_CALL2,
    STAGE4B_CALL3_TEMPLATE,
    STAGE4_FRICTION_SCHEMA,
    CLOSING_TEMPLATE, CLOSING_SCHEMA,
)
from vlm_client import call_vlm_json, call_vlm_text
from retrieval import retrieve, find_title_matches, format_for_prompt
from glossary import append_discovered_terms

# Fixed bilingual hints for Stage 4 retrieval queries. Bilingual because the
# corpus is mostly Spanish/English (plus a Portuguese document) — BGE-M3
# handles cross-lingual retrieval reasonably well on its own, but giving it
# both phrasings directly improves recall against the Spanish-language
# share of the corpus without any extra engineering.
_STAGE4A_TOPIC_HINT = (
    "gesture posture agency subordination corporeality Afro-descendant colonial portraiture "
    "gesto postura agencia subordinacion corporalidad afrodescendiente retrato colonial"
)
_STAGE4B_CASTA_TOPIC_HINT = (
    "casta painting social mobility marginality miscegenation colonial "
    "pintura de castas movilidad social marginalidad mestizaje colonial"
)
_STAGE4B_RELIGIOUS_TOPIC_HINT = (
    "racial hierarchy religious art colonial subversion Afro-descendant saints "
    "jerarquia racial arte religioso colonial subversion santos afrodescendientes"
)


def _build_query(*parts) -> str:
    return " ".join(p for p in parts if p)


def _retrieve_with_title_match(query: str, title: str, author: str = "") -> list:
    """Combines ordinary semantic retrieval with a guaranteed lookup for
    any article whose filename names the same subject as this artwork's
    title/author (see retrieval.find_title_matches) — a proper noun (a
    specific person's or saint's name) can be exactly what an article is
    about without dominating a thematic semantic query enough to rank in
    the top-k, so this checks by name directly rather than relying on
    embedding similarity alone. Title-match chunks are listed first;
    duplicates (same source + text turning up in both lists) are
    collapsed to one entry."""
    title_chunks = find_title_matches(title, author)
    semantic_chunks = retrieve(query)
    seen = {}
    for c in title_chunks + semantic_chunks:
        seen[(c["source"], c["text"])] = c
    return list(seen.values())


def _detect_genre_branch(stage2: dict) -> str:
    """Heuristic genre detection from Stage 2's theme/category fields.
    Adjust this once your controlled vocabulary for Theme/Category is
    finalized — right now it just looks for the word 'casta'."""
    block = stage2.get("stage_2_iconographic_material_culture", {})
    haystack = " ".join(
        str(block.get(k, "")) for k in ("theme", "category", "classification")
    ).lower()
    return "casta" if "casta" in haystack else "religious_secular"


def _collect_material_descriptions(stage2: dict) -> list:
    """Pulls every generic clothing/accessory/object description out of
    Stage 2's output, to use as retrieval queries in Stage 2b."""
    block = stage2.get("stage_2_iconographic_material_culture", {})
    descriptions = []
    for attr in block.get("afrodescendant_attributes", []):
        if attr.get("clothing_style"):
            descriptions.append(attr["clothing_style"])
        descriptions.extend(attr.get("clothing_materials", []))
        descriptions.extend(attr.get("accessories", []))
    for obj in block.get("objects_in_scene", []):
        if obj.get("object"):
            descriptions.append(obj["object"])
    return [d for d in descriptions if d]


def _run_stage2b(model_config: dict, image_path: str, stage2_json: dict) -> dict:
    descriptions = _collect_material_descriptions(stage2_json)

    # One retrieval per generic description, deduplicated by (source, text)
    # so the same passage isn't repeated if several items match it.
    seen = {}
    for desc in descriptions:
        for chunk in retrieve(desc):
            seen[(chunk["source"], chunk["text"])] = chunk
    chunks = list(seen.values())
    reference_material = format_for_prompt(chunks)

    stage2b = call_vlm_json(
        model_config, image_path, GENERAL_INSTRUCTIONS,
        STAGE2B_PROMPT_TEMPLATE.format(
            previous=json.dumps(stage2_json, ensure_ascii=False),
            reference_material=reference_material,
        ),
        STAGE2B_SCHEMA, "stage2b", max_tokens=4000,
    )
    return stage2b


def _run_stage4a(model_config: dict, image_path: str, fig: dict, theme_hint: str,
                 title: str, author: str) -> dict:
    # Each call_vlm_text/call_vlm_json call below is an independent API
    # request with no memory of Stage 1's figure numbering — passing only
    # figure_id (e.g. "fig_08") left the model to re-derive which figure
    # that was from scratch on every single call, purely by re-scanning
    # the image in reading order. On busy scenes (many figures) this
    # silently drifted: a later call could land on a completely different
    # person than Stage 1 actually meant. position_in_space and
    # physiognomy_description are Stage 1's own identifying description of
    # this exact figure, passed through so every call is anchored to the
    # same individual instead of re-identifying them independently.
    figure_id = fig["figure_id"]
    position_in_space = fig.get("position_in_space", "")
    physiognomy_description = fig.get("physiognomy_description", "")

    query = _build_query(_STAGE4A_TOPIC_HINT, theme_hint)
    chunks = _retrieve_with_title_match(query, title, author)
    reference_material = format_for_prompt(chunks)

    reading_a = call_vlm_text(
        model_config, image_path, GENERAL_INSTRUCTIONS,
        STAGE4A_CALL1_TEMPLATE.format(
            figure_id=figure_id, position_in_space=position_in_space,
            physiognomy_description=physiognomy_description, reference_material=reference_material,
        ),
        max_tokens=800,
    )
    reading_b = call_vlm_text(
        model_config, image_path, GENERAL_INSTRUCTIONS,
        STAGE4A_CALL2_TEMPLATE.format(
            figure_id=figure_id, position_in_space=position_in_space,
            physiognomy_description=physiognomy_description, reference_material=reference_material,
        ),
        max_tokens=800,
    )
    friction = call_vlm_json(
        model_config, image_path, GENERAL_INSTRUCTIONS,
        STAGE4A_CALL3_TEMPLATE.format(
            figure_id=figure_id, position_in_space=position_in_space,
            physiognomy_description=physiognomy_description, reading_a=reading_a, reading_b=reading_b,
        ),
        STAGE4_FRICTION_SCHEMA, "stage4a_friction", max_tokens=1000,
    )
    return {
        "figure_id": figure_id,
        "subordination_reading": reading_a,
        "agency_reading": reading_b,
        "friction": friction,
        "consulted_sources": [c["source"] for c in chunks],
    }


def _run_stage4b(model_config: dict, image_path: str, fig: dict, genre_branch: str, theme_hint: str,
                 title: str, author: str) -> dict:
    # See _run_stage4a's comment: figure_id alone isn't enough to keep an
    # independent API call anchored to the same figure Stage 1 identified.
    figure_id = fig["figure_id"]
    position_in_space = fig.get("position_in_space", "")
    physiognomy_description = fig.get("physiognomy_description", "")

    if genre_branch == "casta":
        call1_prompt, call2_prompt = STAGE4B_BRANCH_A_CALL1, STAGE4B_BRANCH_A_CALL2
        topic_hint = _STAGE4B_CASTA_TOPIC_HINT
    else:
        call1_prompt, call2_prompt = STAGE4B_BRANCH_B_CALL1, STAGE4B_BRANCH_B_CALL2
        topic_hint = _STAGE4B_RELIGIOUS_TOPIC_HINT

    query = _build_query(topic_hint, theme_hint)
    chunks = _retrieve_with_title_match(query, title, author)
    reference_material = format_for_prompt(chunks)

    reading_1 = call_vlm_text(
        model_config, image_path, GENERAL_INSTRUCTIONS,
        call1_prompt.format(reference_material=reference_material),
        max_tokens=800,
    )
    reading_2 = call_vlm_text(
        model_config, image_path, GENERAL_INSTRUCTIONS,
        call2_prompt.format(reference_material=reference_material),
        max_tokens=800,
    )
    friction = call_vlm_json(
        model_config, image_path, GENERAL_INSTRUCTIONS,
        STAGE4B_CALL3_TEMPLATE.format(
            figure_id=figure_id, position_in_space=position_in_space,
            physiognomy_description=physiognomy_description, reading_a=reading_1, reading_b=reading_2,
        ),
        STAGE4_FRICTION_SCHEMA, "stage4b_friction", max_tokens=1000,
    )
    return {
        "figure_id": figure_id,
        "genre_branch": genre_branch,
        "reading_1": reading_1,
        "reading_2": reading_2,
        "friction": friction,
        "consulted_sources": [c["source"] for c in chunks],
    }


def analyze_artwork(model_config: dict, image_path: str, csv_row: dict) -> dict:
    result = {}

    # Stage 1 — purely visual, no external resources at all
    stage1 = call_vlm_json(
        model_config, image_path, GENERAL_INSTRUCTIONS,
        STAGE1_PROMPT_TEMPLATE.format(csv_row=json.dumps(csv_row, ensure_ascii=False)),
        STAGE1_SCHEMA, "stage1", max_tokens=4000,
    )
    result.update(stage1)

    # Stage 2 — purely visual, no external resources at all
    stage2 = call_vlm_json(
        model_config, image_path, GENERAL_INSTRUCTIONS,
        STAGE2_PROMPT_TEMPLATE.format(previous=json.dumps(result, ensure_ascii=False)),
        STAGE2_SCHEMA, "stage2", max_tokens=4000,
    )
    result.update(stage2)

    # Stage 2 CSV cross-reference — split into its own call (see prompts.py):
    # the free-text conflict notes were the main driver pushing Stage 2's
    # already-large nested schema past the point where Claude's tool-use
    # reliably holds its structure on content-dense images.
    stage2_conflicts = call_vlm_json(
        model_config, image_path, GENERAL_INSTRUCTIONS,
        STAGE2_CSV_CONFLICTS_PROMPT_TEMPLATE.format(
            previous=json.dumps(result, ensure_ascii=False),
            csv_row=json.dumps(csv_row, ensure_ascii=False),
        ),
        STAGE2_CSV_CONFLICTS_SCHEMA, "stage2_csv_conflicts", max_tokens=2000,
    )
    stage2["stage_2_iconographic_material_culture"]["csv_conflicts"] = \
        stage2_conflicts["csv_conflicts"]

    # Stage 2b — corpus-assisted identification of Stage 2's generic descriptions
    stage2b = _run_stage2b(model_config, image_path, stage2)
    result.update(stage2b)

    # Append any successfully identified terms to the cumulative output
    # glossary (data/discovered_glossary.json) — this is a byproduct of the
    # run, not something consulted as input.
    image_id = str(csv_row.get("Image_ID", ""))
    append_discovered_terms(
        image_id,
        stage2b.get("stage_2b_material_identification", {}).get("identified_elements", []),
    )

    specific_terms = " ".join(
        e["specific_term"]
        for e in stage2b.get("stage_2b_material_identification", {}).get("identified_elements", [])
        if e.get("specific_term")
    )

    # Reusable theme hint for retrieval queries in Stages 3 and 4 — now
    # enriched with any specific terms Stage 2b found, not just the
    # generic Theme/Symbols fields.
    theme_hint = _build_query(
        str(csv_row.get("Theme", "")),
        " ".join(stage2.get("stage_2_iconographic_material_culture", {}).get("theme", [])),
        " ".join(stage2.get("stage_2_iconographic_material_culture", {}).get("symbols", [])),
        specific_terms,
    )

    # Stage 3 — narrative, with retrieval from the article corpus
    title = str(csv_row.get("Title", ""))
    author = str(csv_row.get("Author", ""))
    stage3_chunks = _retrieve_with_title_match(_build_query(theme_hint, title), title, author)
    stage3_reference = format_for_prompt(stage3_chunks)

    stage3 = call_vlm_json(
        model_config, image_path, GENERAL_INSTRUCTIONS,
        STAGE3_PROMPT_TEMPLATE.format(
            previous=json.dumps(result, ensure_ascii=False),
            reference_material=stage3_reference,
        ),
        STAGE3_SCHEMA, "stage3", max_tokens=2000,
    )
    stage3["stage_3_narrative"]["consulted_sources"] = [c["source"] for c in stage3_chunks]
    result.update(stage3)

    # Stage 4 — mayeutics, with retrieval, once per Afro-descendant figure
    figures = [
        f for f in result["stage_1_pre_iconographic"]["figures"]
        if f["perceived_as_afrodescendant"]
    ]
    genre_branch = _detect_genre_branch(stage2)

    result["stage_4a_results"] = []
    result["stage_4b_results"] = []

    for fig in figures:
        result["stage_4a_results"].append(
            _run_stage4a(model_config, image_path, fig, theme_hint, title, author)
        )
        result["stage_4b_results"].append(
            _run_stage4b(model_config, image_path, fig, genre_branch, theme_hint, title, author)
        )

    # Closing — visual metacognition, once per artwork
    all_frictions = {
        "stage_4a": [r["friction"] for r in result["stage_4a_results"]],
        "stage_4b": [r["friction"] for r in result["stage_4b_results"]],
    }
    closing = call_vlm_json(
        model_config, image_path, GENERAL_INSTRUCTIONS,
        CLOSING_TEMPLATE.format(frictions=json.dumps(all_frictions, ensure_ascii=False)),
        CLOSING_SCHEMA, "closing", max_tokens=1000,
    )
    result.update(closing)

    return result