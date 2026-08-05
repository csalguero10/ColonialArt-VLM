"""
Prompt templates and JSON Schemas for each stage of the visual maieutics
methodology. These mirror the spec files (00-05) but as Python objects
that the pipeline can use directly with vLLM's structured-output feature.

Keep this file in sync with the .md spec files if you revise the
methodology — the .md files are the human-readable documentation, this
file is what actually gets sent to the model.
"""

GENERAL_INSTRUCTIONS = """You are an assistant specialized in the iconographic analysis of colonial \
Latin American art (17th-19th centuries), working as a support tool for an art historian. \
Your task is to describe and catalog visual evidence with precision, not to produce a final \
historical interpretation — that decision belongs to the historian.

Reading order: scan the composition first by depth (foreground to background), then left to \
right within each plane. If the work has multiple vignettes, scan them top-left to bottom-right.

Figure numbering: assign each human figure a unique identifier the first time you detect it \
(fig_01, fig_02, ...), following the reading order above. Keep the same identifier for the same \
figure across all stages — never reassign or reorder identifiers.

Description vs. interpretation: describe only what is visually verifiable. Do not infer \
emotional states, intentions, or social roles unless explicitly asked to in a later stage. Avoid \
vocabulary that already presupposes an interpretation (e.g., "slave", "servant") unless the task \
explicitly calls for that level of analysis.

The field "perceived_as_afrodescendant" reflects your visual perception, not an objective racial \
classification. Base it on pictorial conventions (skin tone as painted, physical features as \
rendered by the artist), not on assumptions external to the image.

CSV cross-referencing: you will receive a CSV record as partial ground truth. It was annotated by \
art history students and may contain inaccuracies, especially in physiognomy, objects, and \
symbols fields. If your visual reading matches the CSV, or the CSV has no data for a field, do \
NOT generate an entry in csv_conflicts. Generate an entry only when your reading explicitly \
contradicts the CSV value, including the field, the CSV value, your reading, and a brief note.

Missing values: missing text = "", missing numbers = null, empty lists = []. Never invent a value \
to fill a field.

Output: respond with ONLY a valid JSON object matching the provided schema. No explanatory text \
before or after, no markdown code blocks, no comments inside the JSON. Text values must be in \
English, except literal inscriptions transcribed from the artwork and any historically specific \
terminology identified via corpus lookup (e.g., "huipil"), which must stay in its original \
language/spelling rather than being translated. If you need to quote a title, phrase, or \
inscription inside a text value, use single quotes ('like this') rather than double quotes — \
double quotes inside a JSON string must otherwise be escaped and are a common source of \
formatting errors.

When reference material from the scholarly corpus is provided alongside a question, treat it as \
supporting context that may help you identify iconographic types, historical conventions, or \
social dynamics relevant to colonial art. It does not override direct visual evidence: if the \
reference material conflicts with what is actually visible in the image, the image takes \
precedence, and you should note the discrepancy explicitly rather than silently following the \
text."""


# ---------------------------------------------------------------------------
# Stage 1 — Pre-iconographic + CSV cross-reference
# ---------------------------------------------------------------------------

STAGE1_PROMPT_TEMPLATE = """Here is the CSV record for this artwork:

{csv_row}

Task: load the relevant CSV fields into csv_metadata. Then identify every human figure in the \
image, number them in reading order, determine which are perceived as Afro-descendant, and for \
each one record its spatial position, relative scale, basic physical state (without interpreting \
its meaning yet), and a basic physiognomic description. Describe the artwork's technical \
resources (lighting, color palette, composition notes). Compare your reading against the CSV's \
Face_position and Physiognomy fields and record any real contradiction in csv_conflicts."""

STAGE1_SCHEMA = {
    "type": "object",
    "properties": {
        "csv_metadata": {
            "type": "object",
            "properties": {
                "image_id": {"type": "string"},
                "title": {"type": "string"},
                "author": {"type": "string"},
                "medium": {"type": "string"},
                "date": {"type": "string"},
                "location": {"type": "string"},
                "provenance": {"type": "string"},
                "theme_csv": {"type": "string"},
                "category_csv": {"type": "string"},
                "classification_csv": {"type": "string"},
                "descriptors_csv": {"type": "string"},
                "symbols_csv": {"type": "string"},
                "features_csv": {"type": "string"},
                "scene_csv": {"type": "string"},
                "face_position_csv": {"type": "string"},
                "objects_csv": {"type": "string"},
                "physiognomy_csv": {"type": "string"},
                "donor_csv": {"type": "string"},
                "inscription_csv": {"type": "string"},
            },
            "required": ["image_id", "title", "author", "date"],
        },
        "stage_1_pre_iconographic": {
            "type": "object",
            "properties": {
                "people_present": {"type": "boolean"},
                "total_figures": {"type": "integer"},
                "afrodescendant_present": {"type": "boolean"},
                "number_of_afrodescendants": {"type": "integer"},
                "figures": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "figure_id": {"type": "string"},
                            "perceived_as_afrodescendant": {"type": "boolean"},
                            "position_in_space": {"type": "string"},
                            "relative_scale": {"type": "string"},
                            "pose_state_basic": {"type": "string"},
                            "physiognomy_description": {"type": "string"},
                        },
                        "required": [
                            "figure_id", "perceived_as_afrodescendant", "position_in_space",
                            "relative_scale", "pose_state_basic", "physiognomy_description",
                        ],
                    },
                },
                "technical_resources": {
                    "type": "object",
                    "properties": {
                        "lighting": {"type": "string"},
                        "color_palette": {"type": "string"},
                        "composition_notes": {"type": "string"},
                    },
                    "required": ["lighting", "color_palette", "composition_notes"],
                },
                "csv_conflicts": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "field": {"type": "string"},
                            "csv_value": {"type": "string"},
                            "model_value": {"type": "string"},
                            "note": {"type": "string"},
                        },
                        "required": ["field", "csv_value", "model_value", "note"],
                    },
                },
            },
            "required": [
                "people_present", "total_figures", "afrodescendant_present",
                "number_of_afrodescendants", "figures", "technical_resources", "csv_conflicts",
            ],
        },
    },
    "required": ["csv_metadata", "stage_1_pre_iconographic"],
}


# ---------------------------------------------------------------------------
# Stage 2 — Iconographic: material culture
# ---------------------------------------------------------------------------

STAGE2_PROMPT_TEMPLATE = """Here is the result of Stage 1 for this artwork (use the same figure_id \
values, do not reassign them):

{previous}

Task: for each figure marked perceived_as_afrodescendant, describe the materials, textures, and \
style of their clothing and accessories (avoid evaluative binaries like "rich" vs. "poor" \
clothing) using plain, generic terms — do not guess at specific historical or cultural names yet; \
that happens in Stage 2b. Identify every object in the scene and its exact location relative to a \
figure (held in hand, worn at the belt, on a nearby table, etc.) when applicable. If any textual \
inscription is visible, extract it literally, in its original language. Identify the theme, \
category, classification, descriptors, and symbols of the artwork. Compare your findings against \
the CSV's Objects, Symbols, Theme, Category, Classification, Descriptors, Features, and \
Inscription fields, recording any real contradiction in csv_conflicts."""

STAGE2_SCHEMA = {
    "type": "object",
    "properties": {
        "stage_2_iconographic_material_culture": {
            "type": "object",
            "properties": {
                "theme": {"type": "array", "items": {"type": "string"}},
                "category": {"type": "string"},
                "classification": {"type": "string"},
                "descriptors": {"type": "array", "items": {"type": "string"}},
                "symbols": {"type": "array", "items": {"type": "string"}},
                "features": {"type": "array", "items": {"type": "string"}},
                "afrodescendant_attributes": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "figure_id": {"type": "string"},
                            "clothing_materials": {"type": "array", "items": {"type": "string"}},
                            "clothing_style": {"type": "string"},
                            "accessories": {"type": "array", "items": {"type": "string"}},
                        },
                        "required": ["figure_id", "clothing_materials", "clothing_style", "accessories"],
                    },
                },
                "objects_in_scene": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "object": {"type": "string"},
                            "associated_figure_id": {"type": "string"},  # use "" if the object isn't tied to a specific figure
                            "location_detail": {"type": "string"},
                        },
                        "required": ["object", "associated_figure_id", "location_detail"],
                    },
                },
                "inscription_present": {"type": "boolean"},
                "inscriptions": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "type": {"type": "string"},
                            "text": {"type": "string"},
                        },
                        "required": ["type", "text"],
                    },
                },
                "support": {
                    "type": "object",
                    "properties": {
                        "format": {"type": "string"},
                        "medium": {"type": "string"},
                    },
                    "required": ["format", "medium"],
                },
                "csv_conflicts": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "field": {"type": "string"},
                            "csv_value": {"type": "string"},
                            "model_value": {"type": "string"},
                            "note": {"type": "string"},
                        },
                        "required": ["field", "csv_value", "model_value", "note"],
                    },
                },
            },
            "required": [
                "theme", "category", "classification", "descriptors", "symbols", "features",
                "afrodescendant_attributes", "objects_in_scene", "inscription_present",
                "inscriptions", "support", "csv_conflicts",
            ],
        },
    },
    "required": ["stage_2_iconographic_material_culture"],
}


# ---------------------------------------------------------------------------
# Stage 2b — Material culture identification (corpus-assisted)
# Takes Stage 2's generic descriptions, looks them up against the article
# corpus, and assigns a specific term only where the retrieved text AND the
# image actually agree. There is no input glossary to build or maintain —
# every successfully identified term is instead appended to a growing
# OUTPUT glossary (see glossary.py) as a byproduct of running the corpus.
# ---------------------------------------------------------------------------

STAGE2B_PROMPT_TEMPLATE = """Here is the generic material-culture description from Stage 2 for \
this artwork:

{previous}

Reference material retrieved from the article corpus, matched against the generic descriptions \
above:

{reference_material}

Task: for each generic description of clothing, an accessory, or an object above, check whether \
the reference material supports identifying it with a specific historical or cultural term (for \
example, a specific garment type rather than "woven cloth"). Look at the image again to confirm \
the visual details actually match before assigning a term — the reference material alone is not \
enough; it must agree with what you see. If a specific term is supported, report it in its \
original language/spelling, along with the source file it came from and a short note on why the \
visual evidence supports it. If no item in the reference material clearly matches, or the visual \
details don't agree, leave specific_term empty and explain briefly in confidence_note. Do not \
force an identification — this stage exists to add precision, not to guess."""

STAGE2B_SCHEMA = {
    "type": "object",
    "properties": {
        "stage_2b_material_identification": {
            "type": "object",
            "properties": {
                "identified_elements": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "generic_description": {"type": "string"},
                            "specific_term": {"type": "string"},
                            "term_language": {"type": "string"},
                            "supporting_source": {"type": "string"},
                            "confidence_note": {"type": "string"},
                        },
                        "required": [
                            "generic_description", "specific_term", "term_language",
                            "supporting_source", "confidence_note",
                        ],
                    },
                },
            },
            "required": ["identified_elements"],
        },
    },
    "required": ["stage_2b_material_identification"],
}


# ---------------------------------------------------------------------------
# Stage 3 — Narrative description
# ---------------------------------------------------------------------------

STAGE3_PROMPT_TEMPLATE = """Here is the result of Stages 1 and 2 for this artwork:

{previous}

Reference material retrieved from the scholarly corpus:

{reference_material}

Task: integrate this information with the CSV metadata (title, author, date, location, \
provenance) into a coherent narrative description. Identify the type of scene (individual \
portrait, religious scene, casta painting, allegory, etc.) and, if possible, the specific \
iconographic type (e.g., a saint identifiable by attributes), grounding any such identification \
in concrete visual evidence. Determine whether a donor figure is present. Compare your reading \
against the CSV's Scene and Donor fields, recording any real contradiction in csv_conflicts."""

STAGE3_SCHEMA = {
    "type": "object",
    "properties": {
        "stage_3_narrative": {
            "type": "object",
            "properties": {
                "description": {"type": "string"},
                "donor_present": {"type": "boolean"},
                "csv_conflicts": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "field": {"type": "string"},
                            "csv_value": {"type": "string"},
                            "model_value": {"type": "string"},
                            "note": {"type": "string"},
                        },
                        "required": ["field", "csv_value", "model_value", "note"],
                    },
                },
            },
            "required": ["description", "donor_present", "csv_conflicts"],
        },
    },
    "required": ["stage_3_narrative"],
}


# ---------------------------------------------------------------------------
# Stage 4a — Maieutics: corporeality and agency
# Calls 1 and 2 return free text (no schema); only the friction call (3)
# is schema-constrained. The pipeline assembles the final record in Python.
# ---------------------------------------------------------------------------

STAGE4A_CALL1_TEMPLATE = """Reference material retrieved from the scholarly corpus:

{reference_material}

Analyze the posture, gaze, and gestures of figure {figure_id} in this \
artwork. In what way does this bodily posture communicate submission, religious piety, or \
acceptance of their place within the scene's hierarchy? Specifically consider: is their \
musculature and posture rigid or tense, suggesting imposed restraint or discipline? Is their gaze \
directed downward or toward another figure in a deferential manner? Drawing on the reference \
material above, offer an interpretation — not just a \
literal description of what is visible. Answer in 5-8 sentences"""

STAGE4A_CALL2_TEMPLATE = """Reference material retrieved from the scholarly corpus:

{reference_material}

Analyze the posture, gaze, and gestures of figure {figure_id} in this \
artwork. In what way does this same bodily posture communicate self-control, self-awareness, or a \
position of intellectual or moral authority within the scene? Specifically consider: is the \
posture upright, relaxed, or commanding despite the figure's placement at the margin or \
background of the canvas? Is their gaze directed straight at the viewer, or do they share an \
affectionate gaze with another figure? Drawing on the reference material above, \
offer an interpretation — not just a \
literal description of what is visible. Answer in 5-8 sentences."""

STAGE4A_CALL3_TEMPLATE = """Below are two different readings of the same bodily posture of figure \
{figure_id}:

Reading A (subordination): {reading_a}

Reading B (agency): {reading_b}

Your task is not to decide which reading is correct. Identify the specific visual element — a \
posture, a gaze, a gesture — that both readings used to support opposite conclusions. Verify that \
element directly in the image. If there is a visual element that would allow one of the two \
readings to be ruled out, state it explicitly in resolution_note and set resolvable to true. If \
no such element exists and the ambiguity is genuine, set resolvable to false and say so."""

STAGE4_FRICTION_SCHEMA = {
    "type": "object",
    "properties": {
        "contested_element": {"type": "string"},
        "resolvable": {"type": "boolean"},
        "resolution_note": {"type": "string"},
    },
    "required": ["contested_element", "resolvable", "resolution_note"],
}


# ---------------------------------------------------------------------------
# Stage 4b — Maieutics: classification and miscegenation
# Branches by genre (casta vs. religious/secular); same friction schema.
# ---------------------------------------------------------------------------

STAGE4B_BRANCH_A_CALL1 = """Reference material retrieved from the scholarly corpus:

{reference_material}

How does the domestic or commercial environment depicted in this \
scene reflect a condition of marginality or social destitution for the family or figure \
represented? Consider the reference material above, \
offer an interpretation — not just a \
literal description of what is visible. Answer in 5-8 sentences"""

STAGE4B_BRANCH_A_CALL2 = """Reference material retrieved from the scholarly corpus:

{reference_material}

How does that same domestic or commercial environment reflect social \
mobility, prosperity, or economic stability for the family or figure represented? Consider the reference material above, \
offer an interpretation — not just a \
literal description of what is visible. Answer in 5-8 sentences"""

STAGE4B_BRANCH_B_CALL1 = """Reference material retrieved from the scholarly corpus:

{reference_material}

In what way does this canvas reinforce the racial and social \
hierarchies of the colonial era? Consider the reference material above, \
offer an interpretation — not just a \
literal description of what is visible. Answer in 5-8 sentences."""

STAGE4B_BRANCH_B_CALL2 = """Reference material retrieved from the scholarly corpus:

{reference_material}

In what way does this canvas subvert those same racial and social \
hierarchies? For example, does an Afro-descendant figure appear in a position of spiritual or \
social elevation above other figures in the scene? Consider the reference material above, \
offer an interpretation — not just a \
literal description of what is visible. Answer in 5-8 sentences."""

STAGE4B_CALL3_TEMPLATE = """Below are two different readings of the degree of fixity or fluidity \
in the social role of figure {figure_id}:

Reading A: {reading_a}

Reading B: {reading_b}

Identify the specific visual element — clothing, an object, position, the quality of the depicted \
environment — that both readings used to support opposite conclusions. Verify that element \
directly in the image. If there is an element that would allow one of the two readings to be \
ruled out, state it in resolution_note and set resolvable to true. If the ambiguity is genuine, \
set resolvable to false and say so."""


# ---------------------------------------------------------------------------
# Closing — Visual metacognition
# ---------------------------------------------------------------------------

CLOSING_TEMPLATE = """Here are the frictions identified for this artwork in the corporeality/\
agency and classification/miscegenation analyses, for all figures analyzed:

{frictions}

Based on these, answer: (1) does this painting, as a whole, seek to fix its Afro-descendant \
subjects into a rigid colonial role, or does it reveal cracks, negotiations, and fluidity in \
their social position within the viceregal world? (2) Formulate exactly two critical questions \
about what the artist may have chosen to omit or exaggerate in this work, and how that choice may \
have shaped contemporary viewers' perception."""

CLOSING_SCHEMA = {
    "type": "object",
    "properties": {
        "rigid_or_fluid_assessment": {"type": "string"},
        "critical_questions": {
            "type": "array",
            "items": {"type": "string"},
            "minItems": 2,
            "maxItems": 2,
        },
    },
    "required": ["rigid_or_fluid_assessment", "critical_questions"],
}