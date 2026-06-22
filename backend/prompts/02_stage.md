# Stage 2 — Iconographic Analysis: Material Culture

*Also apply the general instructions in `00_general_instructions.md`. Use the `figure_id` values already assigned in Stage 1; do not reassign them.*

## Context provided to the model

- The image of the artwork.
- The JSON output from Stage 1 (includes `csv_metadata` and the already-assigned `figure_id` values).

*No glossary is consulted here. Stage 2 stays purely visual and describes everything in plain, generic terms — assigning specific historical or cultural names happens in Stage 2b, which looks things up against the article corpus instead.*

## Task

1. For each figure marked as Afro-descendant in Stage 1, describe the materials, textures, and style of their clothing and accessories, using plain generic terms. Avoid evaluative binaries (e.g., "rich clothing" vs. "poor clothing"); do not guess at a specific historical or cultural name yet.
2. Identify all objects present in the scene, without needing to associate them with a specific figure. When an object is in direct contact with a figure (held in hand, worn at the belt, etc.), specify this in `location_detail`.
3. If any textual inscription is visible (phylactery, cartouche, legend), extract the text literally — keep the original language of the inscription as written on the artwork.
4. Identify the theme, category, classification, descriptors, and symbols present in the artwork.
5. Compare your findings against the `Objects`, `Symbols`, `Theme`, `Category`, `Classification`, `Descriptors`, `Features`, and `Inscription` fields of the CSV (available in `csv_metadata`). Record an entry in `csv_conflicts` only if there is a real contradiction.

## Guiding questions

- What information do the clothing, accessories, and surrounding objects provide about the subject's daily life, environment, or background?
- What are the specific materials, textures, and styles of the subject's clothing and accessories, and what do they suggest about their identity?
- What specific objects, goods, or tools are present in the scene, and how do they relate to the subject's environment or activities? Where exactly are they located?
- Is there any textual inscription present in the artwork? If so, what is the literal text?
- What theme, category, or pictorial genre does the artwork belong to?

## Expected output format

```json
{
  "stage_2_iconographic_material_culture": {
    "theme": [],
    "category": "",
    "classification": "",
    "descriptors": [],
    "symbols": [],
    "features": [],
    "afrodescendant_attributes": [
      {
        "figure_id": "fig_01",
        "clothing_materials": [],
        "clothing_style": "",
        "accessories": []
      }
    ],
    "objects_in_scene": [
      { "object": "", "associated_figure_id": null, "location_detail": "" }
    ],
    "inscription_present": false,
    "inscriptions": [],
    "support": {
      "format": "",
      "medium": ""
    },
    "csv_conflicts": []
  }
}
```