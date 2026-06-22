# Stage 1 — Pre-iconographic Reading + Metadata Cross-reference

*Also apply the general instructions in `00_general_instructions.md`.*

## Context provided to the model

- The image of the artwork.
- The corresponding CSV record, in this format:

```json
{
  "Image_ID": "...",
  "Title": "...",
  "Author": "...",
  "Medium": "...",
  "Date": "...",
  "Location": "...",
  "Provenance": "...",
  "Theme": "...",
  "Category": "...",
  "Classification": "...",
  "Descriptors": "...",
  "Symbols": "...",
  "Features": "...",
  "Scene": "...",
  "Face_position": "...",
  "Objects": "...",
  "Gestures": "...",
  "Physiognomy": "...",
  "Donor": "...",
  "Inscription": "..."
}
```

## Task

1. Load the CSV data into the `csv_metadata` block of the output JSON.
2. Observe the entire image before starting to describe it.
3. Identify all human figures present and number them following the reading order (depth, then left to right).
4. For each figure, determine whether it is perceived as Afro-descendant.
5. For each figure, record: spatial position within the composition, relative scale compared to other figures, basic physical state (standing, seated, kneeling, in motion — without interpreting the meaning of that posture yet), and a basic physiognomic description.
6. Describe the artwork's general technical resources: lighting, color palette, compositional notes.
7. Compare your visual reading against the `Face_position` and `Physiognomy` fields of the CSV. Record an entry in `csv_conflicts` only if there is a real contradiction.

## Guiding questions

- How many figures are there in total in the composition, and how many of them are perceived as Afro-descendant?
- What is the spatial position of each figure? Center or margin, foreground or background, larger or smaller scale relative to other figures?
- What are the artwork's technical resources like: lighting, color palette, overall composition?
- What is the basic physical state of each figure?

## Expected output format

```json
{
  "csv_metadata": {
    "image_id": "",
    "title": "",
    "author": "",
    "medium": "",
    "date": "",
    "location": "",
    "provenance": "",
    "theme_csv": "",
    "category_csv": "",
    "classification_csv": "",
    "descriptors_csv": "",
    "symbols_csv": "",
    "features_csv": "",
    "scene_csv": "",
    "face_position_csv": "",
    "objects_csv": "",
    "gestures_csv": "",
    "physiognomy_csv": "",
    "physiognomy_image_csv": "",
    "donor_csv": "",
    "inscription_csv": ""
  },
  "stage_1_pre_iconographic": {
    "people_present": true,
    "total_figures": 0,
    "afrodescendant_present": true,
    "number_of_afrodescendants": 0,
    "figures": [
      {
        "figure_id": "fig_01",
        "perceived_as_afrodescendant": true,
        "position_in_space": "",
        "relative_scale": "",
        "pose_state_basic": "",
        "physiognomy_description": ""
      }
    ],
    "technical_resources": {
      "lighting": "",
      "color_palette": "",
      "composition_notes": ""
    },
    "csv_conflicts": []
  }
}
```