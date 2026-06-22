# Stage 3 — Narrative Description

*Also apply the general instructions. Use the already-assigned `figure_id` values.*

## Context provided to the model

- The image of the artwork.
- The completed JSON from Stages 1, 2, and 2b.
- Reference material: passages retrieved from the article corpus, matched against the artwork's theme, detected symbols, Stage 2b's specific terms, and title.

## Task

1. Integrate the information from the previous stages with the CSV metadata (title, author, date, location, provenance) to construct a coherent narrative description of the artwork.
2. Identify the type of scene depicted (individual portrait, religious scene, casta painting, allegorical scene, etc.) and, if possible, the specific iconographic type (e.g., a saint identifiable by their attributes).
3. Determine whether a donor figure is present in the composition.
4. Ground any iconographic type identification in concrete visual evidence (attributes, clothing, gestures). Do not assume a figure's identity without that basis.
5. Compare your reading against the `Scene` and `Donor` fields of the CSV. Record an entry in `csv_conflicts` only if there is a real contradiction.

## Guiding questions

- What is the overall argument or narrative of the scene, and what function does the Afro-descendant subject serve within it?
- What type of scene does the artwork depict?
- If the artwork belongs to a recognizable genre, what specific iconographic type can be identified from its attributes?
- Is a donor figure present? Where is it located within the composition?
- What broader narratives does the presence of the Afro-descendant subject make evident within the overall composition?

## Expected output format

```json
{
  "stage_3_narrative": {
    "description": "",
    "donor_present": false,
    "csv_conflicts": []
  }
}
```

*(The pipeline additionally records `consulted_sources` — which corpus files were retrieved for this call — directly in code, not as something the model needs to report.)*