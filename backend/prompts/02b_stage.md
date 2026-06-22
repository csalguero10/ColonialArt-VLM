# Stage 2b — Material Culture Identification (Corpus-Assisted)

*Also apply the general instructions. This stage runs after Stage 2 and before Stage 3 — it is the only mechanism for assigning specific historical/cultural terms to material culture. There is no glossary to consult as input.*

*Methodological note: this stage stays at Panofsky's iconographic level (Level 2) — it identifies conventional types ("this is a huipil"), not their social or ideological meaning ("this signifies..."), which remains the work of Stages 3 and 4. The article corpus is used here only to supply naming vocabulary, narrowly scoped — it should never be used to look up casta classification labels (e.g., "morisco," "lobo," "albarazado"); those are not neutral nomenclature, and assigning them here would mean performing racial classification before Stage 4's mayeutics even begins.*

## Context provided to the model

- The image of the artwork.
- The full JSON output from Stage 2 (the generic descriptions of clothing, accessories, and objects).
- Reference material: for each generic description in Stage 2, the pipeline retrieves the most similar passages from the article corpus (by embedding similarity, not keyword match) and presents them here, labeled by source file.

## Task

For each generic description of clothing, an accessory, or an object from Stage 2:

1. Check whether the retrieved reference material supports identifying it with a specific historical or cultural term (e.g., a specific garment type rather than "woven cloth").
2. Look at the image again to confirm the visual details actually match before assigning a term — the reference material alone is not sufficient; it must agree with what is visible.
3. If a specific term is supported by both the text and the image, report it in its original language/spelling, name the source file it came from, and add a short note on why the visual evidence supports it.
4. If no passage in the reference material clearly matches, or the visual details don't agree with what a passage describes, leave `specific_term` empty and explain briefly in `confidence_note`. Do not force an identification — this stage exists to add precision, not to guess.

## Expected output format

```json
{
  "stage_2b_material_identification": {
    "identified_elements": [
      {
        "generic_description": "",
        "specific_term": "",
        "term_language": "",
        "supporting_source": "",
        "confidence_note": ""
      }
    ]
  }
}
```

## Output side-effect: the discovered glossary

Every element here with a non-empty `specific_term` is automatically appended to a cumulative output file, `data/discovered_glossary.json`, deduplicated by term. This file is **not** something you build or maintain — it grows on its own as the pipeline runs across your corpus, recording which source backed each term, why, and in which images it was seen. Treat it as a research output (a discovered vocabulary, with citations) rather than as a configuration file.