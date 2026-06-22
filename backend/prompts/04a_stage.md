# Stage 4a — Visual Maieutics: Corporeality and Agency

This stage works differently from the previous ones: instead of a single agent describing the scene, **three independent calls** are made to the model. The full process is repeated for each Afro-descendant figure identified in Stage 1.

## Context provided to the model (in all three calls)

- The image of the artwork.
- The `figure_id` of the figure being analyzed.
- In Calls 1 and 2: reference material retrieved from the article corpus, using a fixed bilingual topic hint (corporeality, agency, subordination) combined with the artwork's detected theme.

---

## Call 1 — Subordination Agent

Never mention to the model, at any point, that a second, opposing reading will follow.

**Prompt:**

> Reference material retrieved from the scholarly corpus:
>
> `{reference_material}`
>
> Analyze the posture, gaze, and gestures of figure `{figure_id}` in this artwork. In what way does this bodily posture communicate submission, religious piety, or acceptance of their place within the scene's hierarchy? Specifically consider: is their musculature and posture rigid or tense, suggesting imposed restraint or discipline? Is their gaze directed downward or toward another figure in a deferential manner? Answer based only on concrete visual evidence, in 3-5 sentences.

---

## Call 2 — Agency Agent

This call is run independently from Call 1, with the model having no access to that response.

**Prompt:**

> Reference material retrieved from the scholarly corpus:
>
> `{reference_material}`
>
> Analyze the posture, gaze, and gestures of figure `{figure_id}` in this artwork. In what way does this same bodily posture communicate self-control, self-awareness, or a position of intellectual or moral authority within the scene? Specifically consider: is the posture upright, relaxed, or commanding despite the figure's placement at the margin or background of the canvas? Is their gaze directed straight at the viewer, or do they share an affectionate gaze with another figure? Answer based only on concrete visual evidence, in 3-5 sentences.

---

## Call 3 — Friction Agent

This call runs after obtaining the responses from Calls 1 and 2. It receives the image again — to visually verify the claims, not merely to arbitrate between two texts — along with both readings. It does not receive reference material directly; it works from what Calls 1 and 2 already produced.

**Prompt:**

> Below are two different readings of the same bodily posture of figure `{figure_id}`:
>
> Reading A (subordination): `{call_1_response}`
> Reading B (agency): `{call_2_response}`
>
> Your task is not to decide which reading is correct. Identify the specific visual element — a posture, a gaze, a gesture — that both readings used to support opposite conclusions. Verify that element directly in the image. If there is a visual element that would allow one of the two readings to be ruled out, state it explicitly in resolution_note and set resolvable to true. If no such element exists and the ambiguity is genuine, set resolvable to false and say so.

---

## Expected output format (per figure analyzed)

```json
{
  "stage_4a_corporeality_agency": {
    "figure_id": "fig_01",
    "subordination_reading": "",
    "agency_reading": "",
    "friction": {
      "contested_element": "",
      "resolvable": null,
      "resolution_note": ""
    }
  }
}
```

*(The pipeline additionally records `consulted_sources` for this figure's Stage 4a analysis directly in code.)*