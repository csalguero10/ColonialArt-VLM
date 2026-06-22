# Stage 4b — Visual Maieutics: Classification and Miscegenation

As in Stage 4a, three independent calls are made, but the question pair used in Calls 1 and 2 depends on the pictorial genre identified in Stage 3. Calls 1 and 2 also receive reference material retrieved from the article corpus, using a fixed bilingual topic hint specific to each branch, combined with the artwork's detected theme.

## Preliminary step — branching by genre

Before running Calls 1 and 2, check the `theme`/`category` field from Stage 2 and the scene type from Stage 3:

- If the artwork is a **casta painting** → use Branch A.
- If the artwork is **religious or secular** → use Branch B.

---

## Branch A — Casta painting

**Call 1 — Marginality Agent:**

> Reference material retrieved from the scholarly corpus:
>
> `{reference_material}`
>
> How does the domestic or commercial environment depicted in this scene reflect a condition of marginality or social destitution for the family or figure represented? Answer based only on concrete visual evidence (quality of objects, clothing, environment), in 3-5 sentences.

**Call 2 — Social Mobility Agent:**

> Reference material retrieved from the scholarly corpus:
>
> `{reference_material}`
>
> How does that same domestic or commercial environment reflect social mobility, prosperity, or economic stability for the family or figure represented? Answer based only on concrete visual evidence, in 3-5 sentences.

---

## Branch B — Religious or secular

**Call 1 — Reinforced Hierarchy Agent:**

> Reference material retrieved from the scholarly corpus:
>
> `{reference_material}`
>
> In what way does this canvas reinforce the racial and social hierarchies of the colonial era? Answer based only on concrete visual evidence, in 3-5 sentences.

**Call 2 — Subverted Hierarchy Agent:**

> Reference material retrieved from the scholarly corpus:
>
> `{reference_material}`
>
> In what way does this canvas subvert those same racial and social hierarchies? For example, does an Afro-descendant figure appear in a position of spiritual or social elevation above other figures in the scene? Answer based only on concrete visual evidence, in 3-5 sentences.

---

## Call 3 — Friction Agent (both branches)

Does not receive reference material directly; it works from what Calls 1 and 2 already produced, and receives the image again to verify.

> Below are two different readings of the degree of fixity or fluidity in the social role of figure `{figure_id}`:
>
> Reading A: `{call_1_response}`
> Reading B: `{call_2_response}`
>
> Identify the specific visual element — clothing, an object, position, the quality of the depicted environment — that both readings used to support opposite conclusions. Verify that element directly in the image. If there is an element that would allow one of the two readings to be ruled out, state it. If the ambiguity is genuine, state that as well.

---

## Expected output format

```json
{
  "stage_4b_classification_miscegenation": {
    "figure_id": "fig_01",
    "genre_branch": "casta",
    "reading_1": "",
    "reading_2": "",
    "friction": {
      "contested_element": "",
      "resolvable": null,
      "resolution_note": ""
    }
  }
}
```

*(The pipeline additionally records `consulted_sources` for this figure's Stage 4b analysis directly in code.)*