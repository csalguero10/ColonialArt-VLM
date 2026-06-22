# Closing — Visual Metacognition

This runs once per artwork, after completing Stages 4a and 4b for every Afro-descendant figure analyzed.

## Context provided to the model

- The image of the artwork.
- The friction results from Stages 4a and 4b for all figures.

## Prompt

> Based on the frictions identified in the corporeality/agency and classification/miscegenation analyses for this artwork, answer:
>
> 1. Does this painting, as a whole, seek to fix its Afro-descendant subjects into a rigid colonial role, or does it reveal cracks, negotiations, and fluidity in their social position within the viceregal world?
> 2. Formulate two critical questions about what the artist may have chosen to omit or exaggerate in this work, and how that choice may have shaped contemporary viewers' perception.

## Expected output format

```json
{
  "closing_metacognition": {
    "rigid_or_fluid_assessment": "",
    "critical_questions": []
  }
}
```