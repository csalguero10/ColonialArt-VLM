"""
Open-vocabulary grounding: turns short text phrases (from Stage 2's object
and accessory descriptions) into bounding boxes on the actual image, using
Grounding DINO (the "tiny" checkpoint — chosen for its small footprint,
~700MB, and because it's the broadly-corroborated accuracy leader among
open-vocabulary zero-shot detectors across general benchmarks like COCO,
ODinW, and LVIS, as well as in direct head-to-head comparisons against
OWLv2. (An earlier version of this file used OWLv2 based on a single
domain-specific 2026 survey claiming it performed better on paintings —
that recommendation didn't hold up against the much broader body of
general-purpose benchmark evidence, so this reverts to Grounding DINO.)

IMPORTANT — runs in an ISOLATED virtual environment (see
requirements-grounding.txt), never in the main environment: it needs
transformers>=4.44, which conflicts with the transformers==4.38.1 pinned
in the main requirements.txt for DeepSeek-VL. See README for setup.
"""

import torch
from PIL import Image
from transformers import AutoModelForZeroShotObjectDetection, AutoProcessor

from config import GROUNDING_BOX_THRESHOLD, GROUNDING_MODEL_NAME, GROUNDING_TEXT_THRESHOLD

_processor = None
_model = None
_device = None


def _load():
    global _processor, _model, _device
    if _model is None:
        _device = "cuda" if torch.cuda.is_available() else "cpu"
        _processor = AutoProcessor.from_pretrained(GROUNDING_MODEL_NAME)
        _model = AutoModelForZeroShotObjectDetection.from_pretrained(GROUNDING_MODEL_NAME).to(_device)
        _model.eval()


def _format_phrases(phrases: list) -> str:
    """Grounding DINO requires phrases lowercased and each ending in a
    period, concatenated into one string — this is a hard requirement of
    the model's text encoder, not a style preference."""
    cleaned = [p.strip().lower().rstrip(".") + "." for p in phrases if p and p.strip()]
    return " ".join(cleaned)


# Grounding DINO's tokenizer is BERT-style (512-token limit), but the
# model's own text branch is fixed at config.max_text_len = 256 (the fusion
# module's text-feature buffer is sized to it). A single phrase from Stage
# 2 is never remotely close to that, so this is just a defensive guard
# against a pathological one, not something expected to trigger in
# practice.
_MAX_TEXT_TOKENS = 220


def ground_phrases(
    image_path: str,
    phrases: list,
    box_threshold: float = None,
    text_threshold: float = None,
) -> list:
    """Detects each phrase in the image. Returns a list of
    {"phrase": str, "box": [x0, y0, x1, y1], "score": float}, box in pixel
    coordinates of the original image — "phrase" is always the exact input
    string (not whatever Grounding DINO's decoder reconstructs), so callers
    can match detections back to their source data with a plain lookup.

    Queries ONE phrase per model call rather than batching several into a
    single text prompt. Batching seems like the obvious way to cut down
    forward passes, but Grounding DINO's phrase decoder maps detections
    back to text by *position* in the combined prompt, and it noticeably
    confuses that mapping whenever two phrases in the same query share
    words — not just exact duplicates, but anything with overlapping
    vocabulary (e.g. "Silk or silk-like fabric (teal/blue-green tunic)"
    next to "...(tan/brown tunic)" came back merged as garbled labels like
    "silk silk" in testing). Since Stage 2 phrases for the same image
    routinely share words (the same garment/material vocabulary across
    figures), one-phrase-per-call is the only way to get a clean,
    unambiguous label back for each one. It costs one forward pass per
    phrase instead of one per chunk, but each pass is cheap (a handful of
    tokens), and smaller inputs also mean less peak memory per pass — a
    plus on an 8GB machine.

    A phrase can still come back zero, one, or multiple times (e.g. an
    accessory worn by several figures in the same scene returns multiple
    boxes for the same phrase). This function does not attempt to decide
    which box belongs to which figure — see run_grounding.py for how
    detections are associated with figure_ids, and its docstring for the
    honest limits of that association."""
    if not phrases:
        return []

    _load()
    box_threshold = GROUNDING_BOX_THRESHOLD if box_threshold is None else box_threshold
    text_threshold = GROUNDING_TEXT_THRESHOLD if text_threshold is None else text_threshold

    image = Image.open(image_path).convert("RGB")

    detections = []
    for phrase in phrases:
        text = _format_phrases([phrase])
        n_tokens = len(_processor.tokenizer(text)["input_ids"])
        if n_tokens > _MAX_TEXT_TOKENS:
            print(f"[WARN] Skipping ungroundable phrase ({n_tokens} tokens): {phrase!r}")
            continue

        inputs = _processor(images=image, text=text, return_tensors="pt").to(_device)
        with torch.no_grad():
            outputs = _model(**inputs)

        results = _processor.post_process_grounded_object_detection(
            outputs,
            inputs.input_ids,
            box_threshold=box_threshold,
            text_threshold=text_threshold,
            target_sizes=[image.size[::-1]],  # PIL gives (width, height); API wants (height, width)
        )[0]

        for box, score in zip(results["boxes"], results["scores"]):
            detections.append({
                "phrase": phrase,
                "box": [round(float(v), 1) for v in box.tolist()],
                "score": round(float(score), 3),
            })
    return detections