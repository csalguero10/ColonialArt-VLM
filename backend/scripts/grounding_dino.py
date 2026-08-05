"""
Open-vocabulary grounding using Grounding DINO (the "tiny" checkpoint,
~700MB). One of two detector backends kept side by side deliberately —
see grounding_owlv2.py and config.py's GROUNDING_DETECTORS for why: rather
than picking one based on external benchmarks, both run on this actual
corpus so the comparison is empirical, not borrowed from someone else's
domain.

Runs in an ISOLATED virtual environment (see requirements-grounding.txt),
never the main one — needs transformers>=4.44, which conflicts with the
transformers==4.38.1 pinned in requirements.txt for DeepSeek-VL.
"""

import torch
from PIL import Image
from transformers import AutoModelForZeroShotObjectDetection, AutoProcessor

from config import GROUNDING_DETECTORS

_CFG = GROUNDING_DETECTORS["grounding_dino"]

_processor = None
_model = None
_device = None


def _load():
    global _processor, _model, _device
    if _model is None:
        _device = "cuda" if torch.cuda.is_available() else "cpu"
        _processor = AutoProcessor.from_pretrained(_CFG["model_id"])
        _model = AutoModelForZeroShotObjectDetection.from_pretrained(_CFG["model_id"]).to(_device)
        _model.eval()


def _format_phrases(phrases: list) -> str:
    """Grounding DINO requires phrases lowercased and each ending in a
    period, concatenated into one string — a hard requirement of the
    model's text encoder, not a style preference."""
    cleaned = [p.strip().lower().rstrip(".") + "." for p in phrases if p and p.strip()]
    return " ".join(cleaned)


def ground_phrases(image_path: str, phrases: list) -> list:
    """Detects each phrase in the image. Returns a list of
    {"phrase": str, "box": [x0, y0, x1, y1], "score": float}, box in pixel
    coordinates of the original image. See run_grounding.py's docstring
    for the honest limits of associating a detection with a specific
    figure when a phrase matches more than one location."""
    if not phrases:
        return []

    _load()
    image = Image.open(image_path).convert("RGB")
    text = _format_phrases(phrases)

    inputs = _processor(images=image, text=text, return_tensors="pt").to(_device)
    with torch.no_grad():
        outputs = _model(**inputs)

    results = _processor.post_process_grounded_object_detection(
        outputs,
        inputs.input_ids,
        threshold=_CFG["box_threshold"],
        text_threshold=_CFG["text_threshold"],
        target_sizes=[image.size[::-1]],  # PIL gives (width, height); API wants (height, width)
    )[0]

    detections = []
    for box, score, label in zip(results["boxes"], results["scores"], results["text_labels"]):
        detections.append({
            "phrase": label,
            "box": [round(float(v), 1) for v in box.tolist()],
            "score": round(float(score), 3),
        })
    return detections