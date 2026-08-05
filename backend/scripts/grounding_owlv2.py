"""
Open-vocabulary grounding using OWLv2. The second of two detector
backends kept side by side deliberately — see grounding_dino.py and
config.py's GROUNDING_DETECTORS for why: general-purpose benchmarks
(COCO, ODinW, LVIS) favor Grounding DINO, but one narrower 2026 study on
painted/artistic imagery specifically favored OWLv2. Rather than trust
either claim blindly, both run on this actual corpus so the choice is
based on real results here, not borrowed benchmark numbers from a
different domain.

Runs in an ISOLATED virtual environment (see requirements-grounding.txt),
never the main one — needs a newer transformers than the 4.38.1 pinned
in requirements.txt for DeepSeek-VL.
"""

import torch
from PIL import Image
from transformers import Owlv2ForObjectDetection, Owlv2Processor

from config import GROUNDING_DETECTORS

_CFG = GROUNDING_DETECTORS["owlv2"]

_processor = None
_model = None
_device = None


def _load():
    global _processor, _model, _device
    if _model is None:
        _device = "cuda" if torch.cuda.is_available() else "cpu"
        _processor = Owlv2Processor.from_pretrained(_CFG["model_id"])
        _model = Owlv2ForObjectDetection.from_pretrained(_CFG["model_id"]).to(_device)
        _model.eval()


def ground_phrases(image_path: str, phrases: list) -> list:
    """Detects each phrase in the image. Returns a list of
    {"phrase": str, "box": [x0, y0, x1, y1], "score": float}, box in pixel
    coordinates of the original image. Same return shape as
    grounding_dino.ground_phrases() by design, so run_grounding.py and
    compare_grounding.py don't need to know which detector produced a
    given result. Unlike Grounding DINO, OWLv2 takes plain text queries
    directly — no lowercase/trailing-period formatting needed."""
    if not phrases:
        return []

    _load()
    image = Image.open(image_path).convert("RGB")
    text_labels = [phrases]  # OWLv2 expects a list of phrase-lists, one per image in the batch

    inputs = _processor(text=text_labels, images=image, return_tensors="pt").to(_device)
    with torch.no_grad():
        outputs = _model(**inputs)

    target_sizes = torch.tensor([(image.height, image.width)])
    results = _processor.post_process_grounded_object_detection(
        outputs=outputs,
        target_sizes=target_sizes,
        threshold=_CFG["threshold"],
        text_labels=text_labels,
    )[0]

    detections = []
    for box, score, label in zip(results["boxes"], results["scores"], results["text_labels"]):
        detections.append({
            "phrase": label,
            "box": [round(float(v), 1) for v in box.tolist()],
            "score": round(float(score), 3),
        })
    return detections