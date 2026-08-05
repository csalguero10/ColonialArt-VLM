"""
Reads Stage 1/2/2b results already produced by run_experiment.py, grounds
each object/accessory phrase onto the actual image, and saves:
  - a scene graph JSON per image (nodes = figures + detected objects,
    edges = the figure<->object associations already present in Stage 2)
  - a copy of the image with boxes and labels drawn on it, for visual
    inspection (the "highlighted regions" look, similar to VQArt-Bench's
    figures)

Supports EITHER of two detector backends (--detector grounding_dino or
--detector owlv2, see grounding_dino.py / grounding_owlv2.py / config.py's
GROUNDING_DETECTORS) so both can be run on the same corpus and compared
directly, rather than picking one from external benchmarks. Each detector
writes to its own subfolder under data/scene_graphs/ and
data/scene_graphs_viz/, so running both never overwrites the other's
results — see compare_grounding.py once both have been run on the same
images.

HONEST LIMITATION, read before trusting the edges: when a phrase matches
more than one figure in the same scene (e.g. several figures share a
"white head wrap" accessory), the detector returns multiple boxes for
that one phrase, but nothing here verifies WHICH box belongs to WHICH
figure_id — the edge in the scene graph records the semantic association
already present in Stage 2's data, not a spatially verified match. For
scenes with many visually similar figures (as in some of this corpus's
frieze-like compositions), treat the edges as "this type of object is
associated with this figure per Stage 2's reading," not as a guaranteed
one-to-one spatial correspondence. Person-level bounding boxes for each
individual figure are NOT produced by this script — that would need a
separate approach (e.g. YOLO's generic "person" class, complementary to
this open-vocabulary object grounding), which is a reasonable next step
but out of scope here.

Must run in the isolated grounding venv (see requirements-grounding.txt),
NOT the main environment — see README.

Usage (from backend/, using the grounding venv's python):
    venv-grounding/bin/python scripts/run_grounding.py claude-haiku --detector grounding_dino
    venv-grounding/bin/python scripts/run_grounding.py claude-haiku --detector owlv2
    venv-grounding/bin/python scripts/run_grounding.py claude-haiku --detector owlv2 --csv data/test_sample.csv
"""

import argparse
import json
import os

from PIL import Image, ImageDraw, ImageFont

from config import IMAGES_DIR, RESULTS_DIR, SCENE_GRAPH_DIR, SCENE_GRAPH_VIZ_DIR
# ground_phrases is imported dynamically in main() based on --detector

IMAGE_EXTENSIONS = (".webp", ".jpg", ".jpeg", ".png")


def find_image_path(image_id: str):
    for ext in IMAGE_EXTENSIONS:
        candidate = os.path.join(IMAGES_DIR, f"{image_id}{ext}")
        if os.path.exists(candidate):
            return candidate
    return None


def collect_groundable_items(result: dict) -> list:
    """Pulls concrete noun phrases out of Stage 2's output, each tagged
    with the figure_id it's associated with (if any) and what kind of
    item it is. Returns a list of
    {"phrase": str, "figure_id": str or None, "kind": str}."""
    items = []
    stage2 = result.get("stage_2_iconographic_material_culture", {})

    for obj in stage2.get("objects_in_scene", []):
        phrase = obj.get("object", "")
        if phrase:
            items.append({
                "phrase": phrase,
                "figure_id": obj.get("associated_figure_id") or None,
                "kind": "object",
            })

    for attr in stage2.get("afrodescendant_attributes", []):
        figure_id = attr.get("figure_id")
        for accessory in attr.get("accessories", []):
            if accessory:
                items.append({"phrase": accessory, "figure_id": figure_id, "kind": "accessory"})
        if attr.get("clothing_style"):
            items.append({"phrase": attr["clothing_style"], "figure_id": figure_id, "kind": "clothing"})

    return items


def build_scene_graph(image_id: str, result: dict, items: list, detections: list) -> dict:
    figures = result.get("stage_1_pre_iconographic", {}).get("figures", [])

    nodes = [
        {"id": f["figure_id"], "type": "figure", "label": f.get("physiognomy_description", "")}
        for f in figures
    ]

    detections_by_phrase = {}
    for d in detections:
        detections_by_phrase.setdefault(d["phrase"].lower().rstrip("."), []).append(d)

    edges = []
    object_counter = 0
    for item in items:
        phrase_key = item["phrase"].strip().lower()
        for det in detections_by_phrase.get(phrase_key, []):
            object_counter += 1
            obj_id = f"obj_{object_counter:02d}"
            nodes.append({
                "id": obj_id,
                "type": item["kind"],
                "label": item["phrase"],
                "bbox": det["box"],
                "score": det["score"],
            })
            if item["figure_id"]:
                edges.append({"source": obj_id, "target": item["figure_id"], "relation": "associated_with"})

    return {"image_id": image_id, "nodes": nodes, "edges": edges}


# One color per node type, chosen for legibility against painted
# backgrounds rather than the flat debug-red used in the first version.
# (r, g, b) for the outline/label chip; fill uses the same color at low
# alpha for a soft highlight rather than a plain outline.
_TYPE_COLORS = {
    "object": (214, 69, 65),      # terracotta red
    "accessory": (201, 151, 44),  # amber/gold
    "clothing": (58, 99, 130),    # muted teal-blue
}
_DEFAULT_COLOR = (110, 110, 110)


def draw_annotated_image(image_path: str, scene_graph: dict, out_path: str):
    base = Image.open(image_path).convert("RGB")
    overlay = Image.new("RGBA", base.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)
    try:
        font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 15)
    except OSError:
        font = ImageFont.load_default()  # always available; just plainer text

    for node in scene_graph["nodes"]:
        if "bbox" not in node:
            continue
        x0, y0, x1, y1 = node["bbox"]
        color = _TYPE_COLORS.get(node["type"], _DEFAULT_COLOR)

        # Soft translucent fill so the highlighted region reads clearly
        # over a busy painted background, plus a solid outline on top.
        draw.rectangle([x0, y0, x1, y1], fill=color + (55,), outline=color + (255,), width=3)

        label = f'{node["label"]} ({node["score"]:.2f})'
        text_bbox = draw.textbbox((0, 0), label, font=font)
        text_w, text_h = text_bbox[2] - text_bbox[0], text_bbox[3] - text_bbox[1]
        chip_y0 = max(0, y0 - text_h - 8)
        draw.rectangle([x0, chip_y0, x0 + text_w + 10, chip_y0 + text_h + 6], fill=color + (230,))
        draw.text((x0 + 5, chip_y0 + 2), label, fill=(255, 255, 255, 255), font=font)

    annotated = Image.alpha_composite(base.convert("RGBA"), overlay).convert("RGB")
    annotated.save(out_path)


def main(model_name: str, detector: str, csv_path: str = None):
    if detector == "grounding_dino":
        from grounding_dino import ground_phrases
    elif detector == "owlv2":
        from grounding_owlv2 import ground_phrases
    else:
        raise ValueError(f"Unknown detector '{detector}'. Choose 'grounding_dino' or 'owlv2'.")

    results_dir = os.path.join(RESULTS_DIR, model_name)
    if not os.path.isdir(results_dir):
        print(f"No results found at {results_dir}. Run run_experiment.py for this model first.")
        return

    # Each detector gets its own subfolder, so running both never overwrites
    # the other's results — compare_grounding.py reads from both.
    out_graph_dir = os.path.join(SCENE_GRAPH_DIR, detector, model_name)
    out_viz_dir = os.path.join(SCENE_GRAPH_VIZ_DIR, detector, model_name)
    os.makedirs(out_graph_dir, exist_ok=True)
    os.makedirs(out_viz_dir, exist_ok=True)

    result_files = sorted(f for f in os.listdir(results_dir) if f.endswith(".json"))
    if csv_path:
        import pandas as pd
        allowed_ids = set(pd.read_csv(csv_path)["Image_ID"].astype(str))
        result_files = [f for f in result_files if os.path.splitext(f)[0] in allowed_ids]

    for fname in result_files:
        image_id = os.path.splitext(fname)[0]
        out_graph_path = os.path.join(out_graph_dir, f"{image_id}.json")
        if os.path.exists(out_graph_path):
            continue  # checkpoint, same pattern as run_experiment.py

        image_path = find_image_path(image_id)
        if image_path is None:
            print(f"[WARN] Image not found for {image_id}, skipping.")
            continue

        with open(os.path.join(results_dir, fname), encoding="utf-8") as f:
            result = json.load(f)

        items = collect_groundable_items(result)
        phrases = [item["phrase"] for item in items]
        if not phrases:
            print(f"[INFO] {image_id}: no groundable phrases in Stage 2 output.")
            continue

        try:
            detections = ground_phrases(image_path, phrases)
        except Exception as e:
            print(f"[ERROR] {image_id}: grounding failed: {e}")
            continue

        scene_graph = build_scene_graph(image_id, result, items, detections)

        with open(out_graph_path, "w", encoding="utf-8") as f:
            json.dump(scene_graph, f, ensure_ascii=False, indent=2)

        draw_annotated_image(image_path, scene_graph, os.path.join(out_viz_dir, f"{image_id}.jpg"))
        print(f"[OK] {image_id}: {len(phrases)} phrases -> {len(detections)} detections")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("model", help="Model name whose Stage 2 results to ground (a folder under data/results/)")
    parser.add_argument("--detector", choices=["grounding_dino", "owlv2"], required=True,
                        help="Which detector backend to use for this run")
    parser.add_argument("--csv", default=None, help="Optional CSV to restrict to a subset of Image_IDs")
    args = parser.parse_args()
    main(args.model, args.detector, args.csv)