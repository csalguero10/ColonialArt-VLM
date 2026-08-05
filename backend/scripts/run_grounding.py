"""
Reads Stage 1/2/2b results already produced by run_experiment.py, grounds
each object/accessory phrase onto the actual image with Grounding DINO, and
saves:
  - a scene graph JSON per image (nodes = figures + detected objects,
    edges = the figure<->object associations already present in Stage 2)
  - a copy of the image with boxes and labels drawn on it, for visual
    inspection (the "highlighted regions" look, similar to VQArt-Bench's
    figures)

HONEST LIMITATION, read before trusting the edges: when a phrase matches
more than one figure in the same scene (e.g. several figures share a
"white head wrap" accessory), Grounding DINO returns multiple boxes for
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
    venv-grounding/bin/python scripts/run_grounding.py claude-haiku
    venv-grounding/bin/python scripts/run_grounding.py claude-haiku --csv data/test_sample.csv
"""

import argparse
import json
import os

from PIL import Image, ImageDraw, ImageFont

from config import IMAGES_DIR, RESULTS_DIR, SCENE_GRAPH_DIR, SCENE_GRAPH_VIZ_DIR
from grounding import ground_phrases

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


def draw_annotated_image(image_path: str, scene_graph: dict, out_path: str):
    image = Image.open(image_path).convert("RGB")
    draw = ImageDraw.Draw(image)
    try:
        font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 16)
    except OSError:
        font = ImageFont.load_default()  # always available; just plainer text

    for node in scene_graph["nodes"]:
        if "bbox" not in node:
            continue
        x0, y0, x1, y1 = node["bbox"]
        draw.rectangle([x0, y0, x1, y1], outline="red", width=3)
        draw.text((x0, max(0, y0 - 18)), f'{node["label"]} ({node["score"]:.2f})', fill="red", font=font)

    image.save(out_path)


def main(model_name: str, csv_path: str = None):
    results_dir = os.path.join(RESULTS_DIR, model_name)
    if not os.path.isdir(results_dir):
        print(f"No results found at {results_dir}. Run run_experiment.py for this model first.")
        return

    out_graph_dir = os.path.join(SCENE_GRAPH_DIR, model_name)
    out_viz_dir = os.path.join(SCENE_GRAPH_VIZ_DIR, model_name)
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
    parser.add_argument("--csv", default=None, help="Optional CSV to restrict to a subset of Image_IDs")
    args = parser.parse_args()
    main(args.model, args.csv)