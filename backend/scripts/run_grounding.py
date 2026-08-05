"""
Reads Stage 1/2/2b results already produced by run_experiment.py, grounds
each figure and object/accessory phrase onto the actual image with
Grounding DINO, and saves:
  - a scene graph JSON per image (nodes = figures + detected objects,
    edges = the figure<->object associations already present in Stage 2)
  - a copy of the image with boxes and labels drawn on it, for visual
    inspection (the "highlighted regions" look, similar to VQArt-Bench's
    figures)

HONEST LIMITATIONS, read before trusting the edges:

- Figure boxes come from a single generic "person." query, then assigned
  to fig_01, fig_02, ... in left-to-right order (matching the VLM's stated
  reading-order convention). For simple compositions (a row of portrait
  figures) this works well; for scenes with multiple depth planes or
  overlapping figures (this corpus's more crowded frieze-like
  compositions), left-to-right sorting will NOT reliably match the actual
  reading order — treat the figure_id <-> box pairing as best-effort, not
  verified, on those images.
- When a phrase matches more than one figure in the same scene (e.g.
  several figures share a "white head wrap" accessory), Grounding DINO
  returns multiple boxes for that one phrase, but nothing here verifies
  WHICH box belongs to WHICH figure_id — the edge in the scene graph
  records the semantic association already present in Stage 2's data, not
  a spatially verified match.

Must run in the isolated grounding venv (see requirements-grounding.txt),
NOT the main environment — see README.

Usage (from backend/, using the grounding venv's python):
    venv-grounding/bin/python scripts/run_grounding.py claude-haiku
    venv-grounding/bin/python scripts/run_grounding.py claude-haiku --csv data/test_sample.csv
"""

import argparse
import json
import os
import re

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


def _normalize(text: str) -> str:
    """Loose match key for comparing an original phrase against Grounding
    DINO's decoded label text. The decoder reconstructs phrases from
    wordpiece spans and routinely adds spaces around hyphens/slashes
    ("tunic-style" -> "tunic - style"), which breaks a naive exact-string
    comparison; stripping everything but alphanumerics sidesteps that."""
    return re.sub(r"[^a-z0-9]", "", text.lower())


def collect_groundable_items(result: dict) -> dict:
    """Pulls concrete, nameable phrases out of Stage 2's output — objects,
    clothing materials, and accessories — keyed by phrase (case-
    insensitive) with the set of figure_ids that share each one.

    Two deliberate choices, both learned from a first pass that mostly
    failed to link its own detections back to Stage 2's items:

    1. Excludes clothing_style. Stage 2 tends to write it as a full
       descriptive sentence ("Tunic-style garment with European-
       influenced cut; teal/blue-green outer layer with gold embroidered
       trim..."), not a nameable phrase — Grounding DINO grounds short
       noun phrases, not prose, and a sentence never matches any region.
    2. Deduplicates by phrase instead of emitting one entry per figure.
       The same accessory is often listed identically for every figure
       that has it (e.g. every figure in a group portrait wearing the
       same "black hat with white plume"); sending that phrase to the
       model three times in one query doesn't recover three separate
       detections — Grounding DINO already returns every matching
       instance from a single query — and it actively confuses the
       model's phrase-to-box decoding when the same text appears more
       than once in the prompt (observed firsthand: it comes back as
       "black hat black hat black hat black hat black hat" instead of a
       clean "black hat", which then can't be matched to anything)."""
    stage2 = result.get("stage_2_iconographic_material_culture", {})
    items = {}

    def _add(phrase, figure_id, kind):
        phrase = (phrase or "").strip()
        if not phrase:
            return
        key = phrase.lower()
        entry = items.setdefault(key, {"phrase": phrase, "figure_ids": set(), "kind": kind})
        if figure_id:
            entry["figure_ids"].add(figure_id)

    for obj in stage2.get("objects_in_scene", []):
        _add(obj.get("object", ""), obj.get("associated_figure_id") or None, "object")

    for attr in stage2.get("afrodescendant_attributes", []):
        figure_id = attr.get("figure_id")
        for material in attr.get("clothing_materials", []):
            _add(material, figure_id, "material")
        for accessory in attr.get("accessories", []):
            _add(accessory, figure_id, "accessory")

    return items


def ground_figures(image_path: str) -> list:
    """Generic person-level detection, independent of Stage 2's phrases —
    a bare "person." query is exactly the kind of short, common category
    open-vocabulary detectors are most reliable at, unlike the long/
    compound descriptive phrases Stage 2 produces for clothing and
    accessories."""
    return ground_phrases(image_path, ["person"])


def build_scene_graph(image_id: str, result: dict, items: dict,
                       detections: list, figure_boxes: list) -> dict:
    figures = result.get("stage_1_pre_iconographic", {}).get("figures", [])

    # Left-to-right assignment — see the HONEST LIMITATIONS note at the
    # top of this file for when this pairing can't be trusted.
    figure_boxes = sorted(figure_boxes, key=lambda d: d["box"][0])

    nodes = []
    for i, f in enumerate(figures):
        node = {"id": f["figure_id"], "type": "figure", "label": f.get("physiognomy_description", "")}
        if i < len(figure_boxes):
            node["bbox"] = figure_boxes[i]["box"]
            node["score"] = figure_boxes[i]["score"]
        nodes.append(node)

    detections_by_phrase = {}
    for d in detections:
        detections_by_phrase.setdefault(_normalize(d["phrase"]), []).append(d)

    edges = []
    object_counter = 0
    for item in items.values():
        for det in detections_by_phrase.get(_normalize(item["phrase"]), []):
            object_counter += 1
            obj_id = f"obj_{object_counter:02d}"
            nodes.append({
                "id": obj_id,
                "type": item["kind"],
                "label": item["phrase"],
                "bbox": det["box"],
                "score": det["score"],
            })
            for figure_id in item["figure_ids"]:
                edges.append({"source": obj_id, "target": figure_id, "relation": "associated_with"})

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
        color = "dodgerblue" if node["type"] == "figure" else "red"
        x0, y0, x1, y1 = node["bbox"]
        draw.rectangle([x0, y0, x1, y1], outline=color, width=3)
        draw.text((x0, max(0, y0 - 18)), f'{node["label"]} ({node["score"]:.2f})', fill=color, font=font)

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
        phrases = [item["phrase"] for item in items.values()]

        try:
            figure_boxes = ground_figures(image_path)
            detections = ground_phrases(image_path, phrases) if phrases else []
        except Exception as e:
            print(f"[ERROR] {image_id}: grounding failed: {e}")
            continue

        scene_graph = build_scene_graph(image_id, result, items, detections, figure_boxes)

        with open(out_graph_path, "w", encoding="utf-8") as f:
            json.dump(scene_graph, f, ensure_ascii=False, indent=2)

        draw_annotated_image(image_path, scene_graph, os.path.join(out_viz_dir, f"{image_id}.jpg"))
        n_grounded_objects = sum(1 for n in scene_graph["nodes"] if n["type"] != "figure")
        print(f"[OK] {image_id}: {len(figure_boxes)} figures, "
              f"{len(phrases)} phrases -> {n_grounded_objects} objects grounded")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("model", help="Model name whose Stage 2 results to ground (a folder under data/results/)")
    parser.add_argument("--csv", default=None, help="Optional CSV to restrict to a subset of Image_IDs")
    args = parser.parse_args()
    main(args.model, args.csv)
