"""
Compares the two grounding backends (grounding_dino vs. owlv2) after both
have been run on the same model's results with run_grounding.py. Produces:

  - data/grounding_comparison/summary.csv — per-image counts and average
    confidence scores for each detector, so you can see quantitatively
    which one found more of Stage 2's phrases and with what confidence.
  - data/grounding_comparison/side_by_side/<image_id>.jpg — the two
    annotated images stitched together horizontally, for direct visual
    inspection of the same artwork under both detectors.

Run this AFTER running run_grounding.py once with --detector grounding_dino
and once with --detector owlv2 on the same model_name.

Usage (from backend/, using the grounding venv's python):
    venv-grounding/bin/python scripts/compare_grounding.py claude-haiku
    venv-grounding/bin/python scripts/compare_grounding.py claude-haiku --csv data/test_sample.csv
"""

import argparse
import csv
import json
import os

from PIL import Image

from config import RESULTS_DIR, SCENE_GRAPH_DIR, SCENE_GRAPH_VIZ_DIR
from run_grounding import collect_groundable_items

COMPARISON_DIR = "data/grounding_comparison"


def load_scene_graph(detector: str, model_name: str, image_id: str):
    path = os.path.join(SCENE_GRAPH_DIR, detector, model_name, f"{image_id}.json")
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def summarize(scene_graph: dict) -> dict:
    object_nodes = [n for n in scene_graph["nodes"] if n["type"] in ("object", "accessory", "clothing")]
    scores = [n["score"] for n in object_nodes]
    return {
        "n_detected": len(object_nodes),
        "avg_score": round(sum(scores) / len(scores), 3) if scores else None,
    }


def make_side_by_side(model_name: str, image_id: str, out_path: str) -> bool:
    dino_path = os.path.join(SCENE_GRAPH_VIZ_DIR, "grounding_dino", model_name, f"{image_id}.jpg")
    owlv2_path = os.path.join(SCENE_GRAPH_VIZ_DIR, "owlv2", model_name, f"{image_id}.jpg")
    if not (os.path.exists(dino_path) and os.path.exists(owlv2_path)):
        return False

    left = Image.open(dino_path)
    right = Image.open(owlv2_path)
    height = max(left.height, right.height)
    combined = Image.new("RGB", (left.width + right.width + 10, height), (255, 255, 255))
    combined.paste(left, (0, 0))
    combined.paste(right, (left.width + 10, 0))
    combined.save(out_path)
    return True


def main(model_name: str, csv_path: str = None):
    results_dir = os.path.join(RESULTS_DIR, model_name)
    if not os.path.isdir(results_dir):
        print(f"No results found at {results_dir}.")
        return

    os.makedirs(COMPARISON_DIR, exist_ok=True)
    side_by_side_dir = os.path.join(COMPARISON_DIR, "side_by_side")
    os.makedirs(side_by_side_dir, exist_ok=True)

    result_files = sorted(f for f in os.listdir(results_dir) if f.endswith(".json"))
    if csv_path:
        import pandas as pd
        allowed_ids = set(pd.read_csv(csv_path)["Image_ID"].astype(str))
        result_files = [f for f in result_files if os.path.splitext(f)[0] in allowed_ids]

    rows = []
    totals = {"dino": {"detected": 0, "scores": []}, "owlv2": {"detected": 0, "scores": []}}
    n_phrases_total = 0

    for fname in result_files:
        image_id = os.path.splitext(fname)[0]

        dino_graph = load_scene_graph("grounding_dino", model_name, image_id)
        owlv2_graph = load_scene_graph("owlv2", model_name, image_id)
        if dino_graph is None or owlv2_graph is None:
            print(f"[SKIP] {image_id}: missing results for one or both detectors "
                  f"(dino={'yes' if dino_graph else 'no'}, owlv2={'yes' if owlv2_graph else 'no'})")
            continue

        with open(os.path.join(results_dir, fname), encoding="utf-8") as f:
            stage_result = json.load(f)
        n_phrases = len(collect_groundable_items(stage_result))
        n_phrases_total += n_phrases

        dino_summary = summarize(dino_graph)
        owlv2_summary = summarize(owlv2_graph)

        rows.append({
            "image_id": image_id,
            "n_phrases": n_phrases,
            "dino_detected": dino_summary["n_detected"],
            "dino_avg_score": dino_summary["avg_score"],
            "owlv2_detected": owlv2_summary["n_detected"],
            "owlv2_avg_score": owlv2_summary["avg_score"],
        })

        totals["dino"]["detected"] += dino_summary["n_detected"]
        totals["owlv2"]["detected"] += owlv2_summary["n_detected"]
        if dino_summary["avg_score"] is not None:
            totals["dino"]["scores"].append(dino_summary["avg_score"])
        if owlv2_summary["avg_score"] is not None:
            totals["owlv2"]["scores"].append(owlv2_summary["avg_score"])

        make_side_by_side(model_name, image_id, os.path.join(side_by_side_dir, f"{image_id}.jpg"))

    if not rows:
        print("No images have results from both detectors yet. Run run_grounding.py with both --detector values first.")
        return

    summary_path = os.path.join(COMPARISON_DIR, "summary.csv")
    with open(summary_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nWrote {summary_path} ({len(rows)} images) and side-by-side images to {side_by_side_dir}/\n")
    print(f"=== Aggregate over {len(rows)} images, {n_phrases_total} total phrases from Stage 2 ===")
    for name in ("dino", "owlv2"):
        detected = totals[name]["detected"]
        rate = round(100 * detected / n_phrases_total, 1) if n_phrases_total else 0
        avg_score = round(sum(totals[name]["scores"]) / len(totals[name]["scores"]), 3) if totals[name]["scores"] else None
        label = "Grounding DINO" if name == "dino" else "OWLv2"
        print(f"{label:16} detections={detected:4}  detection_rate={rate}%  avg_confidence={avg_score}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("model", help="Model name whose grounding results to compare (a folder under data/results/)")
    parser.add_argument("--csv", default=None, help="Optional CSV to restrict to a subset of Image_IDs")
    args = parser.parse_args()
    main(args.model, args.csv)