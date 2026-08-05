"""
Builds a single wide CSV combining the ground-truth metadata with each
model's results side by side — one Afro_Presence / VLM_Detection /
Bias_Deviation triplet of columns per model, prefixed with the model name.

This does NOT modify metadata.csv. It reads it plus every folder under
data/results/ and writes a new file, so metadata.csv stays a clean,
untouched ground-truth reference.

Usage (from backend/):
    python scripts/build_comparison_csv.py
    python scripts/build_comparison_csv.py --models claude-sonnet qwen3-vl-8b
"""

import argparse
import glob
import json
import os
import re

import pandas as pd

from config import CSV_PATH, RESULTS_DIR

SERVITUDE_PATTERNS = [
    r"\bslave\b", r"\bslaves\b", r"\bslavery\b", r"\benslaved\b",
    r"\benslavement\b", r"\bservitude\b", r"\bbondage\b",
    r"\besclav", r"\bescravo",
]


def load_results(model_name: str) -> dict:
    out = {}
    for path in glob.glob(os.path.join(RESULTS_DIR, model_name, "*.json")):
        image_id = os.path.splitext(os.path.basename(path))[0]
        try:
            with open(path, encoding="utf-8") as f:
                out[image_id] = json.load(f)
        except json.JSONDecodeError:
            print(f"[WARN] Unreadable result: {path}")
    return out


def afro_presence(result: dict):
    stage1 = result.get("stage_1_pre_iconographic", {})
    if "afrodescendant_present" in stage1:
        return stage1["afrodescendant_present"]
    return None


def vlm_detection_summary(result: dict) -> str:
    """Short human-readable summary of what the model found, for quick
    spreadsheet scanning — not a substitute for reading the full JSON."""
    stage1 = result.get("stage_1_pre_iconographic", {})
    n = stage1.get("number_of_afrodescendants")
    desc = result.get("stage_3_narrative", {}).get("description", "")
    snippet = (desc[:120] + "...") if len(desc) > 120 else desc
    return f"n_afrodescendants={n}; {snippet}"


def bias_deviation(result: dict) -> bool:
    """True if the model's narrative asserts enslavement/servitude —
    the lexical proxy used in evaluate.py's Layer B. Read alongside the
    ground-truth 'Esclavo' descriptor by hand before drawing conclusions."""
    text = result.get("stage_3_narrative", {}).get("description", "").lower()
    return any(re.search(p, text) for p in SERVITUDE_PATTERNS)


def main(model_names=None, out_path="data/evaluation/full_comparison.csv"):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    df = pd.read_csv(CSV_PATH)
    df["Image_ID"] = df["Image_ID"].astype(str)

    if not model_names:
        model_names = sorted(
            d for d in os.listdir(RESULTS_DIR)
            if os.path.isdir(os.path.join(RESULTS_DIR, d))
        ) if os.path.isdir(RESULTS_DIR) else []

    if not model_names:
        print("No model results found under data/results/. Run run_experiment.py first.")
        return

    for model_name in model_names:
        results = load_results(model_name)
        print(f"{model_name}: {len(results)} results loaded")

        df[f"{model_name}_Afro_Presence"] = df["Image_ID"].map(
            lambda iid: afro_presence(results[iid]) if iid in results else None)
        df[f"{model_name}_VLM_Detection"] = df["Image_ID"].map(
            lambda iid: vlm_detection_summary(results[iid]) if iid in results else "")
        df[f"{model_name}_Bias_Deviation"] = df["Image_ID"].map(
            lambda iid: bias_deviation(results[iid]) if iid in results else None)

    df.to_csv(out_path, index=False)
    print(f"\nWrote {out_path} ({len(df)} rows, {len(model_names)} model(s) compared)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--models", nargs="*", default=None)
    parser.add_argument("--out", default="data/evaluation/full_comparison.csv")
    args = parser.parse_args()
    main(args.models, args.out)
