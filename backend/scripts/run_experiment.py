"""
Main entry point. Loops over every row of the CSV, runs the full pipeline
for the chosen model, and saves one JSON result per image. Already-
processed images are skipped, so an interrupted run can simply be
re-launched with the same command.

Usage (run from backend/, so relative paths resolve to backend/data/...):
    python scripts/run_experiment.py llama3.2-vision-11b
    python scripts/run_experiment.py deepseek-vl2
    python scripts/run_experiment.py qwen3-vl-8b

For a quick local test on a handful of images, point --csv at a small
sample file instead of the full corpus:
    python scripts/run_experiment.py llama3.2-vision-local --csv data/test_sample.csv
"""

import argparse
import json
import os

import pandas as pd
from tqdm import tqdm

from config import MODEL_REGISTRY, CSV_PATH, IMAGES_DIR, RESULTS_DIR
from pipeline import analyze_artwork


def main(model_name: str, csv_path: str = None):
    if model_name not in MODEL_REGISTRY:
        raise ValueError(f"Unknown model '{model_name}'. Options: {list(MODEL_REGISTRY)}")

    model_config = MODEL_REGISTRY[model_name]
    df = pd.read_csv(csv_path or CSV_PATH)

    out_dir = os.path.join(RESULTS_DIR, model_name)
    os.makedirs(out_dir, exist_ok=True)

    for _, row in tqdm(df.iterrows(), total=len(df), desc=model_name):
        image_id = str(row["Image_ID"])
        out_path = os.path.join(out_dir, f"{image_id}.json")

        if os.path.exists(out_path):
            continue  # checkpoint: already processed

        image_path = os.path.join(IMAGES_DIR, f"{image_id}.jpg")
        if not os.path.exists(image_path):
            print(f"[WARN] Image not found for {image_id}, skipping.")
            continue

        csv_row = row.fillna("").to_dict()

        try:
            result = analyze_artwork(model_config, image_path, csv_row)
        except Exception as e:
            print(f"[ERROR] {image_id} with {model_name}: {e}")
            continue

        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("model", choices=list(MODEL_REGISTRY.keys()))
    parser.add_argument("--csv", default=None, help="Optional path to a smaller CSV for quick local tests")
    args = parser.parse_args()
    main(args.model, args.csv)
