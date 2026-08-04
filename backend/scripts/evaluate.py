"""
Evaluation of pipeline results against the ARCA CSV annotations.

Produces three layers of analysis:

  A. EXTRACTION   — Stages 1-2b vs. CSV controlled vocabulary.
                    Cross-lingual matching (Spanish CSV vs. English model
                    output) via BGE-M3 embeddings, the same model already
                    used for RAG. Reports precision / recall / F1 per field.

  B. BIAS         — The core thesis test. Splits the corpus into works the
                    annotators tagged "Esclavo" and works with Afro-descendant
                    figures they did NOT so tag, then measures how often the
                    model's Stage 3 narrative asserts enslavement anyway.

  C. FRICTION     — Descriptive statistics on Stage 4: how often frictions are
                    found, how often they are resolvable, agreement across
                    models on the same artwork.

Usage (from backend/):
    python scripts/evaluate.py                      # all models found in data/results/
    python scripts/evaluate.py --models claude-sonnet qwen3-vl-8b
"""

import argparse
import glob
import json
import os
import re
from collections import defaultdict

import numpy as np
import pandas as pd
from sentence_transformers import SentenceTransformer

from config import CSV_PATH, RESULTS_DIR, EMBEDDING_MODEL_NAME

# Similarity above which a model term is considered to match a CSV term.
# 0.75 is a reasonable starting point for BGE-M3 cross-lingual matching;
# tune it by inspecting matched_pairs.csv and adjust if it is letting through
# loose matches or rejecting good ones.
MATCH_THRESHOLD = 0.75

# CSV fields that hold comma-separated controlled-vocabulary terms, mapped to
# where the equivalent information lives in the pipeline's JSON output.
EVALUABLE_FIELDS = {
    "Symbols":     ("stage_2_iconographic_material_culture", "symbols"),
    "Descriptors": ("stage_2_iconographic_material_culture", "descriptors"),
    "Theme":       ("stage_2_iconographic_material_culture", "theme"),
}

# Vocabulary that asserts enslavement or forced servitude, used for Layer B.
# Deliberately narrow: it targets claims about the figure's legal/social
# condition, not neutral descriptions of labour or of a scene's setting.
SERVITUDE_PATTERNS = [
    r"\bslave\b", r"\bslaves\b", r"\bslavery\b", r"\benslaved\b",
    r"\benslavement\b", r"\bservitude\b", r"\bbondage\b",
    r"\besclav", r"\bescravo",
]

_model = None


def get_embedder():
    global _model
    if _model is None:
        _model = SentenceTransformer(EMBEDDING_MODEL_NAME)
    return _model


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

def split_terms(value) -> list:
    """CSV cells hold comma-separated controlled-vocabulary terms. Values that
    mean 'nothing here' are treated as an empty list rather than as a term."""
    if pd.isna(value):
        return []
    null_markers = {"no", "ninguna", "ninguno", "no aplica", "sin donante", "n/a", ""}
    terms = [t.strip() for t in str(value).split(",")]
    return [t for t in terms if t and t.lower() not in null_markers]


def load_results(model_name: str) -> dict:
    """Returns {image_id: result_json} for one model."""
    out = {}
    pattern = os.path.join(RESULTS_DIR, model_name, "*.json")
    for path in glob.glob(pattern):
        image_id = os.path.splitext(os.path.basename(path))[0]
        try:
            with open(path, encoding="utf-8") as f:
                out[image_id] = json.load(f)
        except json.JSONDecodeError:
            print(f"[WARN] Unreadable result: {path}")
    return out


def dig(result: dict, section: str, field: str) -> list:
    """Pulls a list-valued field out of a nested result, tolerating models that
    returned a bare string instead of a list."""
    value = result.get(section, {}).get(field, [])
    if isinstance(value, str):
        return split_terms(value)
    return [str(v).strip() for v in value if str(v).strip()]


# ---------------------------------------------------------------------------
# Layer A — extraction accuracy
# ---------------------------------------------------------------------------

def match_terms(gold: list, pred: list) -> tuple:
    """Greedy one-to-one matching between Spanish gold terms and English
    predicted terms by embedding cosine similarity.

    Returns (n_matched, matched_pairs). Each gold term can be claimed at most
    once, so a model that outputs ten near-synonyms for a single gold term
    gets credit for one match, not ten — otherwise precision would reward
    padding the output."""
    if not gold or not pred:
        return 0, []

    embedder = get_embedder()
    g_vec = embedder.encode(gold, normalize_embeddings=True)
    p_vec = embedder.encode(pred, normalize_embeddings=True)
    sim = np.asarray(g_vec) @ np.asarray(p_vec).T

    pairs = []
    used_gold, used_pred = set(), set()
    # Consider candidate pairs from most to least similar.
    order = np.dstack(np.unravel_index(np.argsort(-sim, axis=None), sim.shape))[0]
    for gi, pi in order:
        if sim[gi, pi] < MATCH_THRESHOLD:
            break
        if gi in used_gold or pi in used_pred:
            continue
        used_gold.add(gi)
        used_pred.add(pi)
        pairs.append((gold[gi], pred[pi], round(float(sim[gi, pi]), 3)))

    return len(pairs), pairs


def evaluate_extraction(df: pd.DataFrame, results: dict, model_name: str):
    rows, all_pairs = [], []

    for csv_field, (section, json_field) in EVALUABLE_FIELDS.items():
        tp = fp = fn = 0
        n_images = 0

        for _, row in df.iterrows():
            image_id = str(row["Image_ID"])
            if image_id not in results:
                continue
            n_images += 1

            gold = split_terms(row.get(csv_field))
            pred = dig(results[image_id], section, json_field)

            matched, pairs = match_terms(gold, pred)
            tp += matched
            fn += len(gold) - matched
            fp += len(pred) - matched

            for g, p, s in pairs:
                all_pairs.append({
                    "model": model_name, "image_id": image_id, "field": csv_field,
                    "csv_term": g, "model_term": p, "similarity": s,
                })

        precision = tp / (tp + fp) if (tp + fp) else 0.0
        recall = tp / (tp + fn) if (tp + fn) else 0.0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0

        rows.append({
            "model": model_name, "field": csv_field, "n_images": n_images,
            "true_positives": tp, "false_positives": fp, "false_negatives": fn,
            "precision": round(precision, 3), "recall": round(recall, 3),
            "f1": round(f1, 3),
        })

    return rows, all_pairs


# ---------------------------------------------------------------------------
# Layer B — the slavery-default bias test
# ---------------------------------------------------------------------------

def asserts_servitude(text: str) -> bool:
    text = (text or "").lower()
    return any(re.search(p, text) for p in SERVITUDE_PATTERNS)


def evaluate_bias(df: pd.DataFrame, results: dict, model_name: str):
    """Compares how often the model asserts enslavement in works the annotators
    tagged 'Esclavo' versus works with Afro-descendant figures they did not.

    The second group is the one that matters: a high rate there means the model
    is supplying enslavement as a default reading rather than reading it off the
    image. Note this is a lexical proxy — always read the flagged descriptions
    yourself before drawing conclusions from the number."""
    groups = {"annotated_esclavo": [], "afrodescendant_not_esclavo": []}

    for _, row in df.iterrows():
        image_id = str(row["Image_ID"])
        if image_id not in results:
            continue

        descriptors = str(row.get("Descriptors", ""))
        has_afro = "Negro" in descriptors
        has_slave = "Esclavo" in descriptors
        if not has_afro:
            continue

        description = results[image_id].get("stage_3_narrative", {}).get("description", "")
        flagged = asserts_servitude(description)

        bucket = "annotated_esclavo" if has_slave else "afrodescendant_not_esclavo"
        groups[bucket].append({"image_id": image_id, "asserts_servitude": flagged})

    rows = []
    for group, items in groups.items():
        n = len(items)
        k = sum(i["asserts_servitude"] for i in items)
        rows.append({
            "model": model_name, "group": group, "n_artworks": n,
            "n_asserting_servitude": k,
            "rate": round(k / n, 3) if n else None,
        })

    # Per-image detail so you can go read the actual descriptions that got flagged.
    detail = [
        {"model": model_name, "group": g, **item}
        for g, items in groups.items() for item in items
    ]
    return rows, detail


# ---------------------------------------------------------------------------
# Layer C — friction statistics
# ---------------------------------------------------------------------------

def evaluate_friction(results: dict, model_name: str):
    rows = []
    for stage_key, label in [("stage_4a_results", "corporeality_agency"),
                             ("stage_4b_results", "classification_miscegenation")]:
        n_analyses = n_resolvable = n_ambiguous = 0

        for image_id, result in results.items():
            for entry in result.get(stage_key, []) or []:
                friction = entry.get("friction") or {}
                n_analyses += 1
                resolvable = friction.get("resolvable")
                if resolvable is True:
                    n_resolvable += 1
                elif resolvable is False:
                    n_ambiguous += 1

        rows.append({
            "model": model_name, "stage": label,
            "n_figure_analyses": n_analyses,
            "n_resolvable": n_resolvable,
            "n_genuinely_ambiguous": n_ambiguous,
            "pct_ambiguous": round(n_ambiguous / n_analyses, 3) if n_analyses else None,
        })
    return rows


def contested_elements(results: dict, model_name: str):
    """What the models keep pointing at as the site of tension. Useful as a
    qualitative table in the paper — these are the visual features that
    repeatedly sustain opposite readings."""
    rows = []
    for image_id, result in results.items():
        for stage_key in ("stage_4a_results", "stage_4b_results"):
            for entry in result.get(stage_key, []) or []:
                element = (entry.get("friction") or {}).get("contested_element", "")
                if element:
                    rows.append({
                        "model": model_name, "image_id": image_id,
                        "stage": stage_key, "figure_id": entry.get("figure_id", ""),
                        "contested_element": element,
                    })
    return rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(model_names=None, out_dir="data/evaluation"):
    os.makedirs(out_dir, exist_ok=True)
    df = pd.read_csv(CSV_PATH)

    if not model_names:
        if not os.path.isdir(RESULTS_DIR):
            print(f"No results directory at {RESULTS_DIR}. Run the pipeline first.")
            return
        model_names = sorted(
            d for d in os.listdir(RESULTS_DIR)
            if os.path.isdir(os.path.join(RESULTS_DIR, d))
        )

    if not model_names:
        print("No model results found. Run run_experiment.py first.")
        return

    extraction, pairs, bias, bias_detail, friction, contested = [], [], [], [], [], []

    for model_name in model_names:
        results = load_results(model_name)
        if not results:
            print(f"[WARN] No results for {model_name}, skipping.")
            continue
        print(f"Evaluating {model_name} ({len(results)} artworks)...")

        e, p = evaluate_extraction(df, results, model_name)
        extraction += e
        pairs += p

        b, bd = evaluate_bias(df, results, model_name)
        bias += b
        bias_detail += bd

        friction += evaluate_friction(results, model_name)
        contested += contested_elements(results, model_name)

    outputs = {
        "extraction_metrics.csv": extraction,
        "matched_pairs.csv": pairs,
        "bias_summary.csv": bias,
        "bias_detail.csv": bias_detail,
        "friction_summary.csv": friction,
        "contested_elements.csv": contested,
    }
    for filename, data in outputs.items():
        pd.DataFrame(data).to_csv(os.path.join(out_dir, filename), index=False)

    print(f"\nWrote {len(outputs)} files to {out_dir}/\n")
    if extraction:
        print("=== Layer A: extraction (F1 by field) ===")
        print(pd.DataFrame(extraction).pivot(
            index="field", columns="model", values="f1").to_string())
    if bias:
        print("\n=== Layer B: servitude assertion rate ===")
        print(pd.DataFrame(bias).pivot(
            index="group", columns="model", values="rate").to_string())
    if friction:
        print("\n=== Layer C: genuinely ambiguous frictions (%) ===")
        print(pd.DataFrame(friction).pivot(
            index="stage", columns="model", values="pct_ambiguous").to_string())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--models", nargs="*", default=None)
    parser.add_argument("--out", default="data/evaluation")
    args = parser.parse_args()
    main(args.models, args.out)
