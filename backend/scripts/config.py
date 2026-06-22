"""
Configuration for VLMs served through vLLM (OpenAI-compatible API).

Each entry corresponds to a vLLM server running one model. If you only run
one model at a time on your server (typical for a single-GPU node), you
only need the entry that matches whatever you currently have served on
that port; the others are kept as reference for when you switch models.

To check structured-output (JSON schema) support for a given model/vLLM
version, see: https://docs.vllm.ai/en/latest/features/structured_outputs/
If a model raises a 400 error mentioning unsupported schema features when
you run the experiment, set its "supports_structured_output" to False —
the pipeline will fall back to plain prompting + automatic JSON repair.
"""

MODEL_REGISTRY = {
    # Port 8000 ("GPU slot 0"): pick ONE of these two Llama checkpoints to serve there.
    "llama3.2-vision-11b": {
        "base_url": "http://localhost:8000/v1",
        "model_id": "meta-llama/Llama-3.2-11B-Vision-Instruct",
        "supports_structured_output": True,
    },
    "llama4-scout": {
        "base_url": "http://localhost:8000/v1",
        "model_id": "meta-llama/Llama-4-Scout-17B-16E-Instruct",
        "supports_structured_output": True,
    },
    # Port 8001 ("GPU slot 1")
    "deepseek-vl2": {
        "base_url": "http://localhost:8001/v1",
        "model_id": "deepseek-ai/deepseek-vl2",
        "supports_structured_output": False,
    },
    # Port 8002 ("GPU slot 2")
    "qwen3-vl-8b": {
        "base_url": "http://localhost:8002/v1",
        "model_id": "Qwen/Qwen3-VL-8B-Instruct",
        "supports_structured_output": True,
    },
}

CSV_PATH = "data/metadata.csv"
IMAGES_DIR = "data/images"
RESULTS_DIR = "results"
GLOSSARY_OUTPUT_PATH = "data/discovered_glossary.json"  # output, built automatically by Stage 2b — see glossary.py

# --- Retrieval-augmented generation (RAG) over your document corpus ---
# Applies only to Stages 3 and 4 (narrative and mayeutics), not to Stages 1-2,
# which must stay purely visual per the methodology.
RAG_ENABLED = True
RAG_TOP_K = 4                      # chunks retrieved per query
CORPUS_DIR = "data/corpus"         # put your articles/PDFs/texts here
INDEX_DIR = "data/index"           # built by ingest_corpus.py
EMBEDDING_MODEL_NAME = "BAAI/bge-m3"  # multilingual, runs locally, no external API
CHUNK_SIZE = 1000                  # characters per chunk, approx.
CHUNK_OVERLAP = 150                # characters of overlap between chunks