"""
Configuration for VLMs served through vLLM (OpenAI-compatible API), plus
one entry for quick local testing on a Mac via Ollama.

Each entry corresponds to a server running one model. Pick which one to
use by passing its key as the argument to run_experiment.py — you don't
need to remove or comment out entries you're not using right now; they
just sit here as options.
"""

MODEL_REGISTRY = {
    # --- Local Mac testing (Ollama, no Docker GPU needed) ---
    # Ollama runs directly on the Mac host (Metal GPU access isn't passed
    # through to Docker containers), so from inside the container this is
    # reached via host.docker.internal, not localhost.
    "llama3.2-vision-local": {
        "base_url": "http://host.docker.internal:11434/v1",
        "model_id": "llama3.2-vision",
        "supports_structured_output": False,  # not confirmed for Ollama's OpenAI-compat layer; falls back to JSON repair
    },

    # --- Server / GPU deployment (vLLM) ---
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

# --- Paths matched to the ColonialArt-VLM project's existing structure ---
# IMPORTANT: "data/corpus" is already used by this project for the artwork
# IMAGES synced from Google Drive (see download_corpus.py, docker-compose's
# DRIVE_PATH mount). The RAG article corpus therefore lives in a
# differently named folder (data/articles) to avoid any collision.
CSV_PATH = "data/backup.csv"     # matches the existing gspread/backup.csv inventory
IMAGES_DIR = "data/corpus"       # the artwork images, Drive-synced — NOT the article corpus
RESULTS_DIR = "data/results"
GLOSSARY_OUTPUT_PATH = "data/discovered_glossary.json"  # output, built automatically by Stage 2b — see glossary.py

# --- Retrieval-augmented generation (RAG) over your document corpus ---
# Applies only to Stages 3 and 4 (narrative and mayeutics), not to Stages 1-2,
# which must stay purely visual per the methodology.
RAG_ENABLED = True
RAG_TOP_K = 4                      # chunks retrieved per query
ARTICLES_DIR = "data/articles"     # put your articles/PDFs/scraped web texts here — NOT data/corpus
INDEX_DIR = "data/index"           # built by ingest_corpus.py
EMBEDDING_MODEL_NAME = "BAAI/bge-m3"  # multilingual, runs locally, no external API
CHUNK_SIZE = 1000                  # characters per chunk, approx.
CHUNK_OVERLAP = 150                # characters of overlap between chunks