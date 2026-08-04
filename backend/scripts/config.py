"""
Configuration for all VLM backends:
  - Anthropic API (Claude models, online, requires ANTHROPIC_API_KEY in .env)
  - vLLM (open-source models on a GPU server, OpenAI-compatible endpoint)
  - Ollama (local Mac testing, OpenAI-compatible endpoint)

Add ANTHROPIC_API_KEY=sk-ant-... to the .env file at the root of the project
(same file that already holds DRIVE_PATH and HF_TOKEN). docker-compose passes
it to the container automatically via env_file: .env — nothing else to change.

Pick which model to use by passing its key to run_experiment.py:
    python scripts/run_experiment.py claude-sonnet
    python scripts/run_experiment.py llama3.2-vision-local --csv data/test_sample.csv
    python scripts/run_experiment.py qwen3-vl-8b
"""

MODEL_REGISTRY = {
    # --- Anthropic (Claude, online API) ---
    # No server to start — just needs ANTHROPIC_API_KEY in .env.
    # Use claude-sonnet for the full experiment (best cost/quality balance);
    # claude-haiku for quick cheap tests; claude-opus for maximum depth.
    "claude-sonnet": {
        "provider": "anthropic",
        "model_id": "claude-sonnet-4-6",
        "supports_structured_output": False,  # uses prompt-based JSON extraction, which works well for Claude
    },
    "claude-haiku": {
        "provider": "anthropic",
        "model_id": "claude-haiku-4-5-20251001",
        "supports_structured_output": False,
    },

    # --- Local Mac testing (Ollama, no GPU needed) ---
    # Ollama runs directly on the Mac host; from inside the container it is
    # reached via host.docker.internal, not localhost.
    # Install: brew install ollama && ollama pull llama3.2-vision
    "llama3.2-vision-local": {
        "provider": "openai_compat",
        "base_url": "http://host.docker.internal:11434/v1",
        "model_id": "llama3.2-vision",
        "supports_structured_output": False,
    },

    # --- GPU server (vLLM) ---
    # Port 8000 — pick ONE of these two Llama checkpoints to serve there.
    "llama3.2-vision-11b": {
        "provider": "openai_compat",
        "base_url": "http://localhost:8000/v1",
        "model_id": "meta-llama/Llama-3.2-11B-Vision-Instruct",
        "supports_structured_output": True,
    },
    "llama4-scout": {
        "provider": "openai_compat",
        "base_url": "http://localhost:8000/v1",
        "model_id": "meta-llama/Llama-4-Scout-17B-16E-Instruct",
        "supports_structured_output": True,
    },
    # Port 8001
    "deepseek-vl2": {
        "provider": "openai_compat",
        "base_url": "http://localhost:8001/v1",
        "model_id": "deepseek-ai/deepseek-vl2",
        "supports_structured_output": False,
    },
    # Port 8002
    "qwen3-vl-8b": {
        "provider": "openai_compat",
        "base_url": "http://localhost:8002/v1",
        "model_id": "Qwen/Qwen3-VL-8B-Instruct",
        "supports_structured_output": True,
    },
}

# --- Paths matched to the ColonialArt-VLM project's existing structure ---
# IMPORTANT: "data/corpus" is already used for the artwork IMAGES synced from
# Google Drive (docker-compose mounts DRIVE_PATH there). The RAG article corpus
# therefore lives in data/articles to avoid any collision.
# Your full metadata CSV. The scraper writes it into the Google Drive folder,
# which docker-compose mounts at data/corpus. Step 2 of the README copies it
# to data/ so the pipeline reads a stable local file rather than a synced one.
CSV_PATH = "data/metadata.csv"
IMAGES_DIR = "data/corpus"       # artwork images — NOT the article corpus
RESULTS_DIR = "data/results"
GLOSSARY_OUTPUT_PATH = "data/discovered_glossary.json"

# --- Retrieval-augmented generation (RAG) ---
RAG_ENABLED = True
RAG_TOP_K = 4
ARTICLES_DIR = "data/articles"   # your PDFs and scraped web texts
INDEX_DIR = "data/index"
EMBEDDING_MODEL_NAME = "BAAI/bge-m3"
CHUNK_SIZE = 1000
CHUNK_OVERLAP = 150
