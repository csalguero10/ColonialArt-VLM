# Running the experiment on open-source VLMs

## 1. Install and start vLLM on your server

```bash
pip install vllm
```

Serve **one model at a time** unless you have multiple GPUs and want separate servers on
different ports. Check your GPU's VRAM before picking a checkpoint — vision-language models
need extra memory for the image encoder, and each image injects extra tokens into the KV cache
on top of what a same-sized text-only model would need.

### Llama 3.2 Vision (11B — more modest hardware requirements)
```bash
vllm serve meta-llama/Llama-3.2-11B-Vision-Instruct --port 8000
```

### Llama 4 Scout (newer, MoE, needs significantly more VRAM — alternative to the one above, same port)
```bash
vllm serve meta-llama/Llama-4-Scout-17B-16E-Instruct --port 8000
```

### DeepSeek-VL2
```bash
vllm serve deepseek-ai/deepseek-vl2 --port 8001 \
  --hf_overrides '{"architectures": ["DeepseekVLV2ForCausalLM"]}'
```

### Qwen3-VL (currently one of the strongest open VLMs, good default if unsure)
```bash
vllm serve Qwen/Qwen3-VL-8B-Instruct --port 8002
```

Both Llama-family models require a Hugging Face token with access to the gated checkpoint
(`huggingface-cli login` before serving). DeepSeek-VL2 and Qwen3-VL are openly downloadable.

## Multi-GPU setup (once you know your hardware)

If your server has more than one GPU, you can serve several models at once — one per GPU —
and compare them concurrently instead of one model at a time. Pin each server to a specific
GPU with `CUDA_VISIBLE_DEVICES`, matching the ports already set in `config.py`:

```bash
CUDA_VISIBLE_DEVICES=0 vllm serve meta-llama/Llama-3.2-11B-Vision-Instruct --port 8000 &
CUDA_VISIBLE_DEVICES=1 vllm serve deepseek-ai/deepseek-vl2 --port 8001 \
  --hf_overrides '{"architectures": ["DeepseekVLV2ForCausalLM"]}' &
CUDA_VISIBLE_DEVICES=2 vllm serve Qwen/Qwen3-VL-8B-Instruct --port 8002 &
```

Then, in separate terminal sessions (or with `tmux`/`screen`), run the experiment against each
model in parallel:

```bash
python run_experiment.py llama3.2-vision-11b
python run_experiment.py deepseek-vl2
python run_experiment.py qwen3-vl-8b
```

Each writes to its own `results/<model_name>/` folder, so nothing overwrites or conflicts
between runs. If you end up with only one GPU, ignore this section and serve one model at a
time as shown above, reusing port 8000 each time.

## 2. Point the pipeline at the server

In `config.py`, confirm `base_url` matches where vLLM is listening (`http://localhost:8000/v1`
if the Python script runs on the same machine as the server; otherwise use the server's
address, e.g. `http://<server-ip>:8000/v1`).

## 3. Prepare your data

```
pipeline/
  data/
    metadata.csv          <- your CSV, with the 21 original columns
    images/
      <Image_ID>.jpg       <- one file per row, named after Image_ID
```

## 4. Run

```bash
pip install -r requirements.txt
python run_experiment.py llama3.2-vision-11b
```

Results are saved incrementally to `results/<model_name>/<Image_ID>.json`. Re-running the same
command skips images that already have a saved result, so an interrupted run can simply be
resumed — useful if the server restarts mid-corpus.

To run the same 120 images through a different model for comparison, stop the vLLM server,
start it again with the next model, and run:

```bash
python run_experiment.py qwen3-vl-8b
```

## The glossary 

Stage 2b looks up Stage 2's generic descriptions against your article corpus, and every time it finds a specific term supported by **both** the retrieved text and the image, that term is appended to `data/discovered_glossary.json` automatically. This file is not something you build by hand: it grows on its own as the pipeline runs across your corpus, and it's deduplicated by term, with the source and a short justification for each one, plus which images it was seen in.

Treat it as a research output — a discovered, citation-backed vocabulary — rather than a
configuration file you maintain. There's nothing to set up here beyond making sure
`data/corpus/` has your articles and `ingest_corpus.py` has been run.

## Setting up retrieval over your document corpus (RAG)

Stages 3 and 4 can pull supporting excerpts from a collection of articles and documents you've
gathered, so the model's narrative and mayeutic readings are grounded in actual historiography
rather than purely visual speculation. Stages 1 and 2 never use this — they stay strictly visual,
consistent with the methodology.

1. Drop your articles into `data/corpus/` (PDFs, `.txt`, or `.md` files). Scanned PDFs need OCR
   first — this step only extracts text that's already selectable in the PDF.
2. Build the index once (and again whenever you add new documents):
   ```bash
   python ingest_corpus.py
   ```
   This downloads `BAAI/bge-m3` (a multilingual embedding model, good for Spanish/English/Italian
   academic text) the first time it runs, embeds every chunk, and saves a local FAISS index to
   `data/index/`. No external API calls — everything runs on your server.
3. That's it — `run_experiment.py` will automatically retrieve and inject relevant excerpts into
   Stage 3 and Stage 4 prompts, and record which source files were consulted for each result
   (`consulted_sources` field) for traceability.

To run an ablation (with vs. without retrieval, to check whether the corpus actually changes the
model's readings), set `RAG_ENABLED = False` in `config.py`, run the experiment again into a
separate `results/` subfolder, and compare.

## Notes

- **Structured output reliability varies by model.** If a model raises a 400 error mentioning
  unsupported JSON schema features, set its `"supports_structured_output"` to `False` in
  `config.py` — the pipeline falls back to plain prompting plus automatic JSON extraction and a
  one-shot repair call.
- **The friction agent (Call 3 in Stages 4a/4b) receives the image again**, not just the two
  text readings from Calls 1 and 2 — so it can verify the contested visual element directly
  rather than only arbitrating between two pieces of text.
- **Genre branching for Stage 4b** currently uses a simple keyword check on Theme/Category
  (`_detect_genre_branch` in `pipeline.py`). Once your controlled vocabulary for those CSV
  columns is finalized, tighten this check accordingly.
- Calls 1 and 2 within each Stage 4 sub-stage run sequentially, by design — this keeps the
  execution order easy to trace end-to-end for each figure, which matters if you need to debug
  or audit a specific result later. If wall-clock time becomes a bottleneck once you're running
  across multiple models, this is the first place to introduce concurrency
  (`concurrent.futures.ThreadPoolExecutor`), since vLLM's continuous batching handles concurrent
  requests to the same server efficiently.