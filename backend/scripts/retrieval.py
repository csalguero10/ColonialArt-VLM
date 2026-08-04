"""
Runtime retrieval over the local FAISS index built by ingest_corpus.py.
Used only by Stages 3 and 4 of the pipeline — Stages 1-2 stay purely
visual and never call this module.
"""

import os
import pickle

import faiss
import numpy as np
from sentence_transformers import SentenceTransformer

from config import INDEX_DIR, EMBEDDING_MODEL_NAME, RAG_ENABLED, RAG_TOP_K

_model = None
_index = None
_metadata = None


def _load():
    global _model, _index, _metadata
    if _model is None:
        _model = SentenceTransformer(EMBEDDING_MODEL_NAME)
    if _index is None:
        index_path = os.path.join(INDEX_DIR, "corpus.faiss")
        meta_path = os.path.join(INDEX_DIR, "corpus_metadata.pkl")
        if not os.path.exists(index_path):
            raise FileNotFoundError(index_path)
        _index = faiss.read_index(index_path)
        with open(meta_path, "rb") as f:
            _metadata = pickle.load(f)


def retrieve(query: str, k: int = None) -> list:
    """Returns up to k chunks: [{"text": ..., "source": ..., "score": ...}].
    Returns an empty list if RAG_ENABLED is False, the query is empty, or
    the index hasn't been built yet — so the pipeline keeps working with
    or without retrieval (useful for an ablation: run once with RAG_ENABLED
    = True and once with False, to see whether the corpus actually changes
    the model's readings)."""
    if not RAG_ENABLED or not query.strip():
        return []

    k = k or RAG_TOP_K
    try:
        _load()
    except FileNotFoundError:
        print("[WARN] Corpus index not found — run ingest_corpus.py first. Continuing without RAG.")
        return []

    query_vec = _model.encode([query], normalize_embeddings=True)
    query_vec = np.asarray(query_vec, dtype="float32")
    scores, idxs = _index.search(query_vec, k)

    results = []
    for score, idx in zip(scores[0], idxs[0]):
        if idx == -1:
            continue
        chunk = _metadata[idx]
        results.append({"text": chunk["text"], "source": chunk["source"], "score": float(score)})
    return results


def format_for_prompt(chunks: list) -> str:
    """Formats retrieved chunks into a labeled block for prompt injection."""
    if not chunks:
        return "(no reference material retrieved)"
    return "\n\n".join(f"[Source: {c['source']}]\n{c['text']}" for c in chunks)
