"""
One-time script: extracts text from every document in data/articles/,
chunks it, embeds the chunks with a local multilingual embedding model,
and saves a FAISS index + metadata so the pipeline can retrieve relevant
excerpts at runtime.

Run this once, and again whenever you add new documents to the corpus:
    python ingest_corpus.py

Supports .pdf and plain text files (.txt, .md) — including the .txt files
produced by ingest_web_sources.py. For scanned PDFs without extractable
text, run them through OCR first (see the pdf skill / any OCR tool) before
placing them in data/articles/ — this script does not OCR.
"""

import os
import pickle

import faiss
import numpy as np
from pypdf import PdfReader
from sentence_transformers import SentenceTransformer

from config import ARTICLES_DIR, INDEX_DIR, EMBEDDING_MODEL_NAME, CHUNK_SIZE, CHUNK_OVERLAP


def extract_text(file_path: str) -> str:
    if file_path.lower().endswith(".pdf"):
        reader = PdfReader(file_path)
        return "\n".join(page.extract_text() or "" for page in reader.pages)
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        return f.read()


def chunk_text(text: str, chunk_size: int, overlap: int) -> list:
    """Groups paragraphs into chunks of roughly chunk_size characters,
    carrying a small overlap from the end of one chunk into the next so
    that ideas split across a chunk boundary aren't lost entirely."""
    paragraphs = [p.strip() for p in text.split("\n") if p.strip()]
    chunks, current = [], ""
    for para in paragraphs:
        if len(current) + len(para) <= chunk_size:
            current = (current + " " + para).strip()
        else:
            if current:
                chunks.append(current)
            current = (current[-overlap:] + " " + para).strip() if overlap else para
    if current:
        chunks.append(current)
    return chunks


def main():
    os.makedirs(INDEX_DIR, exist_ok=True)
    os.makedirs(ARTICLES_DIR, exist_ok=True)

    model = SentenceTransformer(EMBEDDING_MODEL_NAME)

    all_chunks = []  # list of {"text": ..., "source": ...}
    files = sorted(f for f in os.listdir(ARTICLES_DIR) if os.path.isfile(os.path.join(ARTICLES_DIR, f)))

    if not files:
        print(f"No files found in {ARTICLES_DIR}/. Add your articles/PDFs there (or run ingest_web_sources.py first) and re-run.")
        return

    for fname in files:
        path = os.path.join(ARTICLES_DIR, fname)
        try:
            text = extract_text(path)
        except Exception as e:
            print(f"[WARN] Could not read {fname}: {e}")
            continue
        for chunk in chunk_text(text, CHUNK_SIZE, CHUNK_OVERLAP):
            all_chunks.append({"text": chunk, "source": fname})

    print(f"Extracted {len(all_chunks)} chunks from {len(files)} files.")

    embeddings = model.encode(
        [c["text"] for c in all_chunks],
        normalize_embeddings=True,
        show_progress_bar=True,
    )
    embeddings = np.asarray(embeddings, dtype="float32")

    index = faiss.IndexFlatIP(embeddings.shape[1])  # cosine similarity via normalized inner product
    index.add(embeddings)

    faiss.write_index(index, os.path.join(INDEX_DIR, "corpus.faiss"))
    with open(os.path.join(INDEX_DIR, "corpus_metadata.pkl"), "wb") as f:
        pickle.dump(all_chunks, f)

    print(f"Index saved to {INDEX_DIR}/ ({len(all_chunks)} chunks from {len(files)} files).")


if __name__ == "__main__":
    main()