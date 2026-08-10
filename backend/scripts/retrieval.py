"""
Runtime retrieval over the local FAISS index built by ingest_corpus.py.
Used only by Stages 3 and 4 of the pipeline — Stages 1-2 stay purely
visual and never call this module.
"""

import os
import pickle
import re

import faiss
import numpy as np
from sentence_transformers import SentenceTransformer

from config import INDEX_DIR, EMBEDDING_MODEL_NAME, RAG_ENABLED, RAG_TOP_K

_model = None
_index = None
_metadata = None
_chunk_words = None  # _significant_words(chunk["text"]) per chunk, same order/index as _metadata
_word_doc_freq = None  # word -> number of distinct chunks containing it, for find_title_matches()

# A word that shows up in more than this many chunks is too generic *in
# this corpus* to be a useful content-match signal, no matter how
# proper-noun-shaped it looks. Measured directly against this corpus:
# "black" turned up in 608/7852 chunks and "maria" in 156 (both unsurprising
# given the corpus's Afro-descendant/colonial-religious focus), while
# "austria" appeared in 14 and "asturias" in 4 — this threshold is set to
# keep genuinely rare words like the latter while dropping domain-common
# ones like the former, rather than hardcoding a fixed word list that would
# need updating by hand as the corpus grows.
_MAX_CONTENT_WORD_DOC_FREQ = 25

# Small, pragmatic stopword list (English + Spanish, the corpus's two main
# languages) for find_title_matches() below — filters out both generic
# connector words ("the"/"of"/"de"/"la") AND generic art-historical
# vocabulary ("portrait"/"painting"/"retrato"), so word-overlap reflects a
# genuine proper-noun or subject match, not two unrelated paintings both
# happening to be a "portrait" of someone. Caught in testing: "Black Artist
# Completing a Portrait of Maria Anna of Austria" matched
# "francis-williams-a-portrait-of-a-writer.txt" on "portrait" alone.
_STOPWORDS = {
    "the", "a", "an", "of", "and", "or", "in", "on", "at", "to", "for", "with", "by", "from",
    "de", "la", "el", "los", "las", "y", "en", "del", "al", "un", "una", "que", "su", "con", "por",
    "portrait", "painting", "picture", "image", "images", "scene", "figure", "artist", "artwork",
    "work", "colonial", "century", "unknown", "anonymous", "attributed",
    "retrato", "pintura", "escena", "figura", "imagen", "obra", "cuadro", "lienzo", "arte",
    "atribuido", "anonimo", "siglo",
}


def _load():
    global _model, _index, _metadata, _chunk_words, _word_doc_freq
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
        # Precomputed once per process rather than inside find_title_matches()
        # itself — that function runs many times per artwork (Stage 3, plus
        # once per Afro-descendant figure in Stage 4a/4b), and re-tokenizing
        # every chunk's full text on each call would add up.
        _chunk_words = [_significant_words(c["text"]) for c in _metadata]
        _word_doc_freq = {}
        for words in _chunk_words:
            for w in words:
                _word_doc_freq[w] = _word_doc_freq.get(w, 0) + 1


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


def _significant_words(text: str) -> set:
    """Lowercased words of length >= 4, minus stopwords — long enough to
    usually be a proper noun or a specific term rather than a generic
    connector, regardless of language."""
    words = re.findall(r"[^\W\d_]+", text.lower())
    return {w for w in words if len(w) >= 4 and w not in _STOPWORDS}


def find_title_matches(title: str, author: str = "", max_chunks_per_source: int = 3,
                       min_content_word_overlap: int = 2) -> list:
    """Looks for article material naming the same subject as this artwork's
    title/author, two ways:

    1. FILENAME match — the article's filename shares a significant word
       with the title/author, e.g. "Francis Williams, the Scholar of
       Jamaica" against "francis-williams-a-portrait-of-a-writer.txt". A
       filename is short and was deliberately chosen, so a single shared
       proper noun there is already a strong, low-noise signal — every
       chunk of a filename-matched article is included (up to the cap).
    2. CONTENT match — for articles whose filename doesn't give any hint
       (many PDFs are named after a journal/issue, not their subject), each
       chunk's own text is checked instead. Full body text is noisier than
       a filename — a common name like "Maria" can appear in passing in
       many unrelated chunks — so this requires at least
       min_content_word_overlap shared words landing in the SAME chunk,
       not just one, before counting it as a match.

    This exists because embedding similarity alone can under-rank an
    article that is *literally about this exact subject*: a proper noun
    carries most of its meaning in the name itself, which doesn't always
    outweigh generic thematic keywords in a semantic query. Comparing raw
    words instead of embeddings also makes this work across languages by
    construction — proper nouns are typically the one part of a title that
    doesn't get translated between an English and a Spanish source.

    Returns the same chunk shape as retrieve() (score fixed at 1.0, i.e.
    maximum confidence), capped at max_chunks_per_source per matched
    article so one very long PDF can't crowd out everything else."""
    if not RAG_ENABLED:
        return []
    query_words = _significant_words(title) | _significant_words(author)
    if not query_words:
        return []
    try:
        _load()
    except FileNotFoundError:
        return []

    filename_matched_sources = {
        source for source in {c["source"] for c in _metadata}
        if query_words & _significant_words(source)
    }

    # Filenames are few and deliberately chosen, so any shared word is a
    # decent signal there — but full body text is a much bigger sample, and
    # a common word appearing in the query (a name like "Maria", or "black"
    # in a corpus that's largely about Afro-descendant subjects) will
    # co-occur with something else by pure chance across thousands of
    # chunks. For content matching specifically, only count words that are
    # actually rare in this corpus, so overlap reflects real specificity.
    rare_query_words = {w for w in query_words if _word_doc_freq.get(w, 0) <= _MAX_CONTENT_WORD_DOC_FREQ}

    results, per_source_count = [], {}
    for chunk, chunk_words in zip(_metadata, _chunk_words):
        source = chunk["source"]
        if per_source_count.get(source, 0) >= max_chunks_per_source:
            continue
        is_match = (
            source in filename_matched_sources
            or len(rare_query_words & chunk_words) >= min_content_word_overlap
        )
        if not is_match:
            continue
        per_source_count[source] = per_source_count.get(source, 0) + 1
        results.append({"text": chunk["text"], "source": source, "score": 1.0})
    return results


def format_for_prompt(chunks: list) -> str:
    """Formats retrieved chunks into a labeled block for prompt injection."""
    if not chunks:
        return "(no reference material retrieved)"
    return "\n\n".join(f"[Source: {c['source']}]\n{c['text']}" for c in chunks)
