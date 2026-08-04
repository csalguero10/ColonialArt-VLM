"""
Fetches web articles (not PDFs) and saves their main text content into
data/articles/, so ingest_corpus.py can pick them up exactly like any
other document — no changes needed there.

Add one URL per line to data/web_sources.txt (blank lines and lines
starting with # are ignored), then run:
    python ingest_web_sources.py

Uses trafilatura to extract the main article content and strip site
navigation, menus, and footer boilerplate automatically — this matters
for WordPress-style academic sites (like Los ingenios del pincel), which
wrap the actual article in a lot of menu/carousel chrome.
"""

import os
import re
import time

import requests
import trafilatura

from config import ARTICLES_DIR

SOURCES_FILE = "data/web_sources.txt"
REQUEST_DELAY_SECONDS = 1.0  # be polite to the servers you're fetching from
HEADERS = {"User-Agent": "Mozilla/5.0 (research corpus builder; personal academic use)"}


def slugify(url: str) -> str:
    slug = url.rstrip("/").split("/")[-1]
    slug = re.sub(r"[^a-zA-Z0-9\-_]", "_", slug) or "page"
    return slug


def fetch_and_extract(url: str) -> str:
    response = requests.get(url, headers=HEADERS, timeout=20)
    response.raise_for_status()
    text = trafilatura.extract(
        response.text,
        include_comments=False,
        include_tables=False,
        favor_recall=True,
    )
    return text or ""


def main():
    if not os.path.exists(SOURCES_FILE):
        print(f"{SOURCES_FILE} not found. Create it with one URL per line and re-run.")
        return

    os.makedirs(ARTICLES_DIR, exist_ok=True)

    with open(SOURCES_FILE, "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip() and not line.strip().startswith("#")]

    if not urls:
        print(f"{SOURCES_FILE} is empty. Add one URL per line and re-run.")
        return

    for url in urls:
        slug = slugify(url)
        out_path = os.path.join(ARTICLES_DIR, f"{slug}.txt")
        if os.path.exists(out_path):
            print(f"[SKIP] {slug}.txt already exists.")
            continue

        try:
            text = fetch_and_extract(url)
        except Exception as e:
            print(f"[ERROR] {url}: {e}")
            continue

        if not text.strip():
            print(f"[WARN] No content extracted from {url} — this page's structure may need a different extractor.")
            continue

        with open(out_path, "w", encoding="utf-8") as f:
            f.write(f"Source: {url}\n\n{text}")

        print(f"[OK] Saved {slug}.txt ({len(text)} characters)")
        time.sleep(REQUEST_DELAY_SECONDS)


if __name__ == "__main__":
    main()
