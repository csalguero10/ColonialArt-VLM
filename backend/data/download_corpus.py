"""
download_corpus.py — Step 1 del pipeline: descarga el corpus de imágenes
y extrae metadata técnica.

Soporta múltiples fuentes:
    - ARCA (arca.uniandes.edu.co) — Nuxt SSR, UUID en el HTML inicial.
    - Wikipedia (en/es/...wikipedia.org) — usa el REST API summary.
    - Cualquier otro sitio — busca el <img> más grande del HTML estático.

Para cada fila del sheet `inventory_metadata` con `Link` válido:
    1. Detecta la fuente por el dominio.
    2. Resuelve el URL real de la imagen.
    3. Si la imagen no está en disco, la descarga a data/corpus/<Image_ID>.<ext>.
    4. La analiza con PIL: dimensiones, modo, tamaño en KB, contraste global.
    5. Escribe los valores en las columnas correspondientes del sheet.

Skip automático: si el archivo ya existe Y la fila tiene `Dimensions (Px)`,
no se re-procesa. `--overwrite` ignora ese skip.

Uso:
    python download_corpus.py
    python download_corpus.py --overwrite
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
import time
from io import BytesIO
from pathlib import Path
from urllib.parse import urlparse, unquote

import gspread
import requests
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials
from PIL import Image

# Hacemos accesible el paquete `src/` cuando se ejecuta desde data/
SCRIPT_DIR = Path(__file__).resolve().parent
BACKEND_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(BACKEND_ROOT))

from src.metadata_pil import analyze_image_bytes  # noqa: E402

# ────────────────────────────────────────────────────────────────────────────
# Configuración
# ────────────────────────────────────────────────────────────────────────────

SHEET_ID = "1ub4_RRp4sZ7yRgg8NuSim8WWTJ7Feex30WwRgoB8lUY"
WORKSHEET_NAME = "inventory_metadata"
CREDENTIALS_FILE = SCRIPT_DIR / "credentials.json"
CORPUS_DIR = SCRIPT_DIR / "corpus"

LINK_COLUMN     = "Link"
IMAGE_ID_COLUMN = "Image_ID"
FILLED_COLUMNS = [
    "Dimensions (Px)",
    "Mode (RGB/L/CMYK)",
    "File_Size (KB)",
    "Mean_Contrast",
]
PROCESSED_MARKER_COL = "Dimensions (Px)"

SLEEP_BETWEEN_OBRAS = 1.0
USER_AGENT = (
    "Mozilla/5.0 (research-scraper colonial-art/1.0; "
    "contact: catalina.salguero@unibo.it)"
)

EXT_MAP = {
    "JPEG": "jpg", "JPG": "jpg",
    "PNG":  "png",
    "TIFF": "tiff", "TIF": "tiff",
    "BMP":  "bmp",
    "GIF":  "gif",
    "WEBP": "webp",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("corpus")


# ────────────────────────────────────────────────────────────────────────────
# Google Sheets
# ────────────────────────────────────────────────────────────────────────────

def get_gspread_client() -> gspread.Client:
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    if not CREDENTIALS_FILE.exists():
        raise FileNotFoundError(f"No se encontró {CREDENTIALS_FILE}")
    creds = Credentials.from_service_account_file(
        str(CREDENTIALS_FILE), scopes=scopes
    )
    return gspread.authorize(creds)


def open_worksheet(gc: gspread.Client):
    sh = gc.open_by_key(SHEET_ID)
    return sh.worksheet(WORKSHEET_NAME)


# ────────────────────────────────────────────────────────────────────────────
# Resolución de URLs según la fuente
# ────────────────────────────────────────────────────────────────────────────

ARCA_UUID_RE = re.compile(
    r"apiarca\.uniandes\.edu\.co/assets/([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-"
    r"[a-f0-9]{4}-[a-f0-9]{12})"
)


def _extract_arca(page_url: str, session: requests.Session) -> str:
    """ARCA: extrae UUID del HTML y arma URL del asset."""
    resp = session.get(page_url, timeout=30, headers={"User-Agent": USER_AGENT})
    resp.raise_for_status()
    m = ARCA_UUID_RE.search(resp.text)
    if not m:
        raise RuntimeError(f"No encontré UUID de ARCA en {page_url}")
    return f"https://apiarca.uniandes.edu.co/assets/{m.group(1)}?key=obra"


def _extract_wikipedia(page_url: str, session: requests.Session) -> str:
    """
    Wikipedia: usa la REST API que devuelve el `originalimage` o
    `thumbnail` del artículo. Mucho más confiable que parsear HTML.
    Maneja también URLs de tipo /wiki/...#/media/File:X.jpg.
    """
    parsed = urlparse(page_url)
    # /wiki/Foo  →  Foo
    path = parsed.path.split("/wiki/", 1)
    if len(path) != 2:
        raise RuntimeError(f"URL de Wikipedia no estándar: {page_url}")
    title = path[1]
    lang = parsed.netloc.split(".")[0] or "en"

    # Si es /wiki/Article#/media/File:Foo.jpg, intentamos primero la imagen
    # directa de Commons usando el nombre del archivo.
    if "#/media/File:" in unquote(page_url):
        file_name = unquote(page_url).split("#/media/File:", 1)[1].split("?", 1)[0]
        # API de Commons para obtener la URL de la imagen original
        api_url = (
            f"https://commons.wikimedia.org/w/api.php"
            f"?action=query&titles=File:{file_name}"
            f"&prop=imageinfo&iiprop=url&format=json"
        )
        r = session.get(api_url, headers={"User-Agent": USER_AGENT}, timeout=30)
        r.raise_for_status()
        pages = r.json().get("query", {}).get("pages", {})
        for _pid, page in pages.items():
            ii = page.get("imageinfo")
            if ii and ii[0].get("url"):
                return ii[0]["url"]

    # Si no, usamos el REST API summary del artículo
    api_url = f"https://{lang}.wikipedia.org/api/rest_v1/page/summary/{title}"
    r = session.get(api_url, headers={"User-Agent": USER_AGENT}, timeout=30)
    r.raise_for_status()
    data = r.json()
    if "originalimage" in data and data["originalimage"].get("source"):
        return data["originalimage"]["source"]
    if "thumbnail" in data and data["thumbnail"].get("source"):
        # Reemplazo el segmento "/thumb/.../<X>px-" para obtener original
        src = data["thumbnail"]["source"]
        src = re.sub(r"/thumb/", "/", src)
        src = re.sub(r"/\d+px-[^/]+$", "", src)
        return src
    raise RuntimeError(f"Wikipedia: el artículo {title} no tiene imagen principal")


def _extract_generic(page_url: str, session: requests.Session) -> str:
    """
    Fallback para cualquier sitio: parsea HTML estático con BeautifulSoup,
    busca el <meta property="og:image"> primero (suele ser la imagen
    "hero" de la página), luego el <img> más grande declarado en el HTML.
    """
    resp = session.get(page_url, timeout=30, headers={"User-Agent": USER_AGENT})
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    # (1) og:image — convención SEO para "la imagen representativa"
    og = soup.find("meta", property="og:image")
    if og and og.get("content"):
        return _absolute_url(og["content"], page_url)

    # (2) <link rel="image_src">
    link = soup.find("link", rel="image_src")
    if link and link.get("href"):
        return _absolute_url(link["href"], page_url)

    # (3) el <img> más grande declarado en el HTML (width × height)
    best = None
    best_area = 0
    for img in soup.find_all("img"):
        if not img.get("src"):
            continue
        try:
            w = int(img.get("width", 0))
            h = int(img.get("height", 0))
        except (TypeError, ValueError):
            w = h = 0
        area = w * h
        if area > best_area:
            best_area = area
            best = img.get("src")
    if best:
        return _absolute_url(best, page_url)

    # (4) último intento: cualquier <img>
    any_img = soup.find("img")
    if any_img and any_img.get("src"):
        return _absolute_url(any_img["src"], page_url)

    raise RuntimeError(f"No encontré ninguna <img> en {page_url}")


def _absolute_url(url: str, base: str) -> str:
    """Convierte /path o //host/path en URL absoluta usando el base."""
    if url.startswith("//"):
        return "https:" + url
    if url.startswith("/"):
        parsed = urlparse(base)
        return f"{parsed.scheme}://{parsed.netloc}{url}"
    return url


def resolve_image_url(page_url: str, session: requests.Session) -> str:
    """Despacha al extractor correcto según el dominio."""
    host = urlparse(page_url).netloc.lower()
    if "arca.uniandes.edu.co" in host:
        return _extract_arca(page_url, session)
    if "wikipedia.org" in host or "wikimedia.org" in host:
        return _extract_wikipedia(page_url, session)
    return _extract_generic(page_url, session)


# ────────────────────────────────────────────────────────────────────────────
# Descarga + análisis
# ────────────────────────────────────────────────────────────────────────────

def _find_existing(image_id: str, dest_dir: Path) -> Path | None:
    """Devuelve el path local de la imagen si ya existe (cualquier extensión)."""
    matches = sorted(dest_dir.glob(f"{image_id}.*"))
    return matches[0] if matches else None


def get_or_download(
    page_url: str, image_id: str, dest_dir: Path, session: requests.Session,
) -> Path:
    """Si ya está en disco devuelve el path. Si no, descarga y guarda."""
    existing = _find_existing(image_id, dest_dir)
    if existing:
        log.info("  · ya en disco: %s", existing.name)
        return existing

    img_url = resolve_image_url(page_url, session)
    log.info("  ↓ %s", img_url)
    resp = session.get(
        img_url, timeout=60,
        headers={"User-Agent": USER_AGENT, "Referer": page_url},
    )
    resp.raise_for_status()
    img_bytes = resp.content

    # Determinar formato leyendo los bytes
    img = Image.open(BytesIO(img_bytes))
    fmt = (img.format or "JPEG").upper()
    ext = EXT_MAP.get(fmt, "jpg")

    dest_dir.mkdir(parents=True, exist_ok=True)
    path = dest_dir / f"{image_id}.{ext}"
    path.write_bytes(img_bytes)
    log.info("  💾 guardada: %s (%d KB)", path.name, len(img_bytes) // 1024)
    return path


def _to_sheet_meta(meta: dict) -> dict:
    """Convierte el dict canónico de metadata_pil al formato del sheet."""
    return {
        "Dimensions (Px)":   f"{meta['width']}x{meta['height']}",
        "Mode (RGB/L/CMYK)": meta["mode"],
        "File_Size (KB)":    meta["size_kb"],
        "Mean_Contrast":     meta["contrast"],
    }


# ────────────────────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────────────────────

def main(overwrite: bool = False) -> None:
    gc = get_gspread_client()
    ws = open_worksheet(gc)

    rows = ws.get_all_values()
    if not rows:
        log.error("Sheet vacío")
        return

    headers = rows[0]
    headers_ci = {h.lower().strip(): h for h in headers}

    if LINK_COLUMN.lower() not in headers_ci:
        raise RuntimeError(f"No encontré columna '{LINK_COLUMN}'")

    link_idx     = headers.index(headers_ci[LINK_COLUMN.lower()])
    image_id_hdr = headers_ci.get(IMAGE_ID_COLUMN.lower())
    image_id_idx = headers.index(image_id_hdr) if image_id_hdr else None
    marker_hdr   = headers_ci.get(PROCESSED_MARKER_COL.lower())
    marker_idx   = headers.index(marker_hdr) if marker_hdr else None

    session = requests.Session()
    processed = skipped = errors = 0

    for i, row in enumerate(rows[1:], start=2):
        url = row[link_idx].strip() if link_idx < len(row) else ""
        if not url.lower().startswith("http"):
            continue

        # Image_ID: obligatorio para todos. Para ARCA podemos derivarlo de la
        # URL, para otros sitios debe estar puesto a mano en el sheet.
        image_id = ""
        if image_id_idx is not None and image_id_idx < len(row):
            image_id = row[image_id_idx].strip()
        if not image_id:
            m = re.search(r"/obras/(\d+)/?", url)
            if m:
                image_id = m.group(1)
            else:
                log.warning(
                    "Fila %d: sin Image_ID. Asígnale uno manualmente en el "
                    "sheet (ej. 90001, 90002…). URL: %s",
                    i, url,
                )
                continue

        # Skip estricto: solo si BOTH archivo en disco AND metadata en sheet
        file_exists = _find_existing(image_id, CORPUS_DIR) is not None
        metadata_done = bool(
            marker_idx is not None
            and marker_idx < len(row)
            and row[marker_idx].strip()
        )
        if file_exists and metadata_done and not overwrite:
            log.info("Fila %d (%s): ya procesada, skip", i, image_id)
            skipped += 1
            continue

        log.info("Fila %d → obra %s (fuente: %s)",
                 i, image_id, urlparse(url).netloc)
        try:
            img_path = get_or_download(url, image_id, CORPUS_DIR, session)
            meta = analyze_image_bytes(
                img_path.read_bytes(),
                file_size_bytes=img_path.stat().st_size,
            )
            sheet_meta = _to_sheet_meta(meta)

            updates = []
            for col_name, value in sheet_meta.items():
                actual = headers_ci.get(col_name.lower().strip())
                if not actual or value == "":
                    continue
                col_idx = headers.index(actual) + 1
                a1 = gspread.utils.rowcol_to_a1(i, col_idx)
                updates.append({"range": a1, "values": [[value]]})

            if updates:
                ws.batch_update(updates, value_input_option="USER_ENTERED")
                log.info("  ✓ %d columnas actualizadas", len(updates))

            processed += 1
            time.sleep(SLEEP_BETWEEN_OBRAS)
        except Exception as e:
            log.error("  ✗ %s", e)
            errors += 1

    log.info("")
    log.info("Resumen: procesadas=%d  skipped=%d  errores=%d",
             processed, skipped, errors)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Descarga corpus + metadata")
    parser.add_argument(
        "--overwrite", action="store_true",
        help="Re-procesa filas aunque ya tengan archivo y metadata",
    )
    args = parser.parse_args()
    main(overwrite=args.overwrite)