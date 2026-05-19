"""
download_corpus.py — Step 1 del pipeline: descarga el corpus de imágenes
desde ARCA y extrae metadata técnica.

Para cada fila del sheet `inventory_metadata` con `Link` válido:
    1. Encuentra el UUID del asset en el HTML de la página de la obra.
    2. Si la imagen no está en data/corpus/<Image_ID>.<ext>, la descarga
       desde apiarca.uniandes.edu.co.
    3. La analiza con PIL: dimensiones, modo, tamaño en KB, contraste global.
    4. Escribe los 4 valores en las columnas correspondientes del sheet.

Match de columnas case-insensitive (igual que arca_scraper.py).

Uso:
    python download_corpus.py             # solo filas no procesadas
    python download_corpus.py --overwrite # re-procesa todas
"""

from __future__ import annotations

import argparse
import logging
import re
import time
from io import BytesIO
from pathlib import Path

import gspread
import requests
from google.oauth2.service_account import Credentials
from PIL import Image, ImageStat

# ────────────────────────────────────────────────────────────────────────────
# Configuración
# ────────────────────────────────────────────────────────────────────────────

SHEET_ID = "1ub4_RRp4sZ7yRgg8NuSim8WWTJ7Feex30WwRgoB8lUY"
WORKSHEET_NAME = "inventory_metadata"
CREDENTIALS_FILE = "credentials.json"

# Carpeta donde se guardan las imágenes. Path relativo al directorio desde
# donde se corre el script (typically data/, así corpus = data/corpus/).
CORPUS_DIR = Path("corpus")

# Headers que este script lee y escribe (match case-insensitive)
LINK_COLUMN     = "Link"
IMAGE_ID_COLUMN = "Image_ID"
FILLED_COLUMNS = [
    "Dimensions (Px)",
    "Mode (RGB/L/CMYK)",
    "File_Size (KB)",
    "Mean_Contrast",
]
# Columna que sirve para detectar "ya procesada" (si tiene valor, skip).
PROCESSED_MARKER_COL = "Dimensions (Px)"

# Cortesía con el servidor de ARCA
SLEEP_BETWEEN_OBRAS = 1.0  # segundos
USER_AGENT = "Mozilla/5.0 (research-scraper arca-corpus/1.0)"

# Versión del asset a descargar:
#   - "key=obra" → versión optimizada para web (~1500px lado mayor)
#   - "download" → original sin transformar (suele ser muy grande)
# Para análisis de YOLO/VLM/contraste, "key=obra" alcanza y pesa la 10ma parte.
ASSET_KEY = "key=obra"

# ────────────────────────────────────────────────────────────────────────────
# Logging
# ────────────────────────────────────────────────────────────────────────────

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
    if not Path(CREDENTIALS_FILE).exists():
        raise FileNotFoundError(
            f"No se encontró {CREDENTIALS_FILE}. Debe estar al lado del script."
        )
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
    return gspread.authorize(creds)


def open_worksheet(gc: gspread.Client):
    sh = gc.open_by_key(SHEET_ID)
    return sh.worksheet(WORKSHEET_NAME)


# ────────────────────────────────────────────────────────────────────────────
# Descarga + análisis de la imagen
# ────────────────────────────────────────────────────────────────────────────

# UUID del asset (Directus usa UUID v4: 36 chars con guiones)
ASSET_UUID_RE = re.compile(
    r"apiarca\.uniandes\.edu\.co/assets/([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-"
    r"[a-f0-9]{4}-[a-f0-9]{12})"
)

EXT_MAP = {
    "JPEG": "jpg", "JPG": "jpg",
    "PNG":  "png",
    "TIFF": "tiff", "TIF": "tiff",
    "BMP":  "bmp",
    "GIF":  "gif",
    "WEBP": "webp",
}


def find_asset_url(page_url: str, session: requests.Session) -> str:
    """
    Busca el UUID del asset en el HTML inicial de la obra y arma el URL de
    descarga. ARCA es Nuxt SSR, así que el UUID viene en el HTML aunque la
    página completa requiera JS.
    """
    resp = session.get(page_url, timeout=30, headers={"User-Agent": USER_AGENT})
    resp.raise_for_status()
    m = ASSET_UUID_RE.search(resp.text)
    if not m:
        raise RuntimeError(f"No encontré UUID del asset en el HTML de {page_url}")
    uuid = m.group(1)
    return f"https://apiarca.uniandes.edu.co/assets/{uuid}?{ASSET_KEY}"


def analyze_image(img_bytes: bytes) -> dict:
    """Extrae metadata técnica de una imagen en bytes."""
    img = Image.open(BytesIO(img_bytes))
    img.load()  # forzar lectura completa

    fmt = (img.format or "JPEG").upper()
    ext = EXT_MAP.get(fmt, fmt.lower())

    # Contraste global = desviación estándar de luminancia (canal L).
    # Es métrica clásica de "qué tan contrastada" es una imagen: 0 = uniforme,
    # ~80 = muy contrastada. La calculo sobre la versión convertida a gris.
    img_gray = img.convert("L")
    stat = ImageStat.Stat(img_gray)
    mean_contrast = round(stat.stddev[0], 2)

    return {
        "_format_ext": ext,
        "Dimensions (Px)":    f"{img.width}x{img.height}",
        "Mode (RGB/L/CMYK)":  img.mode,
        "File_Size (KB)":     round(len(img_bytes) / 1024, 1),
        "Mean_Contrast":      mean_contrast,
    }


def get_or_download(
    page_url: str, image_id: str, dest_dir: Path, session: requests.Session,
) -> tuple[bytes, Path]:
    """
    Si la imagen ya está en disco, la lee. Si no, busca el URL del asset,
    la descarga y la guarda. Devuelve (bytes, ruta).
    """
    existing = sorted(dest_dir.glob(f"{image_id}.*"))
    if existing:
        path = existing[0]
        log.info("  · ya en disco: %s", path)
        return path.read_bytes(), path

    asset_url = find_asset_url(page_url, session)
    log.info("  ↓ %s", asset_url)
    resp = session.get(asset_url, timeout=60, headers={"User-Agent": USER_AGENT})
    resp.raise_for_status()
    img_bytes = resp.content

    # Determinar extensión basada en lo que devolvió el servidor
    fmt = (Image.open(BytesIO(img_bytes)).format or "JPEG").upper()
    ext = EXT_MAP.get(fmt, "jpg")

    dest_dir.mkdir(parents=True, exist_ok=True)
    path = dest_dir / f"{image_id}.{ext}"
    path.write_bytes(img_bytes)
    log.info("  💾 guardada: %s (%d KB)", path, len(img_bytes) // 1024)
    return img_bytes, path


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

    # Resolver columnas (case-insensitive)
    if LINK_COLUMN.lower() not in headers_ci:
        raise RuntimeError(
            f"No encontré columna '{LINK_COLUMN}'. Headers: {headers}"
        )
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

        # Image_ID: del sheet, o lo derivamos del URL
        if image_id_idx is not None and image_id_idx < len(row):
            image_id = row[image_id_idx].strip()
        else:
            image_id = ""
        if not image_id:
            m = re.search(r"/(\d+)/?$", url)
            if not m:
                log.warning("Fila %d: sin Image_ID ni derivable de URL, skip", i)
                continue
            image_id = m.group(1)

        # ¿Ya procesada?
        already = bool(
            marker_idx is not None
            and marker_idx < len(row)
            and row[marker_idx].strip()
        )
        if already and not overwrite:
            log.info("Fila %d (%s): ya procesada, skip", i, image_id)
            skipped += 1
            continue

        log.info("Fila %d → obra %s", i, image_id)
        try:
            img_bytes, _ = get_or_download(url, image_id, CORPUS_DIR, session)
            meta = analyze_image(img_bytes)

            # Escribir las 4 columnas
            updates = []
            for col_name in FILLED_COLUMNS:
                actual = headers_ci.get(col_name.lower().strip())
                if not actual:
                    continue
                value = meta.get(col_name, "")
                if value == "":
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
    parser = argparse.ArgumentParser(description="Descarga corpus ARCA + metadata")
    parser.add_argument("--overwrite", action="store_true",
                        help="Re-procesa filas que ya tengan Dimensions")
    args = parser.parse_args()
    main(overwrite=args.overwrite)