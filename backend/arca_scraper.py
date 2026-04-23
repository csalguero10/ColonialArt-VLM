"""
ARCA scraper — extrae metadatos de obras de https://arca.uniandes.edu.co
y los escribe en la hoja `inventory_metadata` de Google Sheets.

Flujo:
    1. Lee el sheet `inventory_metadata`.
    2. Para cada fila que tenga URL en la columna `Link` (y Title vacío),
       abre la página con Playwright (Nuxt + SSR hidratado por JS).
    3. Extrae campos del DOM renderizado (selectores basados en la
       estructura real de ARCA: <h3> como label + <p>/<ul> como valor).
    4. Normaliza y escribe SOLO las columnas Image_ID, Title, Author,
       Medium, Date, Location, Theme, Category, Descriptors — el resto
       de columnas (Dimensions, Afro_Presence, etc.) se deja intacto.

Mapeo de campos:
    Image_ID     ← id numérico del URL (/obras/8548 → "8548")
    Title        ← titulo (h1)
    Author       ← autor(es) en #autores
    Medium       ← tecnicas
    Date         ← fecha               (string; año "1826", rango
                                        "1700–1799" o texto libre)
    Location     ← ubicacion actual    (pipes "|" → ", ")
    Theme        ← personaje central o tema
    Category     ← relato visual o clasificacion
    Descriptors  ← descriptores
"""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import re
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import gspread
from google.oauth2.service_account import Credentials
from playwright.async_api import Page, async_playwright

# ────────────────────────────────────────────────────────────────────────────
# Configuración
# ────────────────────────────────────────────────────────────────────────────

SHEET_ID = "1ub4_RRp4sZ7yRgg8NuSim8WWTJ7Feex30WwRgoB8lUY"
WORKSHEET_NAME = "inventory_metadata"   # si es None, usa la primera hoja
CREDENTIALS_FILE = "credentials.json"   # service account JSON

# Columna del sheet de donde salen los URLs (por nombre de header).
LINK_COLUMN = "Link"

# Si es True, re-escribe filas que ya tengan datos. Si es False (default),
# solo procesa las filas que tienen Link pero no tienen Title.
OVERWRITE_EXISTING = False

# Carpeta para guardar HTML/JSON por obra en caso de debug
DEBUG_DIR = Path("debug")

# Columnas que el scraper llena. El resto (Dimensions, Afro_Presence, etc.)
# las deja intactas.
FILLED_COLUMNS = [
    "Image_ID", "Title", "Author", "Medium", "Date",
    "Location", "Theme", "Category", "Descriptors",
]

# Mapeo de campos → columna de salida en el sheet.
# Acepta tanto los nombres internos del API (Directus) como los labels
# visibles en el DOM (h3 de cada sección), normalizados con norm_label()
# (minúsculas, sin acentos, espacios colapsados).
FIELD_MAP = {
    # --- nombres internos del API ---
    "titulo":                    "Title",
    "autor":                     "Author",
    "autores":                   "Author",
    "tecnicas":                  "Medium",
    "fecha":                     "Date",
    "ubicacion":                 "Location",
    "personajes":                "Theme",
    "relato_visual":             "Category",
    "categorias":                "Category",
    "descriptores":              "Descriptors",
    # --- labels del DOM (tal como aparecen en los <h3>) ---
    "ubicacion actual":          "Location",
    "personaje central o tema":  "Theme",
    "relato visual":             "Category",
    "clasificacion":             "Category",
}

# ────────────────────────────────────────────────────────────────────────────
# Logging
# ────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("arca")


# ────────────────────────────────────────────────────────────────────────────
# Helpers de normalización
# ────────────────────────────────────────────────────────────────────────────

def strip_accents(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )


def norm_label(s: str) -> str:
    """Normaliza un label: minúsculas, sin acentos, espacios colapsados, sin ':' final."""
    s = strip_accents(s or "").lower().strip().rstrip(":").strip()
    return re.sub(r"\s+", " ", s)


def join_list(value: Any) -> str:
    """
    Convierte un valor (str | list | dict) en un string "a, b, c".
    Sirve para descriptores, técnicas, etc. que pueden llegar como lista.
    """
    if value is None:
        return ""
    if isinstance(value, str):
        parts = [p.strip() for p in re.split(r"[;,\n]+", value) if p.strip()]
        return ", ".join(parts)
    if isinstance(value, (list, tuple)):
        out = []
        for item in value:
            if isinstance(item, dict):
                # busca la clave textual más probable
                for k in ("nombre", "titulo", "valor", "name", "label", "text"):
                    if k in item and item[k]:
                        out.append(str(item[k]).strip())
                        break
            else:
                out.append(str(item).strip())
        return ", ".join(p for p in out if p)
    if isinstance(value, dict):
        for k in ("nombre", "titulo", "valor", "name", "label", "text"):
            if k in value and value[k]:
                return str(value[k]).strip()
    return str(value).strip()


def parse_date(raw: str) -> str:
    """
    Limpia el string de fecha respetando el formato original.
    Devuelve siempre string (no date), porque ARCA mezcla años, rangos y siglos:
      "1826"         → "1826"
      "1700 - 1799"  → "1700–1799"     (normaliza a en-dash)
      "ca. 1783"     → "1783"          (solo el año)
      "Siglo XVIII"  → "Siglo XVIII"   (se deja tal cual)
      ""             → ""
    """
    if not raw:
        return ""
    s = str(raw).strip()

    # Rango "1700 - 1799" / "1700–1799" / "1700—1799"
    m = re.search(
        r"\b(1[0-9]{3}|2[0-9]{3})\s*[-–—]\s*(1[0-9]{3}|2[0-9]{3})\b", s
    )
    if m:
        return f"{m.group(1)}–{m.group(2)}"

    # Año único (posiblemente con "ca.", "circa", "c.", etc. delante)
    m = re.search(r"\b(1[0-9]{3}|2[0-9]{3})\b", s)
    if m:
        return m.group(1)

    # Siglo u otro formato — se respeta literal
    return s


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
            f"No se encontró {CREDENTIALS_FILE}. Ver README para cómo crearlo."
        )
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
    return gspread.authorize(creds)


def open_worksheet(gc: gspread.Client):
    sh = gc.open_by_key(SHEET_ID)
    if WORKSHEET_NAME:
        return sh.worksheet(WORKSHEET_NAME)
    return sh.get_worksheet(0)


def read_links_and_layout(ws) -> tuple[list[dict], list[str]]:
    """
    Lee el sheet y devuelve:
      - jobs: lista de dicts con las filas que tienen Link y (o están vacías,
              o OVERWRITE_EXISTING=True). Cada dict tiene: row_number, url,
              existing (dict header→valor actual)
      - headers: lista de los headers tal como están en la fila 1
    """
    rows = ws.get_all_values()
    if not rows:
        raise RuntimeError("El sheet está vacío — falta la fila de headers.")

    headers = rows[0]
    if LINK_COLUMN not in headers:
        raise RuntimeError(
            f"No encontré la columna '{LINK_COLUMN}' en los headers. "
            f"Headers actuales: {headers}"
        )
    link_idx = headers.index(LINK_COLUMN)

    jobs: list[dict] = []
    for i, row in enumerate(rows[1:], start=2):  # fila 2 es la primera de datos
        url = row[link_idx].strip() if link_idx < len(row) else ""
        if not url.lower().startswith("http"):
            continue

        existing = {h: (row[j] if j < len(row) else "")
                    for j, h in enumerate(headers)}
        already_filled = bool(existing.get("Title", "").strip())
        if already_filled and not OVERWRITE_EXISTING:
            log.info("  · fila %d ya tiene Title, se salta (OVERWRITE_EXISTING=False)", i)
            continue

        jobs.append({"row_number": i, "url": url, "existing": existing})

    log.info("Filas a procesar: %d", len(jobs))
    return jobs, headers


def write_back(ws, headers: list[str], job: dict, scraped: dict[str, str]) -> None:
    """
    Escribe solo las FILLED_COLUMNS en la fila correspondiente, respetando
    el resto de columnas (Dimensions, Afro_Presence, etc.) que quedan intactas.
    Usa batch_update con una sola llamada por fila.
    """
    updates = []
    for col_name in FILLED_COLUMNS:
        if col_name not in headers:
            continue
        value = scraped.get(col_name, "")
        if value == "":
            continue
        col_idx = headers.index(col_name) + 1  # gspread usa índices 1-based
        a1 = gspread.utils.rowcol_to_a1(job["row_number"], col_idx)
        updates.append({"range": a1, "values": [[value]]})

    if updates:
        ws.batch_update(updates, value_input_option="USER_ENTERED")


# ────────────────────────────────────────────────────────────────────────────
# Extracción desde la página
# ────────────────────────────────────────────────────────────────────────────

def _looks_like_obra(obj: Any) -> bool:
    """¿Este dict se parece a los datos de una obra?"""
    if not isinstance(obj, dict):
        return False
    keys = {norm_label(k) for k in obj.keys()}
    signal = {"titulo", "tecnicas", "descriptores", "personajes",
              "relato_visual", "ubicacion", "fecha", "autor", "autores",
              "categorias"}
    return len(keys & signal) >= 2


def _find_obra_in_json(blob: Any) -> dict | None:
    """Busca recursivamente el dict-obra más completo (con más campos)."""
    best: dict | None = None

    def walk(node):
        nonlocal best
        if _looks_like_obra(node):
            if best is None or len(node) > len(best):
                best = node
        if isinstance(node, dict):
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)

    walk(blob)
    return best


def _map_to_row(raw: dict[str, Any], url: str) -> dict[str, str]:
    """Traduce los campos (del DOM o del API) a las columnas del sheet."""
    row = {h: "" for h in FILLED_COLUMNS}

    # Image_ID: último segmento numérico del URL (ej. /obras/8548 → "8548")
    m = re.search(r"/(\d+)/?$", url)
    if m:
        row["Image_ID"] = m.group(1)

    for raw_key, raw_val in raw.items():
        k = norm_label(raw_key)
        col = FIELD_MAP.get(k)
        # Match parcial: "descriptores iconográficos" → "descriptores"
        if col is None:
            for known, target in FIELD_MAP.items():
                if known in k:
                    col = target
                    break
        if col and not row[col]:
            row[col] = join_list(raw_val)

    # Location: ARCA a veces devuelve "Iglesia|Ciudad|País" → "Iglesia, Ciudad, País"
    if row["Location"]:
        parts = [p.strip() for p in row["Location"].split("|") if p.strip()]
        if len(parts) > 1:
            row["Location"] = ", ".join(parts)

    # Limpia el string de fecha (año o rango), pero se queda como string
    if row["Date"]:
        row["Date"] = parse_date(row["Date"])

    return row


# JavaScript que corre dentro del navegador y devuelve los campos de la obra
# extrayéndolos directamente del DOM. Basado en la estructura real de ARCA:
#
#   <h1>TÍTULO</h1>
#   <div id="autores">
#       <a class="autor">AUTOR</a> ...
#   </div>
#   <section class="seccion...">
#       <h3>Fecha</h3>
#       <p class="contenido singular">1826</p>
#   </section>
#   <section class="seccion...">
#       <h3>Técnicas</h3>
#       <ul class="lista contenido">
#           <li><a>Óleo sobre tela</a></li>
#       </ul>
#   </section>
#
_DOM_EXTRACT_JS = r"""
() => {
    const out = {};

    // Título
    const h1 = document.querySelector('h1');
    if (h1) out['titulo'] = (h1.textContent || '').trim();

    // Autor(es): contenedor #autores con enlaces <a class="autor">
    const autoresEls = document.querySelectorAll(
        '#autores a, #autores .autor, [id*="autor"] a'
    );
    const autores = Array.from(autoresEls)
        .map(a => (a.textContent || '').trim())
        .filter(Boolean);
    if (autores.length) out['autor'] = autores;

    // Todas las <section> con un <h3> hijo directo y contenido
    document.querySelectorAll('section').forEach(sec => {
        // buscar h3 como primer nivel dentro de la sección
        const h3 = sec.querySelector(':scope > h3, :scope > div > h3');
        if (!h3) return;
        const label = (h3.textContent || '').trim();
        if (!label || label === '--') return;

        // Caso 1: <p class="contenido singular">valor</p>  (valor único)
        const p = sec.querySelector(
            ':scope > p.singular, :scope > p.contenido, :scope p.singular'
        );
        if (p && (p.textContent || '').trim()) {
            out[label] = (p.textContent || '').trim();
            return;
        }

        // Caso 2: <ul class="lista contenido"><li>...<li></ul>  (varios valores)
        const lis = sec.querySelectorAll(':scope ul.lista li, :scope ul.contenido li');
        if (lis.length) {
            const items = Array.from(lis)
                .map(li => (li.textContent || '').trim())
                .filter(Boolean);
            if (items.length) {
                out[label] = items;
                return;
            }
        }

        // Fallback: cualquier <p> o <ul><li> dentro
        const fp = sec.querySelector(':scope p');
        if (fp && (fp.textContent || '').trim()) {
            out[label] = (fp.textContent || '').trim();
        }
    });

    return out;
}
"""


async def scrape_one(page: Page, url: str, debug: bool) -> dict[str, str]:
    """
    Abre la obra en el navegador, espera a que el DOM termine de renderizarse
    con los datos asincrónicos y extrae los campos directamente del DOM.
    Como fallback, también captura respuestas JSON del API por si algún
    campo no apareció en el DOM.
    """
    log.info("→ %s", url)

    captured_api: list[dict] = []

    async def on_response(resp):
        if "apiarca.uniandes.edu.co" not in resp.url:
            return
        ct = (resp.headers.get("content-type") or "").lower()
        if "json" not in ct:
            return
        try:
            data = await resp.json()
        except Exception:
            return
        captured_api.append({"url": resp.url, "data": data})

    page.on("response", on_response)
    try:
        await page.goto(url, wait_until="networkidle", timeout=60_000)

        # Esperamos a que aparezcan varias secciones con <h3> (los campos
        # se renderizan después de resolver el API call).
        try:
            await page.wait_for_function(
                "() => document.querySelectorAll('section h3').length >= 3",
                timeout=20_000,
            )
        except Exception:
            log.warning("  (timeout esperando secciones; se continúa)")

        # Un pequeño margen para listas que tardan un pelín más
        await page.wait_for_timeout(1500)

        # Extracción desde el DOM
        raw: dict[str, Any] = await page.evaluate(_DOM_EXTRACT_JS)
    finally:
        page.remove_listener("response", on_response)

    # Si algún campo está vacío, intentar completarlo con los datos del API
    # (merge por claves del API).
    api_obra: dict[str, Any] = {}
    for item in captured_api:
        hit = _find_obra_in_json(item["data"])
        if hit and len(hit) > len(api_obra):
            api_obra = hit
    for k, v in api_obra.items():
        # solo completa si el DOM no trajo algo equivalente
        if k not in raw:
            raw[k] = v

    if debug:
        DEBUG_DIR.mkdir(exist_ok=True)
        obra_id = re.search(r"/(\d+)/?$", url)
        tag = obra_id.group(1) if obra_id else "obra"
        (DEBUG_DIR / f"{tag}.html").write_text(await page.content(), encoding="utf-8")
        (DEBUG_DIR / f"{tag}.raw.json").write_text(
            json.dumps(raw, indent=2, ensure_ascii=False, default=str),
            encoding="utf-8",
        )
        (DEBUG_DIR / f"{tag}.api.json").write_text(
            json.dumps(captured_api, indent=2, ensure_ascii=False, default=str),
            encoding="utf-8",
        )
        log.info("  debug: %d respuestas API, %d campos DOM",
                 len(captured_api), len(raw))

    return _map_to_row(raw, url)


async def scrape_all(
    jobs: list[dict], debug: bool = False,
) -> list[tuple[dict, dict[str, str]]]:
    """Ejecuta el scraping de todos los jobs. Devuelve [(job, scraped), ...]."""
    results: list[tuple[dict, dict[str, str]]] = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (research-scraper arca/1.0)",
            locale="es-CO",
        )
        page = await context.new_page()
        for job in jobs:
            try:
                scraped = await scrape_one(page, job["url"], debug)
            except Exception as e:
                log.error("  ERROR en %s → %s", job["url"], e)
                scraped = {"Title": f"ERROR: {e}",
                           **{h: "" for h in FILLED_COLUMNS if h != "Title"}}
            results.append((job, scraped))
        await browser.close()
    return results


# ────────────────────────────────────────────────────────────────────────────
# Backup local
# ────────────────────────────────────────────────────────────────────────────

def write_csv_backup(
    results: list[tuple[dict, dict[str, str]]], path: str = "backup.csv",
) -> None:
    fieldnames = FILLED_COLUMNS + ["Link"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for job, scraped in results:
            writer.writerow({**{h: scraped.get(h, "") for h in FILLED_COLUMNS},
                             "Link": job["url"]})
    log.info("Backup local en %s", path)


# ────────────────────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────────────────────

async def main(debug: bool = False) -> None:
    gc = get_gspread_client()
    ws = open_worksheet(gc)
    jobs, headers = read_links_and_layout(ws)
    if not jobs:
        log.warning("No hay filas para procesar. Asegúrate de tener URLs en la "
                    "columna '%s' y que la columna Title esté vacía "
                    "(o usa OVERWRITE_EXISTING=True).", LINK_COLUMN)
        return

    results = await scrape_all(jobs, debug=debug)
    write_csv_backup(results)

    # Escribir cada fila en el sheet
    for job, scraped in results:
        write_back(ws, headers, job, scraped)
        log.info("  ↳ fila %d actualizada: %s",
                 job["row_number"], scraped.get("Title") or "(sin título)")

    log.info("✔ Listo — %d filas procesadas", len(results))


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Scraper de ARCA → Google Sheets")
    parser.add_argument(
        "--debug", action="store_true",
        help="Guarda HTML y JSON crudo de cada obra en ./debug/ (útil si los "
             "selectores no calzan con el sitio)",
    )
    parser.add_argument(
        "--overwrite", action="store_true",
        help="Re-escribe filas que ya tengan Title. Por defecto las salta.",
    )
    args = parser.parse_args()
    OVERWRITE_EXISTING = args.overwrite or OVERWRITE_EXISTING
    asyncio.run(main(debug=args.debug))