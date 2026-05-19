"""
metadata_pil.py — análisis técnico de imágenes con PIL.

Tres niveles de API:
    analyze_image_bytes(bytes, size?)  →  función pura, bytes → dict
    analyze_image_file(path)           →  path → dict (lee del disco)
    get_corpus_inventory(folder)       →  escanea carpeta → list[dict]

Usado por:
    - data/download_corpus.py     (al descargar, ya tiene los bytes)
    - main.py endpoint /inventory (escaneando la carpeta del corpus)
"""

from io import BytesIO
from pathlib import Path
from typing import Any

from PIL import Image, ImageStat

VALID_EXTENSIONS = (".jpg", ".jpeg", ".png", ".tiff", ".tif", ".bmp", ".webp")


def analyze_image_bytes(
    img_bytes: bytes,
    file_size_bytes: int | None = None,
) -> dict[str, Any]:
    """Bytes de imagen → dict con metadata técnica. Función pura."""
    img = Image.open(BytesIO(img_bytes))
    img.load()

    img_gray = img.convert("L")
    stat = ImageStat.Stat(img_gray)
    brightness = round(stat.mean[0], 2)
    contrast = round(stat.stddev[0], 2)

    img_small = img.resize((1, 1), resample=Image.Resampling.BILINEAR)
    main_color = img_small.getpixel((0, 0))

    size_bytes = file_size_bytes if file_size_bytes is not None else len(img_bytes)

    return {
        "format":         img.format,
        "width":          img.width,
        "height":         img.height,
        "aspect_ratio":   round(img.width / img.height, 2),
        "size_kb":        round(size_bytes / 1024, 2),
        "mode":           img.mode,
        "brightness":     brightness,
        "contrast":       contrast,
        "main_color_rgb": f"RGB{main_color}",
    }


def analyze_image_file(file_path: str | Path) -> dict[str, Any]:
    """Lee un archivo del disco y devuelve metadata + filename."""
    path = Path(file_path)
    img_bytes = path.read_bytes()
    return {
        "filename": path.name,
        **analyze_image_bytes(img_bytes, file_size_bytes=path.stat().st_size),
    }


def get_corpus_inventory(folder_path: str | Path) -> list[dict[str, Any]]:
    """Escanea una carpeta y devuelve metadata de todas las imágenes válidas."""
    folder = Path(folder_path)
    if not folder.exists():
        return []

    inventory: list[dict[str, Any]] = []
    for entry in sorted(folder.iterdir()):
        if entry.is_file() and entry.suffix.lower() in VALID_EXTENSIONS:
            try:
                inventory.append(analyze_image_file(entry))
            except Exception as e:
                inventory.append({"filename": entry.name, "error": str(e)})
    return inventory