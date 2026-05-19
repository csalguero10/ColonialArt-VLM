from fastapi import FastAPI
import os
from dotenv import load_dotenv
from src.metadata_pil import get_corpus_inventory   # ← cambia esta línea

load_dotenv()

app = FastAPI(title="ColonialArt-VLM API")

@app.get("/")
def read_root():
    return {"message": "Backend funcionando correctamente"}

@app.get("/test-drive")
def test_drive():
    internal_path = "/app/data/corpus"
    try:
        files = os.listdir(internal_path)
        return {
            "status": "success",
            "message": "¡Conexión con Drive confirmada!",
            "total_files": len(files),
            "sample_files": files[:5]
        }
    except Exception as e:
        return {
            "status": "error",
            "message": f"No se pudo acceder al Drive: {str(e)}",
            "path_attempted": internal_path
        }

@app.get("/inventory")
def get_inventory():
    path = "/app/data/corpus"
    data = get_corpus_inventory(path)          # ← cambia esta línea
    return {
        "total_cataloged": len(data),
        "items": data
    }