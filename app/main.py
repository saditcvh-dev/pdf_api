from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api import pdf_router
from app.core import state

from app.core.config import settings
from app.tasks import process_pdf_task  # para registrar tareas
import uvicorn
import logging
from pathlib import Path
from datetime import datetime
import fitz  # PyMuPDF para contar páginas reales

# Importar los estados globales desde el módulo dedicado
from app.core.state import pdf_storage, pdf_task_status

logger = logging.getLogger(__name__)

app = FastAPI(
    title="PDF Processing API",
    description="API para extraer texto y buscar en PDFs (con cola de procesamiento)",
    version="2.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(pdf_router.router)

# ────────────────────────────────────────────────────────────────
# Cargar PDFs existentes desde disco al iniciar el servidor
# ────────────────────────────────────────────────────────────────
def load_existing_pdfs_at_startup():
    """
    Lee la carpeta outputs/ y reconstruye pdf_storage + pdf_task_status
    para que los PDFs procesados anteriormente aparezcan en /list
    """
    global pdf_storage, pdf_task_status

    outputs_dir = Path(settings.OUTPUTS_FOLDER)
    extracted_dir = Path(settings.EXTRACTED_FOLDER)

    if not outputs_dir.exists():
        logger.info("No existe carpeta outputs → nada que cargar al inicio")
        return

    logger.info(f"Restaurando PDFs desde disco: {outputs_dir}")

    loaded = 0

    for pdf_file in outputs_dir.glob("*.pdf"):
        pdf_id = pdf_file.stem

        # Evitar duplicados
        if pdf_id in pdf_storage:
            continue

        try:
            doc = fitz.open(pdf_file)
            page_count = len(doc)
            doc.close()
        except Exception as e:
            logger.warning(f"No se pudo leer páginas de {pdf_file}: {e}")
            page_count = None

        # Buscar texto extraído
        txt_candidate = extracted_dir / f"{pdf_id}.txt"
        txt_gz_candidate = extracted_dir / f"{pdf_id}.txt.gz"

        text_path = (
            str(txt_candidate) if txt_candidate.exists() else
            str(txt_gz_candidate) if txt_gz_candidate.exists() else
            None
        )

        size_bytes = pdf_file.stat().st_size
        created_ts = pdf_file.stat().st_ctime

        # Guardar en pdf_storage
        pdf_storage[pdf_id] = {
            "filename": f"{pdf_id}.pdf",          # placeholder (puedes mejorar después)
            "pdf_path": str(pdf_file),
            "size": size_bytes,
            "upload_time": created_ts,
            "task_id": None,
            "use_ocr": True
        }

        # Guardar en pdf_task_status
        pdf_task_status[pdf_id] = {
            "task_id": None,
            "status": "completed",
            "created_at": datetime.fromtimestamp(created_ts),
            "completed_at": datetime.fromtimestamp(pdf_file.stat().st_mtime),
            "pages": page_count,
            "extracted_text_path": text_path,
            "used_ocr": True,
            "error": None,
            "ocr_pdf_path": str(pdf_file)
        }

        loaded += 1
        logger.info(f"  Restaurado: {pdf_id}  |  páginas: {page_count}  |  texto: {text_path or 'sin texto'}")

    logger.info(f"Restauración completada: {loaded} PDFs cargados desde disco")


# Ejecutar la carga al iniciar la aplicación
load_existing_pdfs_at_startup()

@app.get("/")
async def root():
    return {
        "message": "PDF Processing API (v2.0 - Con Cola de Procesamiento)",
        "version": "2.0.0",
        "loaded_pdfs_on_startup": len(pdf_storage),
        "endpoints": {
            "upload": "/api/pdf/upload",
            "status": "/api/pdf/upload-status/{pdf_id}",
            "list": "/api/pdf/list",
            "searchable_pdf": "/api/pdf/{pdf_id}/searchable-pdf",
            # ... agrega los demás que uses
        }
    }

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "pdf-api", "version": "2.0.0"}

if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        reload=True
    )
