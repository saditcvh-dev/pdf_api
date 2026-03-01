from fastapi import APIRouter, UploadFile, File, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse
import os
import time
from datetime import datetime
from pathlib import Path
import re
import logging
import threading
import subprocess
import sys
import gzip
from typing import Optional, Dict, Any

from app.services.pdf_service import PDFService
from app.models.schemas import (
    PDFUploadResponse,
    PDFUploadStatus,
    SearchRequest,
    SearchResponse,
    PDFListResponse,
)
from app.tasks.pdf_tasks import process_pdf_task
from app.core.celery_app import celery_app
from app.core.state import pdf_storage, pdf_task_status
from app.core.config import settings

# =========================
# Setup
# =========================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/pdf", tags=["pdf"])
pdf_service = PDFService()

# =========================
# Nomenclatura / patrones
# =========================

# Regla de filtro "humana" para /list y /global-search
nomenclature_re = re.compile(r"\b\d+[ _-]+\d+(?:-[\d]+)*[ _-]+[CP](?=[\s_-]|$)", re.IGNORECASE)

def _name_matches_nomenclature(pdf_id: str, data: dict = None) -> bool:
    filename = None
    if isinstance(data, dict):
        filename = data.get("filename")
    if not filename:
        filename = pdf_id

    try:
        name_to_check = Path(filename).stem
    except Exception:
        name_to_check = str(filename)

    return bool(nomenclature_re.search(name_to_check) or nomenclature_re.search(str(pdf_id)))

# Patrón estricto de IDs del flujo normal (sin .pdf)
VALID_PATTERN = re.compile(r"^\d+_\d{1,2}-\d{1,2}-\d{1,3}-\d{1,3}_[CP]_[a-f0-9]+$")

# Base id para versionados (si existe DOCS_ROOT)
BASE_ID_PATTERN = re.compile(r"^\d+_\d{1,2}_\d{1,2}_\d{1,3}_\d{1,3}_[CP]$", re.IGNORECASE)
VERSIONED_FILE_PATTERN = re.compile(r"^(?P<base>.+)_v(?P<v>\d+)_(?P<ts>\d+)\.pdf$", re.IGNORECASE)

def _is_base_id(name: str) -> bool:
    return bool(BASE_ID_PATTERN.match(name or ""))

def _pick_latest_version_pdf(folder: Path, base_id: str) -> Optional[Path]:
    """
    Elige el PDF más nuevo por:
      1) versión vN más alta
      2) ts más alto
      3) mtime más alto (fallback)
    """
    best = None
    best_key = None

    try:
        for f in folder.iterdir():
            if not f.is_file() or f.suffix.lower() != ".pdf":
                continue
            m = VERSIONED_FILE_PATTERN.match(f.name)
            if not m:
                continue
            if m.group("base") != base_id:
                continue

            v = int(m.group("v"))
            ts = int(m.group("ts"))
            mtime = int(f.stat().st_mtime)
            key = (v, ts, mtime)

            if best_key is None or key > best_key:
                best_key = key
                best = f
    except Exception:
        return None

    return best

# =========================
# Helpers de archivos / estado
# =========================

def _safe_join_under_root(root: Path, rel: str) -> Path:
    root = root.resolve()
    candidate = (root / rel).resolve()
    if not str(candidate).startswith(str(root)):
        raise HTTPException(status_code=400, detail="Ruta inválida")
    return candidate

def _load_text_from_file(path: str) -> str:
    try:
        if not path:
            return ""
        if path.endswith(".gz"):
            with gzip.open(path, "rt", encoding="utf-8", errors="ignore") as f:
                return f.read()
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            return f.read()
    except Exception:
        return ""

def load_existing_pdfs() -> None:
    """
    Carga PDFs existentes del flujo normal desde UPLOAD_FOLDER/OUTPUTS_FOLDER.
    - Rellena pdf_storage y pdf_task_status.
    - created_at/completed_at se guardan como datetime (porque tu schema así lo pide).
    """
    uploads_dir = Path(settings.UPLOAD_FOLDER)
    extracted_dir = Path(settings.EXTRACTED_FOLDER)
    outputs_dir = Path(settings.OUTPUTS_FOLDER)

    search_dirs = []
    if uploads_dir.exists():
        search_dirs.append(uploads_dir)
    if outputs_dir.exists():
        search_dirs.append(outputs_dir)

    if not search_dirs:
        return

    for base in search_dirs:
        for pdf_file in base.rglob("*.pdf"):
            try:
                pdf_id = pdf_file.stem

                if not VALID_PATTERN.match(pdf_id):
                    continue
                if not pdf_id or pdf_id.startswith("."):
                    continue
                if pdf_id in pdf_storage:
                    continue

                mtime = float(pdf_file.stat().st_mtime)
                created_at_dt = datetime.fromtimestamp(mtime)

                safe_name_part = pdf_id[:-17] if len(pdf_id) > 17 else pdf_id
                readable_name = safe_name_part.replace("_", " ")

                pdf_storage[pdf_id] = {
                    "filename": f"{readable_name}.pdf",
                    "size": pdf_file.stat().st_size,
                    "upload_time": mtime,  # float (tu schema lo pide)
                    "pdf_path": str(pdf_file),
                    "mode": "local",
                    "task_id": None,
                }

                txt_path = extracted_dir / f"{pdf_id}.txt"
                txt_gz_path = extracted_dir / f"{pdf_id}.txt.gz"
                out_pdf = outputs_dir / f"{pdf_id}.pdf"

                actual_txt = txt_path if txt_path.exists() else (txt_gz_path if txt_gz_path.exists() else None)

                if actual_txt or out_pdf.exists():
                    completed_src = actual_txt if actual_txt else out_pdf
                    completed_at_dt = datetime.fromtimestamp(float(completed_src.stat().st_mtime))

                    pdf_task_status[pdf_id] = {
                        "status": "completed",
                        "created_at": created_at_dt,
                        "completed_at": completed_at_dt,
                        "used_ocr": bool(out_pdf.exists()),
                        "extracted_text_path": str(actual_txt) if actual_txt else None,
                        "ocr_pdf_path": str(out_pdf) if out_pdf.exists() else None,
                        "task_id": None,
                        "mode": "local",
                        "pages": None,
                        "error": None,
                    }
                else:
                    pdf_task_status[pdf_id] = {
                        "status": "unknown",
                        "created_at": created_at_dt,
                        "completed_at": None,
                        "task_id": None,
                        "mode": "local",
                        "pages": None,
                        "used_ocr": False,
                        "extracted_text_path": None,
                        "error": None,
                    }
            except Exception as e:
                logger.exception(f"Error cargando PDF {pdf_file}: {e}")

def _infer_status_for_base(base_id: str, extracted_dir: Path, outputs_dir: Path) -> dict:
    txt_path = extracted_dir / f"{base_id}.txt"
    txt_gz_path = extracted_dir / f"{base_id}.txt.gz"
    out_pdf = outputs_dir / f"{base_id}.pdf"

    actual_txt = None
    if txt_path.exists():
        actual_txt = txt_path
    elif txt_gz_path.exists():
        actual_txt = txt_gz_path

    if actual_txt or out_pdf.exists():
        completed_src = actual_txt if actual_txt else out_pdf
        return {
            "status": "completed",
            "created_at": None,
            "completed_at": datetime.fromtimestamp(float(completed_src.stat().st_mtime)),
            "used_ocr": bool(out_pdf.exists()),
            "extracted_text_path": str(actual_txt) if actual_txt else None,
            "ocr_pdf_path": str(out_pdf) if out_pdf.exists() else None,
            "task_id": None,
            "mode": "local",
            "pages": None,
            "error": None,
        }

    return {
        "status": "unknown",
        "created_at": None,
        "completed_at": None,
        "used_ocr": False,
        "extracted_text_path": None,
        "task_id": None,
        "mode": "local",
        "pages": None,
        "error": None,
    }

def _scan_docs_root_latest_versions(docs_root: Path) -> dict:
    """
    { base_id: {"pdf_path": str, "mtime": float, "size": int, "filename": str} }
    """
    latest = {}
    if not docs_root or not docs_root.exists():
        return latest

    for folder in docs_root.rglob("*"):
        if not folder.is_dir():
            continue
        base_id = folder.name
        if not _is_base_id(base_id):
            continue

        best_pdf = _pick_latest_version_pdf(folder, base_id)
        if not best_pdf:
            continue

        st = best_pdf.stat()
        latest[base_id] = {
            "pdf_path": str(best_pdf),
            "mtime": float(st.st_mtime),
            "size": int(st.st_size),
            "filename": best_pdf.name,
        }

    return latest

def _get_docs_root_safe() -> Optional[Path]:
    """
    Tu Settings pegado no trae DOCS_ROOT, pero tu router lo usa.
    Si sí lo tienes en tu proyecto real, esto lo toma.
    Si no existe, regresamos None y desactivamos versionado.
    """
    docs_root_val = getattr(settings, "DOCS_ROOT", None)
    if not docs_root_val:
        return None
    try:
        return Path(docs_root_val)
    except Exception:
        return None

def _ensure_docs_root_in_storage() -> None:
    docs_root = _get_docs_root_safe()
    if not docs_root:
        return
    latest_map = _scan_docs_root_latest_versions(docs_root)
    for base_id, info in latest_map.items():
        if base_id not in pdf_storage:
            pdf_storage[base_id] = {
                "filename": f"{base_id}.pdf",
                "pdf_path": info["pdf_path"],
                "size": info["size"],
                "upload_time": info["mtime"],
                "mode": "local",
                "task_id": None,
            }

# =========================
# Endpoints
# =========================

@router.post("/upload", response_model=PDFUploadResponse)
async def upload_pdf(file: UploadFile = File(...), use_ocr: bool = Query(True)):
    """Sube un PDF. Si hay workers, encola en Celery; si no, procesa en hilo local."""
    try:
        if not file.filename or not file.filename.lower().endswith(".pdf"):
            raise HTTPException(status_code=400, detail="Solo se permiten archivos PDF")

        file_bytes = await file.read()
        if not file_bytes:
            raise HTTPException(status_code=400, detail="Archivo vacío")

        pdf_id = pdf_service.generate_pdf_id(file.filename, file_bytes)
        pages_count = pdf_service.get_pdf_pages_count(file_bytes)

        os.makedirs(settings.UPLOAD_FOLDER, exist_ok=True)
        pdf_path = os.path.join(settings.UPLOAD_FOLDER, f"{pdf_id}.pdf")
        with open(pdf_path, "wb") as f:
            f.write(file_bytes)

        now = datetime.now()

        # Detectar workers Celery
        has_workers = False
        try:
            inspector = celery_app.control.inspect()
            ping = inspector.ping() if inspector else None
            has_workers = bool(ping)
        except Exception:
            has_workers = False

        # Metadatos
        pdf_storage[pdf_id] = {
            "filename": file.filename,
            "pdf_path": pdf_path,
            "size": len(file_bytes),
            "upload_time": time.time(),  # float
            "pages": pages_count,
            "use_ocr": use_ocr,
        }

        # Celery
        if has_workers:
            task = process_pdf_task.delay(pdf_id=pdf_id, pdf_path=pdf_path, use_ocr=use_ocr)

            pdf_storage[pdf_id].update({"task_id": task.id, "mode": "celery"})
            pdf_task_status[pdf_id] = {
                "task_id": task.id,
                "status": "pending",
                "mode": "celery",
                "created_at": now,
                "completed_at": None,
                "pages": pages_count,
                "extracted_text_path": None,
                "ocr_pdf_path": None,
                "used_ocr": use_ocr,
                "error": None,
                "progress": 0,
            }

            return PDFUploadResponse(
                id=pdf_id,
                filename=file.filename,
                size=len(file_bytes),
                task_id=task.id,
                status="pending",
                message="PDF encolado para procesamiento (Celery). Usa /upload-status/{pdf_id}.",
                pages=pages_count,
                estimated_wait_time=10.0,
            )

        # Local
        pdf_storage[pdf_id].update({"task_id": None, "mode": "local"})
        pdf_task_status[pdf_id] = {
            "task_id": None,
            "status": "pending",
            "mode": "local",
            "created_at": now,
            "completed_at": None,
            "pages": pages_count,
            "extracted_text_path": None,
            "ocr_pdf_path": None,
            "used_ocr": use_ocr,
            "error": None,
            "progress": 0,
        }

        def _local_process(pid: str, ppath: str, puse_ocr: bool):
            try:
                pdf_task_status[pid].update({"status": "processing", "progress": 50})
                text, pages, used_ocr_ = pdf_service.extract_text_from_pdf(ppath, use_ocr=puse_ocr)
                text_path = pdf_service.save_extracted_text(text, pid)

                pdf_task_status[pid].update({
                    "status": "completed",
                    "pages": pages,
                    "extracted_text_path": text_path,
                    "used_ocr": used_ocr_,
                    "completed_at": datetime.now(),
                    "error": None,
                    "progress": 100,
                })

                # NO guardar texto completo
                pdf_storage[pid].update({
                    "pages": pages,
                    "text_path": text_path,
                })
            except Exception as e:
                logger.exception(f"Error en procesamiento local {pid}: {e}")
                pdf_task_status[pid].update({
                    "status": "failed",
                    "error": str(e),
                    "completed_at": datetime.now(),
                    "progress": 0,
                })

        threading.Thread(target=_local_process, args=(pdf_id, pdf_path, use_ocr), daemon=True).start()

        return PDFUploadResponse(
            id=pdf_id,
            filename=file.filename,
            size=len(file_bytes),
            task_id="",
            status="pending",
            message="PDF aceptado y procesando localmente (no hay workers). Usa /upload-status/{pdf_id}.",
            pages=pages_count,
            estimated_wait_time=0.0,
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al subir PDF: {str(e)}")

@router.get("/upload-status/{pdf_id}", response_model=PDFUploadStatus)
async def get_upload_status(pdf_id: str):
    if pdf_id not in pdf_task_status:
        raise HTTPException(status_code=404, detail="PDF no encontrado")

    info = pdf_task_status[pdf_id]
    task_id = info.get("task_id")
    mode = info.get("mode") or ("celery" if task_id else "local")

    # Local (no consultar Celery)
    if mode == "local" or not task_id:
        status = info.get("status", "unknown")
        progress = int(info.get("progress") or (10 if status == "pending" else 50 if status == "processing" else 100 if status == "completed" else 0))

        return PDFUploadStatus(
            pdf_id=pdf_id,
            task_id="",
            status=status,
            progress=progress,
            pages=info.get("pages"),
            extracted_text_path=info.get("extracted_text_path"),
            used_ocr=info.get("used_ocr"),
            error=info.get("error"),
            created_at=info.get("created_at"),
            completed_at=info.get("completed_at"),
        )

    # Celery
    task = celery_app.AsyncResult(task_id)
    status_map = {
        "PENDING": "pending",
        "STARTED": "processing",
        "RETRY": "processing",
        "SUCCESS": "completed",
        "FAILURE": "failed",
    }
    current_status = status_map.get(task.state, (task.state or "unknown").lower())

    if task.state == "SUCCESS" and isinstance(task.result, dict):
        result = task.result
        pdf_task_status[pdf_id].update({
            "status": "completed",
            "pages": result.get("pages"),
            "extracted_text_path": result.get("text_path"),
            "ocr_pdf_path": result.get("pdf_path"),
            "used_ocr": result.get("used_ocr"),
            "text_length": result.get("text_length"),
            "completed_at": datetime.now(),
            "error": None,
            "mode": "celery",
            "progress": 100,
        })
        pdf_storage.get(pdf_id, {}).update({
            "pages": result.get("pages"),
            "text_path": result.get("text_path"),
            "completed": True,
        })

    elif task.state == "FAILURE":
        err = str(task.info) if task.info else "Error desconocido"
        pdf_task_status[pdf_id].update({
            "status": "failed",
            "error": err,
            "completed_at": datetime.now(),
            "mode": "celery",
            "progress": 0,
        })

    final = pdf_task_status[pdf_id]
    progress = 10 if current_status == "pending" else 50 if current_status == "processing" else 100 if current_status == "completed" else 0

    return PDFUploadStatus(
        pdf_id=pdf_id,
        task_id=task_id,
        status=current_status,
        progress=progress,
        pages=final.get("pages"),
        extracted_text_path=final.get("extracted_text_path"),
        used_ocr=final.get("used_ocr"),
        error=final.get("error"),
        created_at=final.get("created_at"),
        completed_at=final.get("completed_at"),
    )

@router.post("/{pdf_id}/search", response_model=SearchResponse)
async def search_pdf(pdf_id: str, search_request: SearchRequest):
    if pdf_id not in pdf_storage and pdf_id not in pdf_task_status:
        raise HTTPException(status_code=404, detail="PDF no encontrado")

    start_time = time.time()

    meta = pdf_storage.get(pdf_id, {})
    ts = pdf_task_status.get(pdf_id, {})

    extracted_dir = Path(settings.EXTRACTED_FOLDER)

    # Prioridad: status -> storage -> inferred
    text_path = ts.get("extracted_text_path") or meta.get("text_path")
    if not text_path or not os.path.exists(str(text_path)):
        candidate_txt = extracted_dir / f"{pdf_id}.txt"
        candidate_gz = extracted_dir / f"{pdf_id}.txt.gz"
        if candidate_txt.exists():
            text_path = str(candidate_txt)
        elif candidate_gz.exists():
            text_path = str(candidate_gz)

    text = _load_text_from_file(str(text_path)) if text_path else ""
    if not text:
        raise HTTPException(status_code=404, detail="Texto no encontrado para este PDF")

    # Tu SearchRequest trae use_regex pero tu PDFService decide si lo soporta o no.
    results = pdf_service.search_in_text(
        text,
        search_request.term,
        search_request.case_sensitive
    )

    limited_results = results[:100]
    execution_time = time.time() - start_time

    return SearchResponse(
        term=search_request.term,
        total_matches=len(results),
        results=limited_results,
        pdf_id=pdf_id,
        execution_time=execution_time
    )

@router.get("/{pdf_id}/text")
async def get_pdf_text(pdf_id: str):
    if pdf_id not in pdf_storage and pdf_id not in pdf_task_status:
        raise HTTPException(status_code=404, detail="PDF no encontrado")

    pdf_data = pdf_storage.get(pdf_id, {})
    task_status = pdf_task_status.get(pdf_id, {})

    task_id = task_status.get("task_id") or pdf_data.get("task_id")
    mode = task_status.get("mode") or pdf_data.get("mode") or ("celery" if task_id else "local")

    # refrescar Celery si aplica
    if mode == "celery" and task_id:
        try:
            task = celery_app.AsyncResult(task_id)
            if task.state == "SUCCESS" and isinstance(task.result, dict):
                result = task.result
                pdf_task_status[pdf_id] = pdf_task_status.get(pdf_id, {})
                pdf_task_status[pdf_id].update({
                    "status": "completed",
                    "pages": result.get("pages"),
                    "extracted_text_path": result.get("text_path"),
                    "ocr_pdf_path": result.get("pdf_path"),
                    "used_ocr": result.get("used_ocr"),
                    "text_length": result.get("text_length"),
                    "completed_at": datetime.now(),
                    "error": None,
                    "task_id": task_id,
                    "mode": "celery",
                    "progress": 100,
                })
                if pdf_id in pdf_storage:
                    pdf_storage[pdf_id].update({
                        "pages": result.get("pages"),
                        "text_path": result.get("text_path"),
                        "completed": True,
                    })
            elif task.state == "FAILURE":
                pdf_task_status[pdf_id] = pdf_task_status.get(pdf_id, {})
                pdf_task_status[pdf_id].update({
                    "status": "failed",
                    "error": str(task.info) if task.info else "Error desconocido",
                    "completed_at": datetime.now(),
                    "task_id": task_id,
                    "mode": "celery",
                    "progress": 0,
                })
        except Exception:
            logger.exception(f"No se pudo consultar estado Celery para task {task_id}")

    current_status = pdf_task_status.get(pdf_id, {}).get("status") or pdf_data.get("status")

    if current_status in ("pending", "processing"):
        return JSONResponse(
            status_code=202,
            content={
                "status": current_status,
                "message": "PDF aún está siendo procesado",
                "task_id": task_status.get("task_id") or pdf_data.get("task_id") or "",
                "progress": int(task_status.get("progress") or 0),
            }
        )

    if current_status == "failed":
        return JSONResponse(
            status_code=400,
            content={
                "status": "failed",
                "message": "Error al procesar PDF",
                "error": task_status.get("error") or pdf_data.get("error"),
            }
        )

    extracted_dir = Path(settings.EXTRACTED_FOLDER)
    text_path = task_status.get("extracted_text_path") or pdf_data.get("text_path")

    if not text_path or not os.path.exists(str(text_path)):
        candidate_txt = extracted_dir / f"{pdf_id}.txt"
        candidate_gz = extracted_dir / f"{pdf_id}.txt.gz"
        if candidate_txt.exists():
            text_path = str(candidate_txt)
        elif candidate_gz.exists():
            text_path = str(candidate_gz)

    if text_path and os.path.exists(str(text_path)):
        return FileResponse(str(text_path), media_type="text/plain", filename=f"{pdf_id}_texto.txt")

    raise HTTPException(status_code=404, detail="Texto no encontrado")

@router.get("/{pdf_id}/searchable-pdf")
async def get_searchable_pdf(pdf_id: str):
    outputs_dir = Path(settings.OUTPUTS_FOLDER)

    docs_root = _get_docs_root_safe()

    # Caso A: base_id (versionado)
    if _is_base_id(pdf_id):
        if not docs_root:
            raise HTTPException(status_code=400, detail="DOCS_ROOT no está configurado en settings")
        latest_map = _scan_docs_root_latest_versions(docs_root)
        info = latest_map.get(pdf_id)
        if not info:
            raise HTTPException(status_code=404, detail="No se encontró ninguna versión para ese base_id")

        pdf_path = info["pdf_path"]
        if not os.path.exists(pdf_path):
            raise HTTPException(status_code=404, detail="Archivo no encontrado en DOCS_ROOT")

        return FileResponse(pdf_path, media_type="application/pdf", filename=f"{pdf_id}.pdf")

    # Caso B: flujo normal (outputs por id)
    task_status = pdf_task_status.get(pdf_id, {})
    if not task_status and pdf_id in pdf_storage:
        pdf_task_status[pdf_id] = {
            "task_id": pdf_storage[pdf_id].get("task_id"),
            "status": pdf_storage[pdf_id].get("status", "unknown"),
            "ocr_pdf_path": str(outputs_dir / f"{pdf_id}.pdf"),
            "created_at": datetime.fromtimestamp(float(pdf_storage[pdf_id].get("upload_time", time.time()))),
            "mode": pdf_storage[pdf_id].get("mode") or ("celery" if pdf_storage[pdf_id].get("task_id") else "local"),
            "used_ocr": False,
            "error": None,
        }
        task_status = pdf_task_status[pdf_id]

    if not task_status:
        raise HTTPException(status_code=404, detail="ID de PDF no reconocido")

    current_status = task_status.get("status", "unknown")
    if current_status in ("pending", "processing"):
        return JSONResponse(
            status_code=202,
            content={
                "status": current_status,
                "message": "El PDF aún se está procesando",
                "task_id": task_status.get("task_id") or "",
                "progress": int(task_status.get("progress") or 0),
            }
        )

    if current_status == "failed":
        return JSONResponse(
            status_code=400,
            content={
                "status": "failed",
                "message": "El proceso de OCR falló",
                "error": task_status.get("error", "Sin detalles"),
            }
        )

    ocr_pdf_path = task_status.get("ocr_pdf_path") or str(outputs_dir / f"{pdf_id}.pdf")
    if not os.path.exists(str(ocr_pdf_path)):
        raise HTTPException(status_code=404, detail="El archivo OCR no existe en outputs")

    return FileResponse(str(ocr_pdf_path), media_type="application/pdf", filename=f"{pdf_id}_searchable.pdf")

@router.post("/merge-with-output")
async def merge_with_output(
    first_pdf_id: str = Query(..., description="Ruta relativa dentro de DOCS_ROOT (ej: 01/01/.../archivo.pdf)"),
    file: UploadFile = File(...),
    use_ocr: bool = Query(True),
    position: str = Query("end", description="Dónde insertar el segundo PDF: 'start' o 'end'")
):
    """
    Une un PDF existente dentro de DOCS_ROOT (ruta relativa) + un PDF subido.
    Procesa el segundo con OCRmyPDF, extrae texto, une y deja resultado en OUTPUTS_FOLDER.
    """
    try:
        docs_root = _get_docs_root_safe()
        if not docs_root:
            raise HTTPException(status_code=400, detail="DOCS_ROOT no está configurado en settings")

        first_path = _safe_join_under_root(docs_root, first_pdf_id)
        if not first_path.exists() or not first_path.is_file() or first_path.suffix.lower() != ".pdf":
            raise HTTPException(status_code=404, detail=f"PDF no encontrado: {first_pdf_id}")

        file_bytes = await file.read()
        if not file_bytes:
            raise HTTPException(status_code=400, detail="Archivo vacío")

        second_id = pdf_service.generate_pdf_id(file.filename, file_bytes)

        uploads_dir = Path(settings.UPLOAD_FOLDER)
        uploads_dir.mkdir(parents=True, exist_ok=True)
        second_path = uploads_dir / f"{second_id}.pdf"
        second_path.write_bytes(file_bytes)

        outputs_dir = Path(settings.OUTPUTS_FOLDER)
        outputs_dir.mkdir(parents=True, exist_ok=True)

        ocr_second_output = outputs_dir / f"{second_id}.pdf"

        command = [
            sys.executable, "-m", "ocrmypdf",
            "-l", "spa",
            "--force-ocr",
            "--optimize", "0",
            "--output-type", "pdf",
            "--jpeg-quality", "100",
            str(second_path),
            str(ocr_second_output),
        ]

        result = subprocess.run(command, capture_output=True, text=True, check=False)
        if result.returncode != 0 or not ocr_second_output.exists():
            try:
                if second_path.exists():
                    second_path.unlink()
            except Exception:
                pass
            raise HTTPException(status_code=500, detail=f"OCRmyPDF falló: {result.stderr.strip()}")

        text, pages, used_ocr_ = pdf_service.extract_text_from_pdf(str(second_path), use_ocr=use_ocr)
        text_path = pdf_service.save_extracted_text(text, second_id)

        now = datetime.now()
        pdf_storage[second_id] = {
            "filename": file.filename,
            "pdf_path": str(second_path),
            "size": len(file_bytes),
            "upload_time": time.time(),
            "task_id": None,
            "use_ocr": use_ocr,
            "pages": pages,
            "text_path": text_path,
            "mode": "local",
        }
        pdf_task_status[second_id] = {
            "task_id": None,
            "status": "completed",
            "created_at": now,
            "completed_at": datetime.now(),
            "pages": pages,
            "extracted_text_path": text_path,
            "ocr_pdf_path": str(ocr_second_output),
            "used_ocr": used_ocr_,
            "text_length": len(text),
            "mode": "local",
            "error": None,
            "progress": 100,
        }

        merged_id = f"{first_pdf_id.replace('/', '_')}_{second_id}_merged"
        merged_output = outputs_dir / f"{merged_id}.pdf"

        merged_path = pdf_service.merge_pdfs(
            [str(first_path), str(ocr_second_output)],
            str(merged_output),
            position=position,
        )

        pdf_storage[merged_id] = {
            "filename": f"{merged_id}.pdf",
            "pdf_path": merged_path,
            "size": os.path.getsize(merged_path),
            "upload_time": time.time(),
            "mode": "local",
            "task_id": None,
        }
        pdf_task_status[merged_id] = {
            "task_id": None,
            "status": "completed",
            "created_at": now,
            "completed_at": datetime.now(),
            "pages": pages,
            "extracted_text_path": None,
            "ocr_pdf_path": merged_path,
            "used_ocr": True,
            "mode": "local",
            "error": None,
            "progress": 100,
        }

        return FileResponse(merged_path, media_type="application/pdf", filename=f"{merged_id}.pdf")

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en merge: {str(e)}")

@router.get("/list", response_model=PDFListResponse)
async def list_pdfs():
    """
    Devuelve PDFs filtrados por nomenclatura.
    - created_at/completed_at son datetime (como tu schema).
    - upload_time es float (como tu schema).
    - Si existe DOCS_ROOT, agrega base_id con versión latest (sin duplicar versiones).
    """
    # cargar flujo normal desde disco (si no se hace en startup)
    try:
        load_existing_pdfs()
    except Exception:
        logger.exception("No se pudo cargar PDFs existentes desde disco")

    extracted_dir = Path(settings.EXTRACTED_FOLDER)
    outputs_dir = Path(settings.OUTPUTS_FOLDER)

    pdfs_list: list[Dict[str, Any]] = []

    # 1) Versionados (si existe DOCS_ROOT)
    docs_root = _get_docs_root_safe()
    latest_map = _scan_docs_root_latest_versions(docs_root) if docs_root else {}

    for base_id, info in latest_map.items():
        if not _name_matches_nomenclature(base_id, {"filename": base_id}):
            continue

        ts = pdf_task_status.get(base_id)
        if not ts:
            ts = _infer_status_for_base(base_id, extracted_dir, outputs_dir)

        status = ts.get("status", "unknown")
        progress = 100 if status == "completed" else 50 if status == "processing" else 0

        size_bytes = int(info["size"])
        size_mb = round(size_bytes / (1024 * 1024), 2)

        # Para base_id no tenemos "created_at" real del proceso; dejamos None o inferido si existe
        created_at = ts.get("created_at")
        completed_at = ts.get("completed_at")

        pdfs_list.append({
            "id": base_id,
            "filename": f"{base_id}.pdf",
            "size_bytes": size_bytes,
            "size_mb": size_mb,
            "status": status if status in ("completed", "processing", "pending", "failed") else "pending",
            "progress": progress,
            "pages": ts.get("pages"),
            "task_id": ts.get("task_id") or "",
            "upload_time": float(info["mtime"]),
            "created_at": created_at if isinstance(created_at, datetime) else None,
            "completed_at": completed_at if isinstance(completed_at, datetime) else None,
            "extracted_text_path": ts.get("extracted_text_path"),
            "used_ocr": bool(ts.get("used_ocr", False)),
            "error": ts.get("error"),
        })

        # sync en storage para que global-search lo vea
        if base_id not in pdf_storage:
            pdf_storage[base_id] = {
                "filename": f"{base_id}.pdf",
                "pdf_path": info["pdf_path"],
                "size": size_bytes,
                "upload_time": float(info["mtime"]),
                "mode": "local",
                "task_id": None,
            }

    # 2) Flujo normal (pdf_storage)
    for pdf_id, data in list(pdf_storage.items()):
        if pdf_id in latest_map:
            continue
        if not _name_matches_nomenclature(pdf_id, data):
            continue

        ts = pdf_task_status.get(pdf_id, {})
        status = ts.get("status", "unknown")
        task_id = ts.get("task_id")
        mode = ts.get("mode") or data.get("mode") or ("celery" if task_id else "local")

        # Celery: refrescar si está pending
        if mode == "celery" and status == "pending" and task_id:
            task = celery_app.AsyncResult(task_id)
            if task.state == "STARTED":
                status = "processing"
            elif task.state == "SUCCESS":
                status = "completed"
            elif task.state == "FAILURE":
                status = "failed"

        progress = 100 if status == "completed" else 50 if status == "processing" else 0

        size_bytes = int(data.get("size", 0))
        size_mb = round(size_bytes / (1024 * 1024), 2)

        created_at = ts.get("created_at")
        completed_at = ts.get("completed_at")

        pdfs_list.append({
            "id": pdf_id,
            "filename": data.get("filename", f"{pdf_id}.pdf"),
            "size_bytes": size_bytes,
            "size_mb": size_mb,
            "status": status if status in ("completed", "processing", "pending", "failed") else "pending",
            "progress": int(progress),
            "pages": ts.get("pages"),
            "task_id": task_id or "",
            "upload_time": float(data.get("upload_time", time.time())),
            "created_at": created_at if isinstance(created_at, datetime) else None,
            "completed_at": completed_at if isinstance(completed_at, datetime) else None,
            "extracted_text_path": ts.get("extracted_text_path"),
            "used_ocr": bool(ts.get("used_ocr", False)),
            "error": ts.get("error"),
        })

    # Ordenar por upload_time desc
    pdfs_list.sort(key=lambda x: x.get("upload_time", 0), reverse=True)

    by_status_lists = {
        "completed": [p for p in pdfs_list if p["status"] == "completed"],
        "processing": [p for p in pdfs_list if p["status"] == "processing"],
        "pending": [p for p in pdfs_list if p["status"] == "pending"],
        "failed": [p for p in pdfs_list if p["status"] == "failed"],
    }

    return {
        "total": len(pdfs_list),
        "by_status": {
            "completed": len(by_status_lists["completed"]),
            "processing": len(by_status_lists["processing"]),
            "pending": len(by_status_lists["pending"]),
            "failed": len(by_status_lists["failed"]),
        },
        "pdfs": pdfs_list,
        "summary": by_status_lists,
    }

@router.get("/dashboard")
async def get_dashboard():
    """
    Dashboard simple.
    """
    pdfs_info = []

    for pdf_id, data in pdf_storage.items():
        ts = pdf_task_status.get(pdf_id, {})
        status = ts.get("status", "unknown")
        task_id = ts.get("task_id")
        mode = ts.get("mode") or data.get("mode") or ("celery" if task_id else "local")

        if mode == "celery" and task_id:
            task = celery_app.AsyncResult(task_id)
            if task.state == "STARTED":
                status_display = "En procesamiento"
            elif task.state == "SUCCESS":
                status_display = "Completado"
            elif task.state == "FAILURE":
                status_display = "Error"
            else:
                status_display = "En cola"
        else:
            if status == "completed":
                status_display = "Completado"
            elif status == "processing":
                status_display = "En procesamiento"
            elif status == "failed":
                status_display = "Error"
            else:
                status_display = "En cola"

        progress = 100 if status_display == "Completado" else 50 if status_display == "En procesamiento" else 0
        size_mb = round(float(data.get("size", 0)) / (1024 * 1024), 2)

        pdfs_info.append({
            "numero": len(pdfs_info) + 1,
            "nombre_archivo": data.get("filename", f"{pdf_id}.pdf"),
            "tamaño_mb": size_mb,
            "estado": status_display,
            "progreso": f"{progress}%",
            "paginas": ts.get("pages") or "Procesando...",
            "fecha_subida": ts.get("created_at"),
            "fecha_completado": ts.get("completed_at") or "Pendiente",
            "id_interno": pdf_id,
            "ruta_texto": ts.get("extracted_text_path") or data.get("text_path") or "No disponible",
            "ocr_usado": "Sí" if ts.get("used_ocr") else "No",
            "error": ts.get("error") or "Ninguno",
        })

    estados = {
        "completados": len([p for p in pdfs_info if p["estado"] == "Completado"]),
        "procesando": len([p for p in pdfs_info if p["estado"] == "En procesamiento"]),
        "en_cola": len([p for p in pdfs_info if p["estado"] == "En cola"]),
        "con_error": len([p for p in pdfs_info if p["estado"] == "Error"]),
    }

    return {
        "titulo": "Dashboard de Procesamiento de PDFs",
        "total_pdfs": len(pdf_storage),
        "estados": estados,
        "pdfs": pdfs_info,
        "endpoints_utiles": {
            "consultar_estado": "/api/pdf/upload-status/{pdf_id}",
            "buscar_en_pdf": "/api/pdf/{pdf_id}/search",
            "descargar_texto": "/api/pdf/{pdf_id}/text",
            "lista_detallada": "/api/pdf/list",
        },
    }

@router.delete("/{pdf_id}")
async def delete_pdf(pdf_id: str):
    if pdf_id not in pdf_storage:
        raise HTTPException(status_code=404, detail="PDF no encontrado")

    pdf_data = pdf_storage[pdf_id]

    try:
        for key in ("pdf_path", "text_path", "ocr_pdf_path"):
            path = pdf_data.get(key)
            if path and os.path.exists(str(path)):
                try:
                    os.remove(str(path))
                except Exception:
                    logger.exception(f"No se pudo eliminar el archivo {path} para {pdf_id}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error eliminando archivos: {str(e)}")

    pdf_storage.pop(pdf_id, None)
    pdf_task_status.pop(pdf_id, None)
    return {"message": f"PDF {pdf_id} eliminado exitosamente"}

@router.post("/quick-search")
async def quick_search(
    file: UploadFile = File(...),
    search_term: str = Query(...),
    use_ocr: bool = Query(True)
):
    try:
        start_time = time.time()
        file_bytes = await file.read()
        if not file_bytes:
            raise HTTPException(status_code=400, detail="Archivo vacío")

        os.makedirs(settings.UPLOAD_FOLDER, exist_ok=True)
        temp_id = f"temp_{hash(file_bytes) % 1000000}"
        temp_path = os.path.join(settings.UPLOAD_FOLDER, f"{temp_id}.pdf")

        with open(temp_path, "wb") as f:
            f.write(file_bytes)

        text, _, _ = pdf_service.extract_text_from_pdf(temp_path, use_ocr=use_ocr)
        results = pdf_service.search_in_text(text, search_term)

        try:
            if os.path.exists(temp_path):
                os.remove(temp_path)
        except Exception:
            pass

        return {
            "term": search_term,
            "total_matches": len(results),
            "results": results[:50],
            "pdf_id": "temp",
            "execution_time": time.time() - start_time,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en búsqueda rápida: {str(e)}")

@router.post("/global-search")
async def global_search(
    term: str = Query(..., description="Término de búsqueda"),
    case_sensitive: bool = Query(False, description="Coincidir mayúsculas/minúsculas"),
    context_chars: int = Query(100, description="Caracteres de contexto alrededor del match"),
    max_documents: int = Query(100, description="Máximo número de documentos a procesar")
):
    """
    Busca en PDFs que cumplen nomenclatura.
    - Usa texto desde extracted_texts (txt o txt.gz).
    - Incluye base_id de DOCS_ROOT si DOCS_ROOT existe.
    """
    start_time = time.time()
    extracted_dir = Path(settings.EXTRACTED_FOLDER)

    # asegurar versionados en storage si existe DOCS_ROOT
    _ensure_docs_root_in_storage()

    valid_entries = [
        (pdf_id, meta)
        for pdf_id, meta in pdf_storage.items()
        if _name_matches_nomenclature(pdf_id, meta)
    ][:max_documents]

    document_results = []

    for pdf_id, meta in valid_entries:
        ts = pdf_task_status.get(pdf_id, {})

        text_path = ts.get("extracted_text_path") or meta.get("text_path")
        if not text_path or not os.path.exists(str(text_path)):
            txt_candidate = extracted_dir / f"{pdf_id}.txt"
            gz_candidate = extracted_dir / f"{pdf_id}.txt.gz"
            if txt_candidate.exists():
                text_path = str(txt_candidate)
            elif gz_candidate.exists():
                text_path = str(gz_candidate)
            else:
                text_path = None

        text = _load_text_from_file(text_path) if text_path else ""
        if not text:
            continue

        matches = pdf_service.search_in_text(
            text,
            term,
            case_sensitive=case_sensitive,
            context_chars=context_chars
        )

        if matches:
            filename = meta.get("filename", f"{pdf_id}.pdf")
            document_results.append({
                "pdf_id": pdf_id,
                "filename": filename,
                "nombre_carpeta": Path(filename).stem,
                "total_matches": len(matches),
                "results": matches[:20],
                "score": sum(r.get("score", 0) for r in matches),
            })

    document_results.sort(key=lambda x: x["score"], reverse=True)

    return {
        "term": term,
        "total_documents_with_matches": len(document_results),
        "total_matches": sum(doc["total_matches"] for doc in document_results),
        "execution_time": time.time() - start_time,
        "documents": document_results,
    }

@router.get("/{pdf_id}/result")
async def get_ocr_result(pdf_id: str):
    task_status = pdf_task_status.get(pdf_id)

    if not task_status:
        raise HTTPException(status_code=404, detail="PDF no encontrado")

    status = task_status.get("status")

    if status in ("pending", "processing"):
        return {
            "status": status,
            "message": "PDF aún en procesamiento",
            "task_id": task_status.get("task_id") or "",
        }

    if status == "failed":
        return {
            "status": "failed",
            "error": task_status.get("error"),
        }

    return {
        "status": "completed",
        "pdf_id": pdf_id,
        "used_ocr": task_status.get("used_ocr"),
        "pages": task_status.get("pages"),
        "text_path": task_status.get("extracted_text_path"),
        "download_pdf": f"/api/pdf/{pdf_id}/searchable-pdf",
        "download_text": f"/api/pdf/{pdf_id}/text",
    }