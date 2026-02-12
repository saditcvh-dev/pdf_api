from fastapi import APIRouter, UploadFile, File, HTTPException, Query, Body
from fastapi.responses import FileResponse, JSONResponse
import os
import time
from datetime import datetime
from typing import List, Dict
from app.services.pdf_service import PDFService
from app.models.schemas import (
    PDFUploadResponse, 
    PDFUploadStatus,
    SearchRequest, 
    SearchResponse,
    PDFInfo,
    PDFListItem,
    PDFListResponse
)
from pathlib import Path
import os
from datetime import datetime
from app.core.config import settings
from app.tasks.pdf_tasks import process_pdf_task
from app.core.celery_app import celery_app
from app.core.state import pdf_storage, pdf_task_status
import logging
import threading
import subprocess
import shutil
# from fastapi import APIRouter, UploadFile, File, Query, HTTPException
# import os

router = APIRouter()

BASE_DOCS_PATH = "/data/docs"

pdf_storage = {}
pdf_task_status = {}  # ← esto sobra y causa conflicto

# Logger local
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/pdf", tags=["pdf"])
pdf_service = PDFService()


@router.post("/upload", response_model=PDFUploadResponse)
async def upload_pdf(file: UploadFile = File(...), use_ocr: bool = Query(True)):
    """Sube un PDF a la cola de procesamiento sin esperar a que se procese"""
    try:
        if not file.filename.lower().endswith('.pdf'):
            raise HTTPException(status_code=400, detail="Solo se permiten archivos PDF")
        
        file_bytes = await file.read()
        pdf_id = pdf_service.generate_pdf_id(file.filename, file_bytes)
        pages_count = pdf_service.get_pdf_pages_count(file_bytes)

        # 1. Guardar archivo PDF
        pdf_path = os.path.join(settings.UPLOAD_FOLDER, f"{pdf_id}.pdf")
        with open(pdf_path, "wb") as f:
            f.write(file_bytes)
        
        # 2. Encolar tarea de procesamiento en Celery
        task = process_pdf_task.delay(
            pdf_id=pdf_id,
            pdf_path=pdf_path,
            use_ocr=use_ocr
        )
        
        # 3. Guardar metadatos en memoria
        now = datetime.now()
        pdf_storage[pdf_id] = {
            'filename': file.filename,
            'pdf_path': pdf_path,
            'size': len(file_bytes),
            'upload_time': time.time(),
            'task_id': task.id,
            'use_ocr': use_ocr,
            'pages': pages_count
        }
        
        pdf_task_status[pdf_id] = {
            'task_id': task.id,
            'status': 'pending',
            'created_at': now,
            'completed_at': None,
            'pages': pages_count,
            'extracted_text_path': None,
            'used_ocr': use_ocr,
            'error': None
        }
        
        # 4. Devolver respuesta inmediata
        # Si no hay workers conectados, procesar sincrónicamente (mantener comportamiento previo)
        try:
            inspector = celery_app.control.inspect()
            registered = inspector.registered() if inspector else None
        except Exception:
            registered = None

        if not registered:
            # No hay workers -> procesar en background local (no bloquear la petición)
            logger.warning(f"No Celery workers detectados — procesando en hilo local {pdf_id}")

            def _local_process(pid: str, ppath: str, puse_ocr: bool, t_id: str):
                try:
                    pdf_task_status[pid].update({'status': 'processing'})
                    text, pages, used_ocr = pdf_service.extract_text_from_pdf(ppath, use_ocr=puse_ocr)
                    text_path = pdf_service.save_extracted_text(text, pid)

                    pdf_task_status[pid].update({
                        'status': 'completed',
                        'pages': pages,
                        'extracted_text_path': text_path,
                        'used_ocr': used_ocr,
                        'completed_at': datetime.now()
                    })

                    pdf_storage[pid].update({
                        'pages': pages,
                        'text_path': text_path,
                        'text': ''
                    })
                except Exception as e:
                    logger.exception(f"Error en procesamiento local {pid}: {e}")
                    pdf_task_status[pid].update({'status': 'failed', 'error': str(e)})

            thread = threading.Thread(target=_local_process, args=(pdf_id, pdf_path, use_ocr, task.id), daemon=True)
            thread.start()

            return PDFUploadResponse(
                id=pdf_id,
                filename=file.filename,
                size=len(file_bytes),
                task_id=task.id,
                status="pending",
                message="PDF aceptado y procesando localmente (no hay workers).",
                estimated_wait_time=0.0 ,
                pages=pages_count,
            )

        # Si hay workers, devolvemos response pendiente como antes
        return PDFUploadResponse(
            id=pdf_id,
            filename=file.filename,
            size=len(file_bytes),
            task_id=task.id,
            status="pending",
            message="PDF encolado para procesamiento. Usa el endpoint /upload-status/{pdf_id} para consultar el progreso",
            estimated_wait_time=10.0  # Estimado en segundos
            ,pages=pages_count,
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al encolar PDF: {str(e)}")


@router.get("/upload-status/{pdf_id}", response_model=PDFUploadStatus)
async def get_upload_status(pdf_id: str):
    """
    Consulta el estado actual del procesamiento de un PDF.
    Actualiza la información en memoria cuando la tarea ya terminó.
    """
    if pdf_id not in pdf_task_status:
        raise HTTPException(status_code=404, detail="PDF no encontrado")

    task_info = pdf_task_status[pdf_id]
    task_id = task_info.get('task_id')

    if not task_id:
        raise HTTPException(status_code=500, detail="No se encontró ID de tarea asociado")

    # Consultamos el estado real de Celery
    task = celery_app.AsyncResult(task_id)

    # Mapeo de estados de Celery → nuestros estados
    status_map = {
        'PENDING': 'pending',
        'STARTED': 'processing',
        'RETRY': 'processing',       # si hay reintentos
        'SUCCESS': 'completed',
        'FAILURE': 'failed',
    }

    current_status = status_map.get(task.state, task.state.lower())

    # Si la tarea ya terminó exitosamente → actualizamos el estado en memoria
    if task.state == 'SUCCESS' and isinstance(task.result, dict):
        result = task.result
        
        pdf_task_status[pdf_id].update({
            'status': 'completed',
            'pages': result.get('pages'),
            'extracted_text_path': result.get('text_path'),
            'ocr_pdf_path': result.get('pdf_path'),          # ← ruta del PDF con OCR
            'used_ocr': result.get('used_ocr'),
            'text_length': result.get('text_length'),
            'completed_at': datetime.now(),
            'error': None
        })

        # Opcional: también podemos actualizar pdf_storage si lo usas
        if pdf_id in pdf_storage:
            pdf_storage[pdf_id].update({
                'pages': result.get('pages'),
                'text_path': result.get('text_path'),
                'completed': True
            })

    # Si falló → guardamos el error
    elif task.state == 'FAILURE':
        error_msg = str(task.info) if task.info else "Error desconocido en el procesamiento"
        pdf_task_status[pdf_id].update({
            'status': 'failed',
            'error': error_msg,
            'completed_at': datetime.now()
        })
        logger.error(f"Error en tarea {task_id} para pdf {pdf_id}: {error_msg}")

    # Construimos la respuesta final
    task_status = pdf_task_status[pdf_id]

    # Calculamos un progreso aproximado (puedes ajustarlo según necesites)
    progress = 0
    if current_status == 'completed':
        progress = 100
    elif current_status == 'processing':
        progress = 50
    elif current_status == 'pending':
        progress = 10

    return PDFUploadStatus(
        pdf_id=pdf_id,
        task_id=task_id,
        status=current_status,
        progress=progress,
        pages=task_status.get('pages'),
        extracted_text_path=task_status.get('extracted_text_path'),
        ocr_pdf_path=task_status.get('ocr_pdf_path'),           # ahora estará disponible
        used_ocr=task_status.get('used_ocr'),
        error=task_status.get('error'),
        created_at=task_status.get('created_at'),
        completed_at=task_status.get('completed_at')
    )

@router.post("/{pdf_id}/search", response_model=SearchResponse)
async def search_pdf(pdf_id: str, search_request: SearchRequest):
    if pdf_id not in pdf_storage:
        raise HTTPException(status_code=404, detail="PDF no encontrado")
    
    start_time = time.time()
    pdf_data = pdf_storage[pdf_id]
    text = pdf_data['text']
    
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
    if pdf_id not in pdf_storage:
        raise HTTPException(status_code=404, detail="PDF no encontrado")
    
    # Verificar estado del PDF — preferir la fuente `pdf_task_status` cuando exista
    pdf_data = pdf_storage.get(pdf_id, {})
    task_status = pdf_task_status.get(pdf_id, {})
    current_status = task_status.get('status') or pdf_data.get('status')

    # Si el PDF aún está pendiente, devolver estado
    if current_status == 'pending' or (not current_status and task_status):
        return JSONResponse(
            status_code=202,  # Accepted - aún procesando
            content={
                'status': 'pending',
                'message': 'PDF aún está siendo procesado',
                'task_id': task_status.get('task_id') or pdf_data.get('task_id'),
                'progress': task_status.get('progress', pdf_data.get('progress', 0))
            }
        )

    # Si hay error, devolverlo
    if current_status == 'failed':
        return JSONResponse(
            status_code=400,
            content={
                'status': 'failed',
                'message': 'Error al procesar PDF',
                'error': task_status.get('error') or pdf_data.get('error')
            }
        )

    # Si el PDF está completado, devolver el texto (buscar en pdf_task_status o pdf_storage)
    text_path = task_status.get('extracted_text_path') or pdf_data.get('text_path')
    if text_path and os.path.exists(text_path):
        return FileResponse(
            text_path, 
            media_type='text/plain',
            filename=f"{pdf_id}_texto.txt"
        )
    else:
        raise HTTPException(status_code=404, detail="Texto no encontrado")
    
@router.get("/{pdf_id}/searchable-pdf")
async def get_searchable_pdf(pdf_id: str):
    # Intentamos obtener de task_status primero
    task_status = pdf_task_status.get(pdf_id, {})
    
    # Fallback: si no está en task_status pero sí en storage
    if not task_status and pdf_id in pdf_storage:
        logger.warning(f"pdf_id {pdf_id} encontrado solo en pdf_storage (fallback)")
        # Creamos una entrada mínima en task_status para no fallar
        pdf_task_status[pdf_id] = {
            "task_id": pdf_storage[pdf_id].get("task_id"),
            "status": pdf_storage[pdf_id].get("status", "unknown"),
            "ocr_pdf_path": os.path.join(settings.OUTPUTS_FOLDER, f"{pdf_id}.pdf"),
            "created_at": pdf_storage[pdf_id].get("upload_time")
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
                "task_id": task_status.get("task_id"),
                "progress": task_status.get("progress", 0)
            }
        )

    if current_status == "failed":
        return JSONResponse(
            status_code=400,
            content={
                "status": "failed",
                "message": "El proceso de OCR falló",
                "error": task_status.get("error", "Sin detalles")
            }
        )

    # Ruta del PDF con OCR
    ocr_pdf_path = task_status.get("ocr_pdf_path") or os.path.join(
        settings.OUTPUTS_FOLDER, f"{pdf_id}.pdf"
    )

    if not os.path.exists(ocr_pdf_path):
        logger.error(f"Archivo OCR no encontrado: {ocr_pdf_path}")
        raise HTTPException(
            status_code=404,
            detail="El archivo OCR no se encuentra en la carpeta outputs"
        )

    return FileResponse(
        ocr_pdf_path,
        media_type="application/pdf",
        filename=f"{pdf_id}_searchable.pdf"
    )


@router.post("/merge-with-output")
async def merge_with_output(
    first_pdf_id: str = Query(..., description="ID del PDF ya procesado en outputs"),
    file: UploadFile = File(...),
    use_ocr: bool = Query(True),
    position: str = Query("end", description="Dónde insertar el segundo PDF: 'start' o 'end'")
):
    """
    Recibe un PDF ya procesado (referido por `first_pdf_id` en outputs) y un segundo PDF sin procesar.
    Procesa el segundo con OCRmyPDF, guarda sus textos y luego une ambos PDFs en `outputs`.
    Devuelve el PDF combinado.
    """
    try:
        # Verificar que el primer PDF procesado exista en outputs para desarrolllo ---Versoon para windows 
        # first_path = os.path.join(settings.OUTPUTS_FOLDER, f"{first_pdf_id}.pdf")
        # Extraer partes del ID
            #  Normalizar la ruta para evitar path traversal (../../ etc)
        normalized_path = os.path.normpath(first_pdf_id)

        # Evitar que intenten salir de /data/docs
        if normalized_path.startswith("..") or os.path.isabs(normalized_path):
            raise HTTPException(
                status_code=400,
                detail="Ruta inválida"
            )

        # Construir ruta absoluta real
        first_path = os.path.join(BASE_DOCS_PATH, normalized_path)

        # Validar que realmente esté dentro de /data/docs
        if not os.path.commonpath([first_path, BASE_DOCS_PATH]) == BASE_DOCS_PATH:
            raise HTTPException(
                status_code=400,
                detail="Ruta fuera del directorio permitido"
            )

        # Verificar existencia
        if not os.path.exists(first_path):
            raise HTTPException(
                status_code=404,
                detail=f"PDF no encontrado en producción: {first_path}"
            )


        # Guardar el segundo PDF temporalmente
        file_bytes = await file.read()
        second_id = pdf_service.generate_pdf_id(file.filename, file_bytes)
        uploads_dir = settings.UPLOAD_FOLDER
        os.makedirs(uploads_dir, exist_ok=True)
        second_path = os.path.join(uploads_dir, f"{second_id}.pdf")
        with open(second_path, "wb") as f:
            f.write(file_bytes)

        # Aplicar OCR al segundo PDF (sin Celery, síncrono) usando ocrmypdf
        os.makedirs(settings.OUTPUTS_FOLDER, exist_ok=True)
        ocr_second_output = os.path.join(settings.OUTPUTS_FOLDER, f"{second_id}.pdf")

        command = [
            "ocrmypdf",
            "-l", "spa",
            "--force-ocr",
            "--optimize", "0",
            "--output-type", "pdf",
            "--jpeg-quality", "100",
            second_path,
            ocr_second_output
        ]

        result = subprocess.run(command, capture_output=True, text=True, check=False)
        if result.returncode != 0 or not os.path.exists(ocr_second_output):
            # limpiar temporal
            if os.path.exists(second_path):
                os.remove(second_path)
            raise HTTPException(status_code=500, detail=f"OCRmyPDF falló: {result.stderr.strip()}")

        # Extraer texto del segundo y guardar
        text, pages, used_ocr = pdf_service.extract_text_from_pdf(second_path, use_ocr=use_ocr)
        text_path = pdf_service.save_extracted_text(text, second_id)

        # Actualizar metadatos en memoria
        now = datetime.now()
        pdf_storage[second_id] = {
            'filename': file.filename,
            'pdf_path': second_path,
            'size': len(file_bytes),
            'upload_time': time.time(),
            'task_id': None,
            'use_ocr': use_ocr,
            'pages': pages,
            'text_path': text_path
        }
        pdf_task_status[second_id] = {
            'task_id': None,
            'status': 'completed',
            'created_at': now,
            'completed_at': datetime.now(),
            'pages': pages,
            'extracted_text_path': text_path,
            'ocr_pdf_path': ocr_second_output,
            'used_ocr': used_ocr,
            'text_length': len(text)
        }

        # Unir PDFs (primero procesado + segundo procesado)
        merged_id = f"{first_pdf_id}_{second_id}_merged"
        merged_output = os.path.join(settings.OUTPUTS_FOLDER, f"{merged_id}.pdf")
        merged_path = pdf_service.merge_pdfs([first_path, ocr_second_output], merged_output, position=position)

        # Guardar metadatos del PDF combinado
        pdf_storage[merged_id] = {
            'filename': f"{merged_id}.pdf",
            'pdf_path': merged_path,
            'size': os.path.getsize(merged_path),
            'upload_time': time.time()
        }
        pdf_task_status[merged_id] = {
            'task_id': None,
            'status': 'completed',
            'created_at': now,
            'completed_at': datetime.now(),
            'pages': (pdf_task_status.get(first_pdf_id, {}).get('pages') or 0) + pages,
            'extracted_text_path': None,
            'ocr_pdf_path': merged_path,
            'used_ocr': True
        }

        return FileResponse(merged_path, media_type="application/pdf", filename=f"{merged_id}.pdf")

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en merge: {str(e)}")

@router.get("/{pdf_id}/info", response_model=PDFInfo)
async def get_pdf_info(pdf_id: str):
    if pdf_id not in pdf_storage:
        raise HTTPException(status_code=404, detail="PDF no encontrado")
    
    pdf_data = pdf_storage[pdf_id]
    text_file_size = None
    text_path = pdf_data.get('text_path')
    if text_path and os.path.exists(text_path):
        text_file_size = os.path.getsize(text_path)
    
    return PDFInfo(
        id=pdf_id,
        filename=pdf_data['filename'],
        upload_date=datetime.fromtimestamp(pdf_data['upload_time']),
        size=pdf_data['size'],
        pages=pdf_data.get('pages'),  # Puede ser None si aún se procesa
        has_text=bool(pdf_data.get('text', '').strip()),
        text_file_size=text_file_size
    )

@router.get("/list", response_model=PDFListResponse)
async def list_pdfs():
    """
    Devuelve la lista de todos los PDFs con su estado de procesamiento.
    Útil para ver qué PDFs están en cola, en proceso o completados.
    """
    # ========== AGREGAR: Cargar PDFs existentes en disco ==========
    def load_existing_pdfs():
        uploads_dir = Path("uploads")
        if not uploads_dir.exists():
            return
            
        for pdf_file in uploads_dir.glob("*.pdf"):
            pdf_id = pdf_file.stem
            
            # Solo agregar si no existe
            if pdf_id not in pdf_storage:
                # Convertir datetime a timestamp (float) para Pydantic
                upload_time_dt = datetime.fromtimestamp(pdf_file.stat().st_mtime)
                upload_time_ts = upload_time_dt.timestamp()
                
                pdf_storage[pdf_id] = {
                    'filename': pdf_file.name,
                    'size': pdf_file.stat().st_size,
                    'upload_time': upload_time_ts  # TIMESTAMP en lugar de datetime
                }
                
                # Determinar estado basado en archivos
                txt_path = Path("extracted_texts") / f"{pdf_id}.txt"
                output_pdf_path = Path("outputs") / f"{pdf_id}.pdf"
                
                # Convertir datetime a timestamp para created_at y completed_at
                created_at_dt = datetime.fromtimestamp(pdf_file.stat().st_mtime)
                created_at_ts = created_at_dt.timestamp()
                
                if txt_path.exists() or output_pdf_path.exists():
                    # Usar timestamp de modificación del txt o output
                    if txt_path.exists():
                        completed_at_ts = txt_path.stat().st_mtime
                    else:
                        completed_at_ts = output_pdf_path.stat().st_mtime
                    
                    pdf_task_status[pdf_id] = {
                        'status': 'completed',
                        'created_at': created_at_ts,  # TIMESTAMP
                        'completed_at': completed_at_ts,  # TIMESTAMP
                        'used_ocr': output_pdf_path.exists(),
                        'extracted_text_path': str(txt_path) if txt_path.exists() else None
                    }
                else:
                    # PDF subido pero no procesado
                    pdf_task_status[pdf_id] = {
                        'status': 'unknown',
                        'created_at': created_at_ts  # TIMESTAMP
                    }
    # ========== FIN DE LA PARTE AGREGADA ==========
    
    # Llamar a la función para cargar PDFs existentes
    load_existing_pdfs()
    
    pdfs_list = []
    
    for pdf_id, data in pdf_storage.items():
        # Obtener estado de procesamiento
        task_status_info = pdf_task_status.get(pdf_id, {})
        status = task_status_info.get('status', 'unknown')
        task_id = task_status_info.get('task_id')
        
        # Si el PDF aún se está procesando, consultar estado real en Celery
        if status == 'pending' and task_id:
            task = celery_app.AsyncResult(task_id)
            if task.state == 'STARTED':
                status = 'processing'
            elif task.state == 'SUCCESS':
                status = 'completed'
            elif task.state == 'FAILURE':
                status = 'failed'
        
        # Calcular progreso basado en estado
        if status == 'completed':
            progress = 100
        elif status == 'processing':
            progress = 50
        elif status == 'failed':
            progress = 0
        else:  # pending, unknown
            progress = 0
        
        # Calcular tamaño en MB
        size_mb = round(data['size'] / (1024 * 1024), 2)
        
        # Obtener valores asegurando tipos correctos para Pydantic
        task_id_val = task_id if task_id else ""
        
        # Para campos datetime, asegurarse de que son timestamps (float) o None
        upload_time_val = data.get('upload_time')
        created_at_val = task_status_info.get('created_at')
        completed_at_val = task_status_info.get('completed_at')
        
        # Si los valores son datetime, convertirlos a timestamp
        if isinstance(upload_time_val, datetime):
            upload_time_val = upload_time_val.timestamp()
        if isinstance(created_at_val, datetime):
            created_at_val = created_at_val.timestamp()
        if isinstance(completed_at_val, datetime):
            completed_at_val = completed_at_val.timestamp()
        
        pdfs_list.append({
            "id": pdf_id,
            "filename": data['filename'],
            "size_bytes": data['size'],
            "size_mb": size_mb,
            "status": status,
            "progress": progress,
            "pages": task_status_info.get('pages'),
            "task_id": task_id_val,  # Siempre string, nunca None
            "upload_time": upload_time_val,  # timestamp float
            "created_at": created_at_val,  # timestamp float o None
            "completed_at": completed_at_val,  # timestamp float o None
            "extracted_text_path": task_status_info.get('extracted_text_path'),
            "used_ocr": task_status_info.get('used_ocr', False),
            "error": task_status_info.get('error')
        })
    
    # Agrupar por estado para facilitar visualización
    by_status = {
        'completed': [p for p in pdfs_list if p['status'] == 'completed'],
        'processing': [p for p in pdfs_list if p['status'] == 'processing'],
        'pending': [p for p in pdfs_list if p['status'] == 'pending'],
        'failed': [p for p in pdfs_list if p['status'] == 'failed'],
    }
    
    return {
        "total": len(pdf_storage),
        "by_status": {
            "completed": len(by_status['completed']),
            "processing": len(by_status['processing']),
            "pending": len(by_status['pending']),
            "failed": len(by_status['failed'])
        },
        "pdfs": pdfs_list,
        "summary": {
            "completed": by_status['completed'],
            "processing": by_status['processing'],
            "pending": by_status['pending'],
            "failed": by_status['failed']
        }
    }
@router.get("/dashboard")
async def get_dashboard():
    """
    Dashboard visual amigable para ver el estado de los PDFs.
    Devuelve información legible para los usuarios.
    """
    pdfs_info = []
    
    for pdf_id, data in pdf_storage.items():
        task_status_info = pdf_task_status.get(pdf_id, {})
        status = task_status_info.get('status', 'unknown')
        task_id = task_status_info.get('task_id')
        
        # Consultar estado real en Celery
        if task_id:
            task = celery_app.AsyncResult(task_id)
            if task.state == 'STARTED':
                status = 'processing'
                status_display = "⏳ En procesamiento"
            elif task.state == 'SUCCESS':
                status = 'completed'
                status_display = "✅ Completado"
            elif task.state == 'FAILURE':
                status = 'failed'
                status_display = "❌ Error"
            else:
                status_display = "⏸️ En cola"
        else:
            status_display = "⏸️ En cola"
        
        # Calcular progreso
        if status == 'completed':
            progress = 100
            progress_bar = "████████████████████ 100%"
        elif status == 'processing':
            progress = 50
            progress_bar = "██████████░░░░░░░░░░ 50%"
        else:
            progress = 0
            progress_bar = "░░░░░░░░░░░░░░░░░░░░ 0%"
        
        size_mb = round(data['size'] / (1024 * 1024), 2)
        
        pdfs_info.append({
            "numero": len(pdfs_info) + 1,
            "nombre_archivo": data['filename'],
            "tamaño_mb": size_mb,
            "estado": status_display,
            "progreso": f"{progress}%",
            "barra_progreso": progress_bar,
            "paginas": task_status_info.get('pages') or "Procesando...",
            "fecha_subida": task_status_info.get('created_at'),
            "fecha_completado": task_status_info.get('completed_at') or "Pendiente",
            "id_interno": pdf_id,
            "ruta_texto": task_status_info.get('extracted_text_path') or "No disponible",
            "ocr_usado": "Sí" if task_status_info.get('used_ocr') else "No",
            "error": task_status_info.get('error') or "Ninguno"
        })
    
    # Contar estados
    estados = {
        "completados": len([p for p in pdfs_info if "✅" in p['estado']]),
        "procesando": len([p for p in pdfs_info if "⏳" in p['estado']]),
        "en_cola": len([p for p in pdfs_info if "⏸️" in p['estado']]),
        "con_error": len([p for p in pdfs_info if "❌" in p['estado']])
    }
    
    return {
        "titulo": "📊 Dashboard de Procesamiento de PDFs",
        "total_pdfs": len(pdf_storage),
        "estados": estados,
        "pdfs": pdfs_info,
        "endpoints_ultiles": {
            "consultar_estado": "/api/pdf/upload-status/{pdf_id}",
            "buscar_en_pdf": "/api/pdf/search/{pdf_id}",
            "descargar_texto": "/api/pdf/{pdf_id}/text",
            "lista_detallada": "/api/pdf/list"
        }
    }

@router.delete("/{pdf_id}")
async def delete_pdf(pdf_id: str):
    if pdf_id not in pdf_storage:
        raise HTTPException(status_code=404, detail="PDF no encontrado")
    
    pdf_data = pdf_storage[pdf_id]
    
    try:
        if os.path.exists(pdf_data['pdf_path']):
            os.remove(pdf_data['pdf_path'])
        if os.path.exists(pdf_data['text_path']):
            os.remove(pdf_data['text_path'])
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error eliminando archivos: {str(e)}")
    
    del pdf_storage[pdf_id]
    
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
        
        temp_id = f"temp_{hash(file_bytes) % 1000000}"
        temp_path = os.path.join(settings.UPLOAD_FOLDER, f"{temp_id}.pdf")
        
        with open(temp_path, "wb") as f:
            f.write(file_bytes)
        
        text, pages, _ = pdf_service.extract_text_from_pdf(temp_path, use_ocr=use_ocr)
        results = pdf_service.search_in_text(text, search_term)
        
        if os.path.exists(temp_path):
            os.remove(temp_path)
        
        execution_time = time.time() - start_time
        
        return SearchResponse(
            term=search_term,
            total_matches=len(results),
            results=results[:50],
            pdf_id="temp",
            execution_time=execution_time
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en búsqueda rápida: {str(e)}")


# ======================
# NUEVO ENDPOINT: BÚSQUEDA GLOBAL
# ======================

@router.post("/global-search")
async def global_search(
    term: str = Query(..., description="Término de búsqueda"),
    case_sensitive: bool = Query(False, description="Coincidir mayúsculas/minúsculas"),
    context_chars: int = Query(100, description="Caracteres de contexto alrededor del match"),
    max_documents: int = Query(100, description="Máximo número de documentos a procesar")
):
    """
    Busca un término en TODOS los PDFs procesados y almacenados.
    Devuelve lista de documentos con coincidencias, ordenados por relevancia.
    """
    start_time = time.time()

    # Usar el método del servicio
    document_results = pdf_service.search_across_documents(
        search_term=term,
        case_sensitive=case_sensitive,
        context_chars=context_chars,
        max_documents=max_documents
    )

    # Calcular estadísticas globales
    total_matches = sum(len(doc['results']) for doc in document_results)
    total_documents_with_matches = len(document_results)

    # Enriquecer respuesta con filename desde pdf_storage (opcional)
    enriched_results = []
    for doc in document_results:
        pdf_id = doc['pdf_id']
        filename = pdf_storage.get(pdf_id, {}).get('filename', pdf_id)
        
        enriched_results.append({
            "pdf_id": pdf_id,
            "filename": filename,
            "total_matches": len(doc['results']),
            "results": doc['results'][:20],  # Limitar resultados por documento
            "score": sum(r['score'] for r in doc['results'])
        })

    execution_time = time.time() - start_time

    return {
        "term": term,
        "total_documents_with_matches": total_documents_with_matches,
        "total_matches": total_matches,
        "execution_time": execution_time,
        "documents": enriched_results
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
            "task_id": task_status.get("task_id")
        }

    if status == "failed":
        return {
            "status": "failed",
            "error": task_status.get("error")
        }

    return {
        "status": "completed",
        "pdf_id": pdf_id,
        "used_ocr": task_status.get("used_ocr"),
        "pages": task_status.get("pages"),
        "text_path": task_status.get("extracted_text_path"),
        "ocr_pdf_path": task_status.get("ocr_pdf_path"),
        "download_pdf": f"/api/pdf/{pdf_id}/searchable-pdf",
        "download_text": f"/api/pdf/{pdf_id}/text"
    }





