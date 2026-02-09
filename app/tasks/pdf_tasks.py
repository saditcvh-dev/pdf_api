

from celery import shared_task
import logging
import subprocess
import os
from app.services.pdf_service import PDFService
from app.core.config import settings

logger = logging.getLogger(__name__)
BASE_DIR = os.path.abspath(os.getcwd())

OCRMY_PDF = os.path.join(
    BASE_DIR,
    "venv",
    "Scripts",
    "ocrmypdf.exe"
)

@shared_task(bind=True, name="process_pdf_task")
def process_pdf_task(self, pdf_id: str, pdf_path: str, use_ocr: bool = True):
    """
    Procesa el PDF completo: aplica OCRmyPDF + extrae texto + guarda archivos.
    Versión sin subtarea para evitar el error de .get() dentro de Celery.
    """
    try:
        logger.info(f"[INICIO] Procesando PDF: {pdf_id} | Ruta: {pdf_path}")

        # ────────────────────────────────────────────────────────────────
        # Etapa 1: Aplicar OCR con ocrmypdf
        # ────────────────────────────────────────────────────────────────
        self.update_state(
            state="PROGRESS",
            meta={
                "status": "Aplicando OCR con OCRmyPDF...",
                "current": 1,
                "total": 3,
                "pdf_id": pdf_id
            }
        )

        ocr_pdf_path = os.path.join(settings.OUTPUTS_FOLDER, f"{pdf_id}.pdf")
        os.makedirs(settings.OUTPUTS_FOLDER, exist_ok=True)
        command = [
            OCRMY_PDF,
            "-l", "spa",
            "--force-ocr",
            "--tagged-pdf-mode", "ignore",
            "--optimize", "0",
            "--output-type", "pdf",
            "--jpeg-quality", "100",
            "--max-image-mpixels", "1000",
            "--jobs", "8",
            pdf_path,
            ocr_pdf_path
        ]

        # command = [
        #     "ocrmypdf",
        #     "-l", "spa",
        #     "--force-ocr",
        #     "--optimize", "0",
        #     "--output-type", "pdf",
        #     "--jpeg-quality", "100",
        #     pdf_path,
        #     ocr_pdf_path
        # ]

        logger.info(f"Ejecutando comando: {' '.join(command)}")

        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False  # No lanzamos excepción automática
        )

        if result.returncode != 0:
            logger.error(f"OCRmyPDF falló (código {result.returncode})\nStderr: {result.stderr}")
            raise RuntimeError(f"OCRmyPDF falló con código {result.returncode}: {result.stderr.strip()}")

        if not os.path.exists(ocr_pdf_path):
            raise RuntimeError("OCRmyPDF no generó el archivo de salida")

        logger.info(f"OCR completado → archivo generado: {ocr_pdf_path}")

        # ────────────────────────────────────────────────────────────────
        # Etapa 2: Extracción de texto
        # ────────────────────────────────────────────────────────────────
        self.update_state(
            state="PROGRESS",
            meta={
                "status": "Extrayendo texto...",
                "current": 2,
                "total": 3,
                "pdf_id": pdf_id
            }
        )

        pdf_service = PDFService()

        # Puedes cambiar a ocr_pdf_path si prefieres extraer del resultado OCR
        text, pages, used_ocr = pdf_service.extract_text_from_pdf(
            ocr_pdf_path, use_ocr=False
        )

        logger.info(f"[ÉXITO] Texto extraído: {len(text):,} caracteres | {pages} páginas | OCR usado: {used_ocr}")

        # ────────────────────────────────────────────────────────────────
        # Etapa 3: Guardado final
        # ────────────────────────────────────────────────────────────────
        self.update_state(
            state="PROGRESS",
            meta={
                "status": "Guardando archivos finales...",
                "current": 3,
                "total": 3,
                "pdf_id": pdf_id
            }
        )

        final_pdf_path = pdf_service.save_processed_pdf(ocr_pdf_path, pdf_id)
        text_path = pdf_service.save_extracted_text(text, pdf_id)

        logger.info(f"[ÉXITO] PDF con OCR guardado en: {final_pdf_path}")
        logger.info(f"[ÉXITO] Texto guardado en: {text_path}")

        # Resultado final
        result_dict = {
            'pdf_id': pdf_id,
            'pages': pages,
            'pdf_path': final_pdf_path,
            'text_path': text_path,
            'used_ocr': used_ocr,
            'text_length': len(text),
            'status': 'completed'
        }

        # Opcional: marcar éxito explícitamente
        self.update_state(
            state="SUCCESS",
            meta=result_dict
        )

        return result_dict

    except Exception as e:
        logger.exception(f"[ERROR] Fallo en procesamiento de {pdf_id}")
        raise
