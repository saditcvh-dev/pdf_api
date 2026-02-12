# C:\Angular\OCR\pdf_api\app\services\pdf_service.py

import fitz  # PyMuPDF
import re
import os
import uuid
import hashlib
import logging
import concurrent.futures
from typing import List, Tuple, Dict
from datetime import datetime
from pathlib import Path
from bisect import bisect_left
from app.core import state

from .ocr_service import OCRService
from app.core.config import settings
import gc
import gzip
from app.core.state import pdf_storage, pdf_task_status

from PyPDF2 import PdfReader
from io import BytesIO

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PDFService:
    def __init__(self):
        self.ocr_service = OCRService(settings.TESSERACT_CMD)
        self.max_workers = min(4, os.cpu_count() or 2)  # Ajuste seguro de hilos

    def extract_text_from_pdf(self, pdf_path: str, use_ocr: bool = True,
                             language: str = 'spa', batch_size: int = 20) -> Tuple[str, int, bool]:
        """Extrae texto de PDF optimizado para velocidad y memoria"""
        doc = None
        try:
            doc = fitz.open(pdf_path)
            total_pages = len(doc)
            used_ocr = False

            logger.info(f"Iniciando extracción de {total_pages} páginas")

            if total_pages > 100:
                text, used_ocr = self._extract_large_pdf(doc, total_pages, use_ocr, language, batch_size)
            else:
                text, used_ocr = self._extract_small_pdf(doc, total_pages, use_ocr, language)

            return text, total_pages, used_ocr

        except Exception as e:
            logger.error(f"Error extrayendo texto: {e}")
            return "", 0, False
        finally:
            if doc:
                doc.close()
            gc.collect()

    def _extract_small_pdf(self, doc: fitz.Document, total_pages: int,
                          use_ocr: bool, language: str) -> Tuple[str, bool]:
        """Extracción secuencial para PDFs pequeños"""
        text_parts = []
        used_ocr = False

        for page_num in range(total_pages):
            page = doc.load_page(page_num)
            page_text = page.get_text()

            if not page_text.strip() and use_ocr:
                page_text = self._ocr_page(page, page_num, language)
                if page_text.strip():
                    used_ocr = True

            text_parts.append(f"\n--- Página {page_num + 1} ---\n{page_text}")

            if (page_num + 1) % 10 == 0:
                logger.info(f"Procesadas {page_num + 1}/{total_pages} páginas")

        return "".join(text_parts), used_ocr

    def _extract_large_pdf(self, doc: fitz.Document, total_pages: int,
                          use_ocr: bool, language: str, batch_size: int) -> Tuple[str, bool]:
        """Extracción paralela por lotes para PDFs grandes"""
        text_parts = [""] * total_pages
        used_ocr = False

        def process_page(page_num: int):
            nonlocal used_ocr
            try:
                page = doc.load_page(page_num)
                page_text = page.get_text("text")

                if not page_text.strip() and use_ocr:
                    page_text = self._ocr_page(page, page_num, language)
                    if page_text.strip():
                        used_ocr = True

                return page_num, f"\n--- Página {page_num + 1} ---\n{page_text}"
            except Exception as e:
                logger.error(f"Error en página {page_num + 1}: {e}")
                return page_num, f"\n--- Página {page_num + 1} ---\n[Error procesando página]"

        for batch_start in range(0, total_pages, batch_size):
            batch_end = min(batch_start + batch_size, total_pages)
            page_nums = range(batch_start, batch_end)

            logger.info(f"Procesando lote paralelo {batch_start + 1}-{batch_end}")

            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                for page_num, text in executor.map(process_page, page_nums):
                    text_parts[page_num] = text

            gc.collect()

        return "".join(text_parts), used_ocr

    def _ocr_page(self, page: fitz.Page, page_num: int, language: str) -> str:
        """OCR optimizado con DPI dinámico - Fallback a extracción básica si falla"""
        try:
            rect = page.rect
            # DPI más alto para texto pequeño, más bajo para páginas grandes
            base_dpi = 400 if max(rect.width, rect.height) < 1000 else 300
            zoom = base_dpi / 72
            mat = fitz.Matrix(zoom, zoom)

            pix = page.get_pixmap(matrix=mat, alpha=False)
            img_bytes = pix.tobytes("png")

            return self.ocr_service.extract_text_from_image(img_bytes, language)

        except Exception as e:
            logger.warning(f"OCR falló en página {page_num + 1}: {e}. Usando extracción básica...")
            # Fallback: intenta extraer texto sin OCR
            try:
                return page.get_text()
            except:
                return ""

    def extract_text_from_pdf_bytes(self, pdf_bytes: bytes, use_ocr: bool = True,
                                   language: str = 'spa', batch_size: int = 20) -> Tuple[str, int, bool]:
        """Extracción directa desde bytes (sin tocar disco)"""
        doc = None
        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            total_pages = len(doc)

            if total_pages > 100:
                text, used_ocr = self._extract_large_pdf(doc, total_pages, use_ocr, language, batch_size)
            else:
                text, used_ocr = self._extract_small_pdf(doc, total_pages, use_ocr, language)

            return text, total_pages, used_ocr

        except Exception as e:
            logger.error(f"Error procesando PDF desde bytes: {e}")
            return "", 0, False
        finally:
            if doc:
                doc.close()
            gc.collect()

    def search_in_text(self, text: str, search_term: str,
                      case_sensitive: bool = False, context_chars: int = 100) -> List[dict]:
        """Búsqueda rápida con detección eficiente de página"""
        if not text or not search_term:
            return []

        # Detectar separadores de página
        page_matches = list(re.finditer(r'--- Página (\d+) ---', text))
        if not page_matches:
            return []  # Sin estructura de páginas

        page_positions = [(int(m.group(1)), m.start()) for m in page_matches]
        positions = [pos for _, pos in page_positions]

        search_text = text if case_sensitive else text.lower()
        search_term_adj = search_term if case_sensitive else search_term.lower()

        results = []
        for match in re.finditer(re.escape(search_term_adj), search_text):
            start_pos = match.start()

            # Búsqueda binaria para encontrar página
            idx = bisect_left(positions, start_pos)
            if idx == 0:
                page_num = page_positions[0][0]
            elif idx >= len(positions):
                page_num = page_positions[-1][0]
            else:
                page_num = page_positions[idx - 1][0]

            start = max(0, start_pos - context_chars)
            end = min(len(text), match.end() + context_chars)

            context = text[start:end]
            snippet = text[match.start():match.end()]

            results.append({
                'page': page_num,
                'position': start_pos,
                'context': context,
                'snippet': snippet,
                'score': self._calculate_relevance_score(context, search_term)
            })

        results.sort(key=lambda x: x['score'], reverse=True)
        return results

    def _calculate_relevance_score(self, context: str, search_term: str) -> float:
        term_count = context.lower().count(search_term.lower())
        context_len = len(context)
        return (term_count * 10) + (100 / max(1, context_len / 100))

    def save_extracted_text(self, text: str, pdf_id: str) -> str:
        filename = f"{pdf_id}.txt"
        filepath = Path(settings.EXTRACTED_FOLDER) / filename
        filepath.parent.mkdir(parents=True, exist_ok=True)

        if len(text) > 10 * 1024 * 1024:
            import gzip
            with gzip.open(str(filepath) + '.gz', 'wt', encoding='utf-8') as f:
                f.write(text)
            final_path = str(filepath) + '.gz'
        else:
            filepath.write_text(text, encoding='utf-8')
            final_path = str(filepath)

        logger.info(f"Texto guardado en *{final_path}")
        return final_path
    
    def generate_pdf_id(self, filename: str, file_bytes: bytes) -> str:
        """ID único basado en contenido (ideal para deduplicación)"""
        content_hash = hashlib.sha256(file_bytes).hexdigest()[:16]
        safe_name = re.sub(r'[^\w\-_]', '_', Path(filename).stem)[:30]
        return f"{safe_name}_{content_hash}"

    def get_pdf_info(self, pdf_path: str) -> dict:
        try:
            doc = fitz.open(pdf_path)
            info = {
                'pages': len(doc),
                'size_mb': round(os.path.getsize(pdf_path) / (1024 * 1024), 2),
                'has_text': False,
                'estimated_processing_time': round(len(doc) * 0.5 / 60, 2),
                'page_sizes': []
            }

            sample_pages = min(5, len(doc))
            for i in range(sample_pages):
                page = doc.load_page(i)
                if page.get_text().strip():
                    info['has_text'] = True
                rect = page.rect
                info['page_sizes'].append({'width': round(rect.width), 'height': round(rect.height)})

            doc.close()
            return info
        except Exception as e:
            logger.error(f"Error obteniendo info PDF: {e}")
            return {}

    def analyze_pdf_structure(self, pdf_path: str) -> dict:
        try:
            doc = fitz.open(pdf_path)
            total = len(doc)
            analysis = {
                'total_pages': total,
                'likely_scanned': True,
                'recommended_dpi': 300,
                'suggested_batch_size': 15,
                'processing_strategy': 'hybrid'
            }

            sample_indices = [0, total // 2, total - 1] if total > 2 else [0]
            text_pages = 0

            for idx in sample_indices[:3]:
                if idx < total:
                    page_text = doc.load_page(idx).get_text()
                    if len(page_text.strip()) > 100:
                        text_pages += 1

            if text_pages == len(sample_indices):
                analysis.update({
                    'processing_strategy': 'text_only',
                    'likely_scanned': False,
                    'recommended_dpi': 150
                })
            elif text_pages == 0:
                analysis.update({
                    'processing_strategy': 'ocr_only',
                    'likely_scanned': True,
                    'recommended_dpi': 350,
                    'suggested_batch_size': 8
                })

            if total > 300:
                analysis['suggested_batch_size'] = 20

            doc.close()
            return analysis
        except Exception as e:
            logger.error(f"Error analizando estructura: {e}")
            return {}

    def search_across_documents(self, search_term: str, case_sensitive: bool = False, 
                               context_chars: int = 100, max_documents: int = 50) -> List[Dict]:
        """
        Busca en múltiples textos extraídos guardados en EXTRACTED_FOLDER.
        
        Args:
            search_term: Término de búsqueda
            case_sensitive: Sensible a mayúsculas
            context_chars: Caracteres de contexto
            max_documents: Máximo de documentos a procesar (para rendimiento)
        
        Returns:
            Lista de dicts con: pdf_id, filepath, results (lista de matches por doc)
        """
        results = []
        extracted_path = Path(settings.EXTRACTED_FOLDER)
        if not extracted_path.exists():
            logger.warning(f"Carpeta {extracted_path} no existe")
            return results

        # Listar archivos .txt y .txt.gz
        files = list(extracted_path.glob('*.txt')) + list(extracted_path.glob('*.txt.gz'))
        files = files[:max_documents]  # Limitar para evitar sobrecarga

        logger.info(f"Buscando en {len(files)} documentos")

        def load_text(filepath: Path) -> Tuple[str, str]:
            """Carga texto, manejando gz"""
            try:
                if filepath.suffix == '.gz':
                    with gzip.open(filepath, 'rt', encoding='utf-8') as f:
                        return f.read(), str(filepath)
                else:
                    return filepath.read_text(encoding='utf-8'), str(filepath)
            except Exception as e:
                logger.error(f"Error cargando {filepath}: {e}")
                return "", ""

        # Cargar y buscar en paralelo
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(load_text, f): f for f in files}
            
            for future in concurrent.futures.as_completed(futures):
                text, filepath = future.result()
                if not text:
                    continue
                    
                pdf_id = Path(filepath).stem  # Quitar .txt o .txt.gz
                if pdf_id.endswith('.txt'):
                    pdf_id = pdf_id[:-4]  # Limpiar si es .txt.gz
                
                matches = self.search_in_text(
                    text, 
                    search_term, 
                    case_sensitive=case_sensitive, 
                    context_chars=context_chars
                )
                
                if matches:
                    results.append({
                        'pdf_id': pdf_id,
                        'filepath': filepath,
                        'results': matches
                    })

        # Ordenar por relevancia total (suma de scores en results)
        results.sort(key=lambda x: sum(r['score'] for r in x['results']), reverse=True)
        
        return results
    
    # ======================================================================
    # GUARDADO DE PDF PROCESADO - MODIFICADO POR NICO
    # FINALIDAD: Organizar el archivo PDF resultante del OCR en la carpeta final.
    # FUNCIÓN: 
    #   1. Define la ruta de destino dentro de la carpeta 'outputs'.
    #   2. Crea el directorio si aún no existe.
    #   3. Mueve (o guarda) el PDF generado a esta ubicación definitiva.
    # DEVUELVE: El path completo (str) de dónde quedó el PDF de Nico.
    # ======================================================================
    def save_processed_pdf(self, current_ocr_path: str, pdf_id: str) -> str:
        # Definimos el nombre del archivo y la carpeta de salida
        filename = f"{pdf_id}.pdf"
        filepath = Path(settings.OUTPUTS_FOLDER) / filename
        
        # Ruta final donde queremos que viva el PDF
        # final_filepath = output_dir / filename
        
        # Aseguramos que la carpeta 'outputs' exista
        filepath.parent.mkdir(parents=True, exist_ok=True)

        # Si el archivo viene de una ruta temporal, lo movemos a la final
        import shutil
        if os.path.exists(current_ocr_path):
            shutil.move(current_ocr_path, str(filepath))
        
        logger.info(f"PDF con OCRmyPDF GUARDADO: {filepath}")
        
        return str(filepath)

    def merge_pdfs(self, pdf_paths: List[str], output_path: str, position: str = 'end') -> str:
        """
        Une una lista de PDFs en un único archivo de salida usando PyMuPDF.
        position: 'end' para anexar cada documento al final del acumulado,
                  'start' para insertarlos al inicio (comportamiento original).
        Devuelve la ruta del PDF resultante.
        """
        try:
            if not pdf_paths:
                raise ValueError("No se proporcionaron PDFs para unir")

            out_path = Path(output_path)
            out_path.parent.mkdir(parents=True, exist_ok=True)

            # Abrir el primer documento como base
            merged = fitz.open(pdf_paths[0])

            for p in pdf_paths[1:]:
                if not os.path.exists(p):
                    logger.warning(f"Archivo a unir no encontrado: {p}")
                    continue
                src = fitz.open(p)
                if position == 'end':
                    # Insertar al final
                    merged.insert_pdf(src, start_at=len(merged))
                else:
                    # Insertar al inicio (comportamiento por defecto)
                    merged.insert_pdf(src, start_at=0)
                src.close()

            merged.save(str(out_path))
            merged.close()

            logger.info(f"PDFs unidos en: {out_path}")
            return str(out_path)

        except Exception as e:
            logger.exception(f"Error al unir PDFs: {e}")
            raise