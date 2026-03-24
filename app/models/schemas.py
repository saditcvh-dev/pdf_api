from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime


from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime


# ===============================
# RESPUESTA AL SUBIR PDF (QUEUE)
# ===============================
class PDFUploadResponse(BaseModel):
    id: str
    filename: str
    size: int
    task_id: str  # ID de la tarea en Celery
    status: str  # "pending", "processing", "completed", "failed"
    message: str
    pages: Optional[int] = None #  número de páginas
    estimated_wait_time: Optional[float] = None  # segundos estimados


# ===============================
# ESTADO DEL PROCESAMIENTO
# ===============================
class PDFUploadStatus(BaseModel):
    pdf_id: str
    task_id: str
    status: str  # "pending", "processing", "completed", "failed"
    pages: Optional[int] = None
    extracted_text_path: Optional[str] = None
    used_ocr: Optional[bool] = None
    progress: Optional[int] = None  # 0-100
    error: Optional[str] = None
    created_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


# ===============================
# PETICIÓN DE BÚSQUEDA
# ===============================
class SearchRequest(BaseModel):
    term: str
    case_sensitive: bool = False
    use_regex: bool = False


# ===============================
# RESULTADO INDIVIDUAL
# ===============================
class SearchResult(BaseModel):
    page: int
    position: int
    context: str
    snippet: str


# ===============================
# RESPUESTA DE BÚSQUEDA
# ===============================
class SearchResponse(BaseModel):
    term: str
    total_matches: int
    results: List[SearchResult]
    pdf_id: str
    execution_time: float


# ===============================
# INFORMACIÓN GENERAL DEL PDF
# ===============================
class PDFInfo(BaseModel):
    id: str
    filename: str
    upload_date: datetime
    size: int
    pages: int
    has_text: bool
    text_file_size: Optional[int] = None


# ===============================
# 🔥 ANALYSIS (EVITA TU ERROR)
# ===============================
class PDFAnalysis(BaseModel):
    pdf_id: str
    pages: int
    used_ocr: bool
    extracted_text_path: Optional[str] = None


# ===============================
# ITEM INDIVIDUAL EN LISTA DE PDFs
# ===============================
class PDFListItem(BaseModel):
    id: str
    filename: str
    size_bytes: int
    size_mb: float
    status: str  # "completed", "processing", "pending", "failed"
    progress: int  # 0-100
    pages: Optional[int] = None
    task_id: str
    upload_time: float
    created_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    extracted_text_path: Optional[str] = None
    used_ocr: bool
    error: Optional[str] = None


# ===============================
# RESPUESTA AGRUPADA DE LISTA DE PDFs
# ===============================
class PDFListResponse(BaseModel):
    total: int
    page: int = 1
    limit: int = 50
    total_pages: int = 1
    by_status: dict  # {"completed": 5, "processing": 2, "pending": 1, "failed": 0}
    pdfs: List[PDFListItem]
    summary: dict  # Contiene las listas agrupadas por estado
