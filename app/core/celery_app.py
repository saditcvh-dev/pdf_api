from celery import Celery
from app.core.config import settings

# Crear instancia de Celery
celery_app = Celery(
    "pdf_api",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
)

# Configuración de Celery
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_time_limit=2 * 60 * 60 + 60,  # 2 horas + 1 min
    task_soft_time_limit=2 * 60 * 60,  # 2 horas
    broker_connection_retry_on_startup=True,
    broker_connection_retry=True,
    worker_prefetch_multiplier=1,  # Un task a la vez por worker
)

# Autodiscover tasks
celery_app.autodiscover_tasks(['app.tasks'])
