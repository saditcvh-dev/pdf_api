# app/core/state.py
from typing import Dict, Any, Optional
from datetime import datetime
import logging
import json
from redis import Redis
from app.core.config import settings

logger = logging.getLogger(__name__)

# Reutilizando la URL que ya tienes para Celery
redis_client = Redis.from_url(settings.CELERY_BROKER_URL, decode_responses=True)

class DatetimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

def datetime_decoder(d):
    for k, v in d.items():
        if isinstance(v, str):
            try:
                # Intenta restaurar ISO format strings a datetime
                if 'T' in v and len(v) >= 19:
                    d[k] = datetime.fromisoformat(v)
            except ValueError:
                pass
    return d

class RedisHashDict:
    """Proxy apoyado en Redis (para no perder variables al reiniciar servidor) y compartirlas con Celery."""
    def __init__(self, hash_name: str):
        self.hash_name = hash_name

    def __contains__(self, key: str) -> bool:
        return redis_client.hexists(self.hash_name, key)

    def __getitem__(self, key: str) -> Dict[str, Any]:
        val = redis_client.hget(self.hash_name, key)
        if val is None:
            raise KeyError(key)
        return json.loads(val, object_hook=datetime_decoder)

    def __setitem__(self, key: str, value: Dict[str, Any]):
        redis_client.hset(self.hash_name, key, json.dumps(value, cls=DatetimeEncoder))

    def get(self, key: str, default: Any = None) -> Any:
        val = redis_client.hget(self.hash_name, key)
        if val is None:
            return default
        return json.loads(val, object_hook=datetime_decoder)

    def update_key(self, key: str, update_dict: Dict[str, Any]):
        """
        Convenience function.
        En lugar de usar `pdf_storage[id].update(...)`, ahora usarás `pdf_storage.update_key(id, {...})`
        """
        current = self.get(key, {})
        current.update(update_dict)
        self[key] = current

    def __len__(self) -> int:
        return redis_client.hlen(self.hash_name)

    def items(self):
        all_data = redis_client.hgetall(self.hash_name)
        return [(k, json.loads(v, object_hook=datetime_decoder)) for k, v in all_data.items()]

    def keys(self):
        return redis_client.hkeys(self.hash_name)

    def values(self):
        all_data = redis_client.hgetall(self.hash_name)
        return [json.loads(v, object_hook=datetime_decoder) for v in all_data.values()]

    def clear(self):
        redis_client.delete(self.hash_name)

    def pop(self, key: str, default: Any = None) -> Any:
        v = self.get(key, default)
        redis_client.hdel(self.hash_name, key)
        return v

# Instancias Redis super puestas sobre el código
pdf_storage = RedisHashDict("pdf_storage")
pdf_task_status = RedisHashDict("pdf_task_status")

def reset_state():
    pdf_storage.clear()
    pdf_task_status.clear()
    logger.info("Estado global de PDFs reiniciado y limpiado de Redis")
