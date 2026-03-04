from databases import Database
from app.core.config import settings
from urllib.parse import urlparse, quote_plus, urlunparse

db = None
if hasattr(settings, "DATABASE_URL") and settings.DATABASE_URL:
    url = settings.DATABASE_URL
    # La contraseña puede contener #, por lo que urlparse no sirve directamente
    # Formato: postgresql://user:password@host:port/dbname
    
    # 1. Separar scheme "postgresql://" del resto
    if "://" in url:
        scheme, rest = url.split("://", 1)
        
        # 2. Separar credenciales de host/db usando el último '@'
        if "@" in rest:
            creds, host_db = rest.rsplit("@", 1)
            
            # 3. Separar usuario y contraseña usando el primer ':'
            if ":" in creds:
                user, password = creds.split(":", 1)
                
                # 4. Codificar la contraseña de forma segura
                encoded_password = quote_plus(password)
                
                # 5. Reconstruir URL
                safe_url = f"{scheme}://{user}:{encoded_password}@{host_db}"
                db = Database(safe_url)
            else:
                db = Database(url)
        else:
            db = Database(url)
    else:
        db = Database(url)
