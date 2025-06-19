# utils.py

from datetime import datetime
import pytz

# Zona horaria de Chile
CHILE_TZ = pytz.timezone("America/Santiago")

def get_current_time_chile():
    """
    Retorna la hora actual en la zona horaria de Chile, 
    en formato ISO 8601 con sufijo Z.
    """
    return datetime.now(CHILE_TZ).strftime("%Y-%m-%dT%H:%M:%SZ")

def iso_now_utc():
    """
    Retorna la hora actual en formato ISO 8601 UTC con sufijo Z.
    """
    return f"{datetime.utcnow().isoformat()}Z"

def safe_get(d, key, default=None):
    """
    Obtiene el valor del diccionario `d` para la clave `key`.
    Si no existe o es None, retorna `default`.
    """
    return d.get(key) if d.get(key) is not None else default

def log(msg, level="INFO"):
    """
    Imprime un mensaje con timestamp UTC y nivel de log.
    Niveles: INFO, DEBUG, WARN, ERROR
    """
    timestamp = iso_now_utc()
    print(f"[{level}] {timestamp} {msg}")
