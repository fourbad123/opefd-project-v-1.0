# triggers.py

from utils import log

def is_trigger_met(noise_level, config):
    """
    Evalúa si el valor de `noise_level` cumple con el trigger definido en la configuración.
    
    Los posibles tipos de triggers son:
    - trigger_integer: compara como número (float)
    - trigger_string: compara como string exacto
    - trigger_time: compara como string (formato hora)
    - trigger_True_False: compara como booleano

    Retorna True si el trigger se cumple, de lo contrario False.
    """
    try:
        if config.get("trigger_integer") is not None:
            return float(noise_level) == float(config["trigger_integer"])
        if config.get("trigger_string") is not None:
            return str(noise_level) == config["trigger_string"]
        if config.get("trigger_time") is not None:
            return str(noise_level) == config["trigger_time"]
        if config.get("trigger_True_False") is not None:
            return bool(noise_level) == config["trigger_True_False"]
    except Exception as e:
        log(f"Error evaluando trigger para config ID {config.get('_id')}: {e}", level="WARN")
        return False

    return False
