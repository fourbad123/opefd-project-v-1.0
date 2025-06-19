import requests
from utils import log, get_current_time_chile

BASE_URL = "https://cmms.cp.lsst.org/openmaint"

def get_prev_maint_configs(token):
    headers = {"CMDBuild-Authorization": token}
    url = f"{BASE_URL}/services/rest/v3/classes/PrevMaintConfig/cards"
    try:
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        return r.json().get("data", [])
    except Exception as e:
        log(f"No se pudieron obtener las configuraciones de mantenimiento: {e}", level="ERROR")
        return []

def get_prev_maint_config_by_id(token, config_id):
    headers = {"CMDBuild-Authorization": token}
    url = f"{BASE_URL}/services/rest/v3/classes/PrevMaintConfig/cards/{config_id}"
    try:
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        return r.json().get("data", {})
    except Exception as e:
        log(f"No se pudo obtener PrevMaintConfig {config_id}: {e}", level="ERROR")
        return {}

def get_noise_level(token, asset_id):
    headers = {"CMDBuild-Authorization": token}
    url = f"{BASE_URL}/services/rest/v3/classes/Asset/cards/{asset_id}"
    try:
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        return r.json()["data"].get("NoiseLevel")
    except Exception as e:
        log(f"[CMMS] ❌ Error al obtener NoiseLevel del asset {asset_id}: {e}", level="ERROR")
        return None

def create_pm_instance(token, config_data):
    headers = {
        "CMDBuild-Authorization": token,
        "Content-Type": "application/json"
    }

    body = {
        "maintConf": config_data["_id"],
        "PrevMaintConfig": config_data["_id"],
        "ShortDescr": config_data.get("Description"),
        "Site": config_data.get("Site"),
        "Action": config_data.get("Action"),
        "CISubset": config_data.get("CISubset"),
        "Team": config_data.get("Team"),
        "Priority": config_data.get("Priority"),
        "EstimatedDuration": config_data.get("EstimatedDuration"),
        "Notes": config_data.get("Notes"),
        "ActivityType": config_data.get("ActivityType")
    }

    url = f"{BASE_URL}/services/rest/v3/processes/PreventiveMaint/instances"
    try:
        r = requests.post(url, headers=headers, json=body)
        r.raise_for_status()
        pm_id = r.json().get("data", {}).get("_id")
        log(f"[PM] ✅ PM creada correctamente con ID {pm_id}")
        return pm_id
    except Exception as e:
        log(f"[PM] ❌ Error al crear PM: {e}", level="ERROR")
        return None

def advance_pm_to_execution(token, pm_id):
    headers = {
        "CMDBuild-Authorization": token,
        "Content-Type": "application/json"
    }

    url_activities = f"{BASE_URL}/services/rest/v3/processes/PreventiveMaint/instances/{pm_id}/activities"
    try:
        r = requests.get(url_activities, headers=headers)
        r.raise_for_status()
        activities = r.json().get("data", [])
        if not activities:
            log(f"[PM] ❌ No hay actividades disponibles para PM {pm_id}", level="ERROR")
            return False
        activity_id = activities[0]["_id"]
    except Exception as e:
        log(f"[PM] ❌ Error al obtener actividades de la PM {pm_id}: {e}", level="ERROR")
        return False

    url_advance = f"{BASE_URL}/services/rest/v3/processes/PreventiveMaint/instances/{pm_id}"
    payload = {
        "_activity": activity_id,
        "_type": "PreventiveMaint",
        "_advance": True,
        "status": "acceptance",
        "execution_date": get_current_time_chile().split("T")[0]
    }

    try:
        r = requests.put(url_advance, headers=headers, json=payload)
        r.raise_for_status()
        log(f"[PM] ✅ PM {pm_id} avanzó directamente a 'Execution'")
        return True
    except Exception as e:
        log(f"[PM] ❌ Error al avanzar PM {pm_id} a 'Execution': {e}", level="ERROR")
        return False
