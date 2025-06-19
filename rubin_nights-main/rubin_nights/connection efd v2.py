import asyncio
import logging
import requests
from influx_query import EfdQueryClient
from sistems import config_with_interval

BASE_URL = "https://cmms.cp.lsst.org/openmaint"
AUTH_URL = f"{BASE_URL}/services/rest/v3/sessions?scope=service&returnId=true"

credentials = {
    "username": "admin",
    "password": "admin"
}

def get_token():
    headers = {"Content-Type": "application/json"}
    response = requests.post(AUTH_URL, headers=headers, json=credentials)

    if response.status_code == 200:
        token = response.json().get("data", {}).get("_id")
        if token:
            print(f"Token obtenido: {token}")
            return token
    print(f"Error al obtener el token: {response.status_code} - {response.text}")
    return None


async def get_efd_value(site: str, db_name: str, measurement: str, field: str, time_interval: str = None):
    try:
        client = EfdQueryClient(site=site, db_name=db_name)

        if measurement == "lsst.sal.ATCamera.logevent_shutterDetailedState":
            query = f'SELECT count("{field}") AS "count" FROM "efd"."autogen"."{measurement}" WHERE "{field}" = \'open\' AND time > now() - {time_interval}'
        else:
            query = f'SELECT "{field}" FROM "efd"."autogen"."{measurement}" WHERE time > now() - {time_interval}'

        print(f"Consulta EFD: {query}")
        result = client.query(query)

        if result.empty:
            logging.warning(f"Consulta vacía para {measurement}")
            return None

        #Elegir valor según tipo de trigger
        return result.iloc[-1][field] if "count" not in result.columns else result.iloc[-1]["count"]

    except Exception as e:
        logging.error(f"Error consultando EFD ({measurement}): {e}")
        return None


async def update_cmms_value(token: str, asset_id: str, attribute: str, value):
    url = f"{BASE_URL}/services/rest/v3/classes/Asset/cards/{asset_id}"
    headers = {
        "CMDBuild-Authorization": token,
        "Content-Type": "application/json"
    }

    try:
        payload = {attribute: value if isinstance(value, int) else str(value)}
        print(f"Enviando a CMMS (asset {asset_id}): {payload}")
        response = requests.put(url, json=payload, headers=headers)
        response.raise_for_status()
        logging.info(f"Actualizado {attribute} de asset {asset_id} con {value}")
    except Exception as e:
        logging.error(f"Error actualizando asset {asset_id}: {e}")


#Activar PM
def avanzar_pm_en_openmaint(token: str, asset_id: str):
    try:
        url = f"{BASE_URL}/services/rest/v3/classes/WorkOrder"
        headers = {"CMDBuild-Authorization": token}

        response = requests.get(url, headers=headers, params={"filter": f"(IdClassAsset={asset_id}) AND (CurrentStatus='PM-Acceptance')"})
        response.raise_for_status()

        data = response.json().get("data", [])
        for pm in data:
            pm_id = pm.get("Id")
            if pm_id:
                print(f"Activando PM {pm_id} para asset {asset_id}")
                exec_url = f"{BASE_URL}/services/rest/v3/classes/WorkOrder/{pm_id}/actions/execute"
                exec_response = requests.post(exec_url, headers=headers)
                exec_response.raise_for_status()
                logging.info(f"PM {pm_id} activado correctamente para asset {asset_id}")

    except Exception as e:
        logging.error(f"Error al activar PM para asset {asset_id}: {e}")


# Verifica condiciones de trigger
async def check_pm_trigger(token, cfg, value):
    trigger_type = cfg.get("trigger_type")

    if trigger_type == "umbral":
        threshold = cfg.get("threshold")
        if threshold is not None and value >= threshold:
            print(f"[Trigger UMBRAL] Valor {value} >= {threshold} → Ejecutando PM")
            avanzar_pm_en_openmaint(token, cfg["asset_id"])

    elif trigger_type == "contador":
        limite = cfg.get("limite_activaciones")
        if limite is not None and value >= limite:
            print(f"[Trigger CONTADOR] Activaciones {value} >= {limite} → Ejecutando PM")
            avanzar_pm_en_openmaint(token, cfg["asset_id"])


async def monitor_subsystem(token: str, site: str, db_name: str, cfg: dict):
    measurement = cfg["measurement"]
    field = cfg["field"]
    asset_id = cfg["asset_id"]
    attribute = cfg["attribute"]
    time_interval = cfg.get("time_interval", "30d")  # default 30 días

    while True:
        value = await get_efd_value(site, db_name, measurement, field, time_interval)
        if value is not None:
            await update_cmms_value(token, asset_id, attribute, value)
            await check_pm_trigger(token, cfg, value)
        await asyncio.sleep(5)


async def main():
    site = "summit"
    db_name = "efd"
    token = get_token()

    if not token:
        logging.error("No se pudo obtener el token. Abortando.")
        return

    tasks = []
    for cfg in config_with_interval:
        tasks.append(monitor_subsystem(token, site, db_name, cfg))

    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())