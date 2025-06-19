import time
import logging
import requests
from influx_query import EfdQueryClient
BASE_URL = "https://cmms.cp.lsst.org/openmaint"
AUTH_URL = f"{BASE_URL}/services/rest/v3/sessions?scope=service&returnId=true"
credentials = {
    "username": "admin",
    "password": "admin"
}


def get_efd_value(site: str, db_name: str, measurement: str):
    """Consulta el último valor de una medición en el EFD."""
    try:
        client = EfdQueryClient(site=site, db_name=db_name)
        query = f'SELECT "analog_I" FROM "efd"."autogen"."lsst.sal.ATCamera.power" '
        result = client.query(query)

        print(f"Consulta EFD: {query}")  # Debug
        print(f"Resultado EFD: {result}")  # Debug

        if result.empty:
            logging.warning("Consulta EFD vacía.")
            return None

        last_value = result.iloc[-1]["analog_I"]  # 🔹 Obtiene el último valor de la columna "substate"
        print(f"Último valor obtenido: {last_value}")  # Debug
        return last_value
    except Exception as e:
        logging.error(f"Error consultando EFD: {e}")
        return None


def get_token():
    headers = {"Content-Type": "application/json"}
    response = requests.post(AUTH_URL, headers=headers, json=credentials)

    if response.status_code == 200:
        token = response.json().get("data", {}).get("_id")
        if token:
            print(f"Token obtenido: {token}")  # Asegúrate de que el token esté llegando correctamente
            return token
    print(f"Error al obtener el token: {response.status_code} - {response.text}")
    return None

def update_cmms_noiselevel(token: str, motor_id: str, noise_level: str):
    """Envía el valor actualizado al CMMS."""
    url = f"https://cmms.cp.lsst.org/openmaint/services/rest/v3/classes/Asset/cards/463107"
    headers = {"CMDBuild-Authorization": token, "Content-Type": "application/json"}
    payload = {"NoiseLevel": int(noise_level)}
    print(f"Enviando a CMMS: {payload}")  # Debug
    try:
        response = requests.put(url, json=payload, headers=headers)
        response.raise_for_status()
        logging.info(f"Actualizado NoiseLevel a {noise_level}")
        print(f"Respuesta CMMS: {response.status_code} - {response.text}")  # Debug
    except Exception as e:
        logging.error(f"Error enviando datos a OpenMaint: {e}")


def main():
    site = "summit"
    db_name = "efd"
    measurement = "lsst.sal.MTMount.compressedAir"
    motor_id = "409980"  # Debug: ID del motor

    token = get_token()
    if not token:
        logging.error("No se pudo obtener el token de OpenMaint. Saliendo...")
        return

    while True:
        voltage = get_efd_value(site, db_name, measurement)
        print(f"Voltaje obtenido: {voltage}")  # Debug
        if voltage is not None:
            update_cmms_noiselevel(token, motor_id, voltage)
        time.sleep(5)  # Consulta cada 5 segundos


if __name__ == "__main__":
    main()