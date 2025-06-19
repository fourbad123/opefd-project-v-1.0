# This file is part of {{ cookiecutter.package_name }}.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import asyncio
import os
from datetime import datetime

import pytz
import httpx
from dotenv import load_dotenv

from influx_query import EfdQueryClient
from shutter_counter import (
    get_shutter_activations,
    load_last_activation,
    save_last_activation,
)

# Load environment variables
load_dotenv()

ASSET_ENDPOINT = os.getenv("CMMS_ASSET_ENDPOINT")
CHILE_TZ = pytz.timezone("America/Santiago")


def timestamp():
    return datetime.now(CHILE_TZ).strftime("%Y-%m-%d %H:%M:%S")


def query_influx_value(
    client_efd,
    field,
    measurement,
    interval,
    sal_index=None
):
    if sal_index is not None:
        query = (
            f'SELECT "{field}" '
            f'FROM "{measurement}" '
            f'WHERE time > now() - {interval} '
            f'AND "salIndex" = {sal_index} '
            f'ORDER BY time DESC LIMIT 1'
        )
        print(f"{timestamp()} [INFO] Executing query with salIndex={sal_index}")
    else:
        query = (
            f'SELECT "{field}" '
            f'FROM "{measurement}" '
            f'WHERE time > now() - {interval} '
            f'ORDER BY time DESC LIMIT 1'
        )
        print(f"{timestamp()} [INFO] Executing query without salIndex")

    result = client_efd.query(query)
    return result.iloc[0][field] if not result.empty else None


async def get_cmms_attribute(client, token, asset_id, attribute):
    url = f"{ASSET_ENDPOINT}/{asset_id}"
    headers = {"CMDBuild-Authorization": token}

    try:
        response = await client.get(url, headers=headers)
        response.raise_for_status()
        data = response.json().get("data", {})
        return data.get(attribute, 0)
    except Exception as e:
        print(f"{timestamp()} [ERROR CMMS] Failed to get {attribute} from {asset_id}: {e}")
        return 0


async def update_cmms_value(client, token, asset_id, attribute, value):
    url = f"{ASSET_ENDPOINT}/{asset_id}"
    headers = {
        "CMDBuild-Authorization": token,
        "Content-Type": "application/json",
    }
    payload = {attribute: int(value)}

    try:
        response = await client.put(url, json=payload, headers=headers)
        response.raise_for_status()
        print(f"{timestamp()} [CMMS] Updated {attribute}={value} for asset {asset_id}")
    except Exception as e:
        print(f"{timestamp()} [ERROR CMMS] Failed to update asset {asset_id}: {e}")


async def monitor_subsystem(token, site, db_name, cfg):
    measurement = cfg["measurement"]
    field = cfg["field"]
    asset_id = cfg["asset_id"]
    attribute = cfg["attribute"]
    interval = cfg.get("time_interval", "24h")
    client_db = cfg.get("db_name", db_name)
    client_efd = EfdQueryClient(site=site, db_name=client_db)

    last_cmms_count = None
    last_activations = None

    # Aquí creamos el cliente HTTP con verify=False para toda la sesión
    async with httpx.AsyncClient(verify=False) as client_http:
        while True:
            if (
                attribute == "AC_count"
                and measurement == "lsst.sal.MTDome.apertureShutter"
            ):
                value = get_shutter_activations(site, client_db, measurement, interval)
                cmms_current = await get_cmms_attribute(
                    client_http, token, asset_id, attribute
                )

                if last_cmms_count is None:
                    last_cmms_count = cmms_current
                    last_activations = load_last_activation(asset_id)

                    print(f"{timestamp()} [INFO] First cycle. CMMS: {cmms_current}, detected activations: {value}")

                    new_activations = value - last_activations
                    if new_activations > 0:
                        new_total = cmms_current + new_activations
                        await update_cmms_value(client_http, token, asset_id, attribute, new_total)
                        save_last_activation(asset_id, value)
                        last_cmms_count = new_total

                        print(f"{timestamp()} [INFO] CMMS updated with {new_total} (added {new_activations})")
                    else:
                        print(f"{timestamp()} [INFO] No new activations to add")
                else:
                    if value > last_activations:
                        new_activations = value - last_activations
                        new_total = last_cmms_count + new_activations

                        print(f"{timestamp()} [INFO] New activations detected: {new_activations} → total: {new_total}")

                        await update_cmms_value(client_http, token, asset_id, attribute, new_total)
                        save_last_activation(asset_id, value)
                        last_cmms_count = new_total
                        last_activations = value
                    else:
                        print(f"{timestamp()} [INFO] No new activations (current value: {value})")
            else:
                try:
                    value = query_influx_value(
                        client_efd=client_efd,
                        field=field,
                        measurement=measurement,
                        interval=interval,
                        sal_index=cfg.get("salIndex")
                    )
                    print(f"{timestamp()} [INFO] {measurement}.{field} = {value}")
                    await update_cmms_value(client_http, token, asset_id, attribute, value)
                except Exception as e:
                    print(f"{timestamp()} [ERROR EFD] {e}")

            await asyncio.sleep(60)

