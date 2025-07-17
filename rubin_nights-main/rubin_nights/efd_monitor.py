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

# Load environment variables from .env file
load_dotenv()

ASSET_ENDPOINT = os.getenv("CMMS_ASSET_ENDPOINT")
CHILE_TZ = pytz.timezone("America/Santiago")

LOG_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def current_timestamp() -> str:
    """Return the current timestamp formatted with Chile timezone."""
    return datetime.now(CHILE_TZ).strftime(LOG_TIME_FORMAT)


def query_latest_influx_value(
    client_efd: EfdQueryClient,
    measurement: str,
    field: str,
    interval: str,
    sal_index: int | None = None
) -> float | None:
    """
    Query the latest value from InfluxDB.

    Args:
        client_efd: Instance of EfdQueryClient.
        measurement: Measurement name in InfluxDB.
        field: Field to query.
        interval: Time interval string for query.
        sal_index: Optional SAL index filter.

    Returns:
        The latest float value or None if not found.
    """
    if sal_index is not None:
        query = (
            f'SELECT "{field}" FROM "{measurement}" '
            f'WHERE time > now() - {interval} AND "salIndex" = {sal_index} '
            f'ORDER BY time DESC LIMIT 1'
        )
        print(f"{current_timestamp()} [INFO] Executing InfluxDB query with salIndex={sal_index}")
    else:
        query = (
            f'SELECT "{field}" FROM "{measurement}" '
            f'WHERE time > now() - {interval} '
            f'ORDER BY time DESC LIMIT 1'
        )
        print(f"{current_timestamp()} [INFO] Executing InfluxDB query without salIndex")

    result = client_efd.query(query)
    if not result.empty:
        return result.iloc[0][field]
    return None


async def get_cmms_attribute(
    client: httpx.AsyncClient,
    token: str,
    asset_id: str | int,
    attribute: str
) -> int | float | None:
    """
    Fetch the current attribute value from CMMS asset.

    Args:
        client: httpx AsyncClient instance.
        token: Authentication token.
        asset_id: Asset ID in CMMS.
        attribute: Attribute name to fetch.

    Returns:
        Attribute value or None on error.
    """
    url = f"{ASSET_ENDPOINT}/{asset_id}"
    headers = {"CMDBuild-Authorization": token}

    try:
        response = await client.get(url, headers=headers)
        response.raise_for_status()
        data = response.json().get("data", {})
        return data.get(attribute, None)
    except Exception as err:
        print(f"{current_timestamp()} [ERROR CMMS] Failed to get '{attribute}' from asset {asset_id}: {err}")
        return None


async def update_cmms_attribute(
    client: httpx.AsyncClient,
    token: str,
    asset_id: str | int,
    attribute: str,
    value: int | float
) -> None:
    """
    Update attribute value on CMMS asset.

    Args:
        client: httpx AsyncClient instance.
        token: Authentication token.
        asset_id: Asset ID in CMMS.
        attribute: Attribute name to update.
        value: New value to set.
    """
    url = f"{ASSET_ENDPOINT}/{asset_id}"
    headers = {
        "CMDBuild-Authorization": token,
        "Content-Type": "application/json",
    }
    payload = {attribute: int(value)}

    try:
        response = await client.put(url, headers=headers, json=payload)
        response.raise_for_status()
        print(f"{current_timestamp()} [CMMS] Updated {attribute}={value} for asset {asset_id}")
    except Exception as err:
        print(f"{current_timestamp()} [ERROR CMMS] Failed to update asset {asset_id}: {err}")


async def monitor_subsystem(
    token: str,
    site: str,
    db_name: str,
    config: dict
) -> None:
    """
    Main async loop to monitor a subsystem and update CMMS accordingly.

    Args:
        token: CMMS authentication token.
        site: Site name or identifier.
        db_name: Database name for EFD queries.
        config: Configuration dictionary with keys:
            - measurement
            - field
            - asset_id
            - attribute
            - time_interval (optional)
            - salIndex (optional)
    """
    measurement = config["measurement"]
    field = config["field"]
    asset_id = config["asset_id"]
    attribute = config["attribute"]
    interval = config.get("time_interval", "24h")
    influx_db_name = config.get("db_name", db_name)
    client_efd = EfdQueryClient(site=site, db_name=influx_db_name)

    last_cmms_value = None
    last_activation_count = None

    async with httpx.AsyncClient(verify=False) as http_client:
        while True:
            if attribute == "AC_count" and measurement == "lsst.sal.MTDome.apertureShutter":
                activation_count = get_shutter_activations(site, influx_db_name, measurement, interval)
                cmms_value = await get_cmms_attribute(http_client, token, asset_id, attribute)

                # Aquí la corrección para evitar sumas con None
                if cmms_value is None:
                    cmms_value = 0

                if last_cmms_value is None:
                    last_cmms_value = cmms_value
                    last_activation_count = load_last_activation(asset_id)

                    print(f"{current_timestamp()}CMMS value={cmms_value}shutter act.={activation_count}")

                    new_activations = activation_count - last_activation_count
                    if new_activations > 0:
                        updated_value = cmms_value + new_activations
                        await update_cmms_attribute(http_client, token, asset_id, attribute, updated_value)
                        save_last_activation(asset_id, activation_count)
                        last_cmms_value = updated_value
                        print(f"{current_timestamp()}updated to {updated_value} (added {new_activations})")
                    else:
                        print(f"{current_timestamp()} [INFO] No new shutter activations detected.")
                else:
                    if activation_count > last_activation_count:
                        new_activations = activation_count - last_activation_count
                        updated_value = last_cmms_value + new_activations
                        print(
                            f"{current_timestamp()}{new_activations}new activations,updating{updated_value}"
                            )

                        await update_cmms_attribute(http_client, token, asset_id, attribute, updated_value)
                        save_last_activation(asset_id, activation_count)
                        last_cmms_value = updated_value
                        last_activation_count = activation_count
                    else:
                        print(f"{current_timestamp()} No new activations, current count: {activation_count})")

            else:
                try:
                    value = query_latest_influx_value(
                        client_efd,
                        measurement,
                        field,
                        interval,
                        sal_index=config.get("salIndex")
                    )
                    if value is not None:
                        print(f"{current_timestamp()} [INFO] {measurement}.{field} = {value}")
                        await update_cmms_attribute(http_client, token, asset_id, attribute, value)
                    else:
                        print(f"{current_timestamp()} [WARN] No value returned for {measurement}.{field}")
                except Exception as err:
                    print(f"{current_timestamp()} [ERROR EFD] Query failed: {err}")

            await asyncio.sleep(60)
