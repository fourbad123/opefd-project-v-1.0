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

import json
import os
from datetime import datetime

import pytz
import pandas as pd

from influx_query import EfdQueryClient

CHILE_TZ = pytz.timezone("America/Santiago")


def get_shutter_activations(site: str, db_name: str, measurement: str, time_interval: str = "24h") -> int:
    """
    Count the number of shutter activations
    where positionActual0 or positionActual1 > 90
    within a given time interval from InfluxDB.

    Args:
        site (str): Observatory site identifier.
        db_name (str): Name of the InfluxDB database.
        measurement (str): Measurement name in InfluxDB.
        time_interval (str): Time interval to query (e.g. "24h").

    Returns:
        int: Number of detected shutter activations.
    """
    try:
        client = EfdQueryClient(site=site, db_name=db_name)

        query = (
            f'SELECT "positionActual0", "positionActual1" '
            f'FROM "{measurement}" '
            f'WHERE time > now() - {time_interval} '
            f'ORDER BY time ASC'
        )

        result: pd.DataFrame = client.query(query)

        if result.empty:
            print(f"[DEBUG] No shutter data in the last {time_interval}.")
            return 0

        # Activation condition: either positionActual0 or positionActual1 > 90
        activity = (result["positionActual0"] > 90) | (result["positionActual1"] > 90)
        activity = activity.astype(bool)

        # Count rising edges (from False to True)
        activations = activity & (~activity.shift(1).fillna(False))
        count_activations = int(activations.sum())

        print(f"[DEBUG] Shutter activations >90% in last {time_interval}: {count_activations}")
        return count_activations

    except Exception as e:
        print(f"[ERROR SHUTTER] Failed to query or process shutter data: {e}")
        return 0


def load_last_activation(asset_id: str) -> int:
    """
    Load the last saved shutter activation count
    for a given asset from a local JSON file.

    Args:
        asset_id (str): Asset identifier.

    Returns:
        int: Last recorded activation count,
        or 0 if not found or failed to read.
    """
    path = f"activations_{asset_id}.json"
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                data = json.load(f)
                last_activations = data.get("last_activations", 0)
                if isinstance(last_activations, dict):
                    last_activations=last_activations.get("value",0)if isinstance(last_activations,dict)else 0
                return int(last_activations)
            print("Last shutter activations in 24h: " + last_activations)
        except Exception as e:
            print(f"[ERROR LOAD] Could not read activation file '{path}': {e}")
            return 0
    return 0


def save_last_activation(asset_id: str, count: int) -> None:
    """
    Save the latest shutter activation count and timestamp for a given asset.

    Args:
        asset_id (str): Asset identifier.
        count (int): Current number of shutter activations.
    """
    path = f"activations_{asset_id}.json"
    data = {
        "last_activations": int(count),
        "last_update": datetime.now(CHILE_TZ).strftime("%Y-%m-%dT%H:%M:%S")
    }

    try:
        with open(path, "w") as f:
            json.dump(data, f, indent=4)
    except Exception as e:
        print(f"[ERROR SAVE] Could not write activation file '{path}': {e}")
