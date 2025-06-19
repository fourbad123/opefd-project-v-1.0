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

from influx_query import EfdQueryClient

CHILE_TZ = pytz.timezone("America/Santiago")


def get_shutter_activations(site, db_name, measurement, time_interval="24h"):
    """
    Queries InfluxDB to get the number of shutter activations
    in the specified time interval.
    """
    try:
        client = EfdQueryClient(site=site, db_name=db_name)
        query = f'''
            SELECT "positionActual0", "positionActual1"
            FROM "{measurement}"
            WHERE time > now() - {time_interval}
            ORDER BY time ASC
        '''
        result = client.query(query)

        if result.empty:
            print("[DEBUG] No shutter data found in the interval.")
            return 0

        activity = (result["positionActual0"] > 90) | \
                   (result["positionActual1"] > 90)
        activity = activity.infer_objects(copy=False).astype(bool)

        activations = activity.astype(bool) & (~activity.shift(1).fillna(False).astype(bool))

        count_activations = activations.sum()

        print(
            f"[DEBUG] Shutter activations >90% in the last {time_interval}: "
            f"{count_activations}"
        )
        return count_activations

    except Exception as e:
        print(f"[ERROR SHUTTER] {e}")
        return 0


def load_last_activation(asset_id):
    """
    Loads the previous number of activations from a JSON file.
    """
    path = f"activations_{asset_id}.json"
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                data = json.load(f)
                return data.get("last_activations", 0)
        except Exception as e:
            print(f"[ERROR LOAD] Failed to read activation file {path}: {e}")
            return 0
    return 0


def save_last_activation(asset_id, count):
    """
    Saves the current number of activations to a JSON file.
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
        print(f"[ERROR SAVE] Failed to write activation file {path}: {e}")
