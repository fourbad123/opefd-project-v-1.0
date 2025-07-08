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

import httpx
import pytz
from dotenv import load_dotenv

from sistems import config_with_interval  # Local import

# Load environment variables from .env
load_dotenv()

CHILE_TZ = pytz.timezone("America/Santiago")


def get_current_time_chile():
    return datetime.now(CHILE_TZ).strftime("%Y-%m-%dT%H:%M:%SZ")


# Load full URLs from .env
URL_PREV_MAINT_CONFIG_CARDS = os.getenv("URL_PREV_MAINT_CONFIG_CARDS")
URL_PREV_MAINT_CONFIG_CARD = os.getenv("URL_PREV_MAINT_CONFIG_CARD")
URL_ASSET_CARD = os.getenv("URL_ASSET_CARD")
URL_CREATE_PM_INSTANCE = os.getenv("URL_CREATE_PM_INSTANCE")
URL_ACTIVITIES = os.getenv("URL_ACTIVITIES")
URL_ADVANCE = os.getenv("URL_ADVANCE")


async def get_prev_maint_configs(client, token):
    headers = {"CMDBuild-Authorization": token}
    try:
        response = await client.get(
            URL_PREV_MAINT_CONFIG_CARDS,
            headers=headers,
            timeout=10
        )
        response.raise_for_status()
        return response.json().get("data", [])
    except Exception as e:
        print(f"[ERROR] Failed to fetch maintenance configurations: {e}")
        return []


async def get_prev_maint_config_by_id(client, token, config_id):
    headers = {"CMDBuild-Authorization": token}
    url = URL_PREV_MAINT_CONFIG_CARD.format(config_id=config_id)
    try:
        response = await client.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json().get("data", {})
    except Exception as e:
        print(f"[ERROR] Failed to fetch config {config_id}: {e}")
        return {}


async def get_attribute(client, token, asset_id, attribute):
    headers = {"CMDBuild-Authorization": token}
    url = URL_ASSET_CARD.format(asset_id=asset_id)
    try:
        response = await client.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()["data"].get(attribute)
    except Exception as e:
        print(f"[CMMS ERROR] Failed to get '{attribute}' from asset {asset_id}: {e}")
        return None


def is_trigger_met(noise_level, config):
    try:
        trigger_fields = [
            ("trigger_integer", float),
            ("trigger_string", str),
            ("trigger_time", str),
            ("trigger_True_False", lambda x: str(x).lower() in ("true", "1")),
        ]

        for field, cast_fn in trigger_fields:
            trigger_value = config.get(field)
            if trigger_value is not None:
                if noise_level is None:
                    print(f"[WARN] Value is None for config {config.get('_id')}")
                    return False
                try:
                    noise_casted = cast_fn(noise_level)
                    trigger_casted = cast_fn(trigger_value)
                    return noise_casted == trigger_casted
                except (ValueError, TypeError) as e:
                    print(f"[WARN] Casting failed for {field}: {e}")
                    return False

        print(f"[WARN] No trigger found in config {config.get('_id')}")
        return False

    except Exception as e:
        print(f"[ERROR] Failed to evaluate trigger: {e}")
        return False


async def check_existing_active_pm(client, token, config_id):
    headers = {
        "CMDBuild-Authorization": token,
        "Content-Type": "application/json",
    }
    try:
        response = await client.get(
            URL_CREATE_PM_INSTANCE,
            headers=headers,
            timeout=10
        )
        response.raise_for_status()
        for pm in response.json().get("data", []):
            if pm.get("PrevMaintConfig") == config_id:
                status = pm.get("_status_description", "").lower()
                if status != "aborted":
                    return True, pm.get("_id")
        return False, None
    except Exception as e:
        print(f"[WARN] Failed to check existing PM: {e}")
        return False, None


async def create_pm(client, token, config_id):
    headers = {
        "CMDBuild-Authorization": token,
        "Content-Type": "application/json",
    }
    config_data = await get_prev_maint_config_by_id(client, token, config_id)
    if not config_data:
        return None

    body = {
        "maintConf": config_id,
        "PrevMaintConfig": config_id,
        "ShortDescr": config_data.get("Description"),
        "Site": config_data.get("Site"),
        "Action": config_data.get("Action"),
        "CISubset": config_data.get("CISubset"),
        "Team": config_data.get("Team"),
        "Priority": config_data.get("Priority"),
        "EstimatedDuration": config_data.get("EstimatedDuration"),
        "Notes": config_data.get("Notes"),
        "ActivityType": config_data.get("ActivityType"),
    }

    try:
        response = await client.post(
            URL_CREATE_PM_INSTANCE,
            headers=headers,
            json=body,
            timeout=10
        )
        response.raise_for_status()
        return response.json().get("data", {}).get("_id")
    except Exception as e:
        print(f"[PM ERROR] Failed to create PM: {e}")
        return None


async def advance_pm(client, token, pm_id):
    headers = {
        "CMDBuild-Authorization": token,
        "Content-Type": "application/json",
    }

    url_activities = URL_ACTIVITIES.format(pm_id=pm_id)
    try:
        response = await client.get(url_activities, headers=headers, timeout=10)
        response.raise_for_status()
        activity_id = response.json()["data"][0]["_id"]
    except Exception as e:
        print(f"[PM ERROR] Failed to fetch activities: {e}")
        return False

    url_advance = URL_ADVANCE.format(pm_id=pm_id)
    payload = {
        "_activity": activity_id,
        "_type": "PreventiveMaint",
        "_advance": True,
        "status": "acceptance",
        "execution_date": datetime.now(CHILE_TZ).strftime("%Y-%m-%d"),
    }

    try:
        response = await client.put(url_advance, headers=headers, json=payload, timeout=10)
        response.raise_for_status()
        return True
    except Exception as e:
        print(f"[PM ERROR] Failed to advance PM: {e}")
        return False


async def maintenance_loop(token):
    async with httpx.AsyncClient(verify=False) as client:
        while True:
            configs = await get_prev_maint_configs(client, token)
            print(f"[INFO] {len(configs)} configurations fetched")

            for config in configs:
                config_id = config.get("_id")
                asset_id = config.get("Asset_related")  

                if not asset_id:
                    print(f"[WARN] Configuration missing 'Asset_related' field: ID {config_id}")
                    continue

                system_config = next(
                    (c for c in config_with_interval if c["asset_id"] == str(asset_id)),
                    None
                )
                if not system_config:
                    print(f"[WARN] No configuration found for asset {asset_id}")
                    continue

                attribute = system_config["attribute"]
                asset_value = await get_attribute(client, token, asset_id, attribute)
                if asset_value is None:
                    continue

                if not is_trigger_met(asset_value, config):
                    continue

                print(
                    f"[TRIGGER] Trigger met: config {config_id} — "
                    f"asset {asset_id} → {attribute}={asset_value}"
                )

                pm_exists, existing_pm_id = await check_existing_active_pm(client, token, config_id)
                if pm_exists:
                    print(
                        f"[INFO] Existing PM found for config {config_id} → PM ID: {existing_pm_id}"
                    )
                    continue

                pm_id = await create_pm(client, token, config_id)
                if not pm_id:
                    continue

                print(f"[PM] New PM created with ID {pm_id}")
                if await advance_pm(client, token, pm_id):
                    print(f"[PM] PM {pm_id} advanced to 'planning' status")

            await asyncio.sleep(60)
