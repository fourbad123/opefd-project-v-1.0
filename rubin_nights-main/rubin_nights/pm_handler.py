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

# Load environment variables from .env file
load_dotenv()

CHILE_TZ = pytz.timezone("America/Santiago")

# Environment URLs
URL_PREV_MAINT_CONFIG_CARDS = os.getenv("URL_PREV_MAINT_CONFIG_CARDS")
URL_PREV_MAINT_CONFIG_CARD = os.getenv("URL_PREV_MAINT_CONFIG_CARD")
URL_ASSET_CARD = os.getenv("URL_ASSET_CARD")
URL_CREATE_PM_INSTANCE = os.getenv("URL_CREATE_PM_INSTANCE")
URL_ACTIVITIES = os.getenv("URL_ACTIVITIES")
URL_ADVANCE = os.getenv("URL_ADVANCE")


def get_current_time_chile() -> str:
    "Return the current time in Chile timezone formatted as UTC string."
    return datetime.now(CHILE_TZ).strftime("%Y-%m-%dT%H:%M:%SZ")


async def get_prev_maint_configs(client: httpx.AsyncClient, token: str) -> list:
    """
    Fetch all preventive maintenance configurations.

    Args:
        client: HTTP async client.
        token: Authorization token.

    Returns:
        List of maintenance configuration dicts, or empty list on failure.
    """
    headers = {"CMDBuild-Authorization": token}
    try:
        response = await client.get(URL_PREV_MAINT_CONFIG_CARDS, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json().get("data", [])
    except Exception as e:
        print(f"[ERROR] Could not fetch configurations: {e}")
        return []


async def get_prev_maint_config_by_id(client: httpx.AsyncClient, token: str, config_id: str) -> dict:
    """
    Fetch a specific preventive maintenance configuration by ID.

    Args:
        client: HTTP async client.
        token: Authorization token.
        config_id: Configuration identifier.

    Returns:
        Dict of configuration data or empty dict on failure.
    """
    headers = {"CMDBuild-Authorization": token}
    url = URL_PREV_MAINT_CONFIG_CARD.format(config_id=config_id)
    try:
        response = await client.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json().get("data", {})
    except Exception as e:
        print(f"[ERROR] Could not fetch config {config_id}: {e}")
        return {}


async def get_attribute(client: httpx.AsyncClient, token: str, asset_id: str | int, attribute: str):
    """
    Get a specific attribute from a CMMS asset.

    Args:
        client: HTTP async client.
        token: Authorization token.
        asset_id: Asset identifier.
        attribute: Attribute name.

    Returns:
        Value of the attribute or None on failure.
    """
    headers = {"CMDBuild-Authorization": token}
    url = URL_ASSET_CARD.format(asset_id=asset_id)
    try:
        response = await client.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()["data"].get(attribute)
    except Exception as e:
        print(f"[CMMS ERROR] Could not get '{attribute}' from asset {asset_id}: {e}")
        return None


def is_trigger_met(value, config: dict) -> bool:
    """
    Check if the maintenance trigger condition is met based on config.

    Args:
        value: Current value of the monitored attribute.
        config: Configuration dictionary containing trigger values.

    Returns:
        True if trigger condition is met, False otherwise.
    """
    try:
        trigger_types = [
            ("trigger_integer", float),
            ("trigger_string", str),
            ("trigger_time", str),
            ("trigger_True_False", lambda x: str(x).lower() in ("true", "1")),
        ]

        for key, caster in trigger_types:
            trigger_value = config.get(key)
            if trigger_value is not None:
                if value is None:
                    print(f"[WARN] Value is None for config {config.get('_id')}")
                    return False
                try:
                    return caster(value) == caster(trigger_value)
                except (ValueError, TypeError) as e:
                    print(f"[WARN] Failed casting {key}: {e}")
                    return False

        print(f"[WARN] No valid trigger in config {config.get('_id')}")
        return False
    except Exception as e:
        print(f"[ERROR] Trigger evaluation failed: {e}")
        return False


async def check_existing_active_pm(client: httpx.AsyncClient,
                                   token: str,
                                   config_id: str) -> tuple[bool, str | None]:
    """
    Check if there is already an active PM for the given configuration.

    Args:
        client: HTTP async client.
        token: Authorization token.
        config_id: Configuration identifier.

    Returns:
        Tuple (exists: bool, pm_id: str or None).
    """
    headers = {
        "CMDBuild-Authorization": token,
        "Content-Type": "application/json",
    }
    try:
        response = await client.get(URL_CREATE_PM_INSTANCE, headers=headers, timeout=10)
        response.raise_for_status()
        for pm in response.json().get("data", []):
            if pm.get("PrevMaintConfig") == config_id:
                status = pm.get("_status_description", "").lower()
                if status != "aborted":
                    return True, pm.get("_id")
        return False, None
    except Exception as e:
        print(f"[WARN] Could not check for existing PM: {e}")
        return False, None


async def create_pm(client: httpx.AsyncClient, token: str, config_id: str) -> str | None:
    """
    Create a new preventive maintenance (PM) instance.

    Args:
        client: HTTP async client.
        token: Authorization token.
        config_id: Configuration identifier.

    Returns:
        PM instance ID string if created, None otherwise.
    """
    headers = {
        "CMDBuild-Authorization": token,
        "Content-Type": "application/json",
    }
    config_data = await get_prev_maint_config_by_id(client, token, config_id)
    if not config_data:
        return None

    payload = {
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
        response = await client.post(URL_CREATE_PM_INSTANCE, headers=headers, json=payload, timeout=10)
        response.raise_for_status()
        return response.json().get("data", {}).get("_id")
    except Exception as e:
        print(f"[PM ERROR] Could not create PM: {e}")
        return None


async def advance_pm(client: httpx.AsyncClient, token: str, pm_id: str) -> bool:
    """
    Advance the PM workflow to the planning phase.

    Args:
        client: HTTP async client.
        token: Authorization token.
        pm_id: PM instance identifier.

    Returns:
        True if advanced successfully, False otherwise.
    """
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
        print(f"[PM ERROR] Could not fetch activities: {e}")
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
        print(f"[PM ERROR] Could not advance PM: {e}")
        return False


async def maintenance_loop(token: str) -> None:
    """
    Main monitoring loop to check triggers and generate PMs.

    Args:
        token: Authorization token.
    """
    async with httpx.AsyncClient(verify=False) as client:
        while True:
            configs = await get_prev_maint_configs(client, token)
            print(f"[INFO] Retrieved {len(configs)} configurations.")

            for config in configs:
                config_id = config.get("_id")
                asset_id = config.get("Asset_related")

                if not asset_id:
                    print(f"[WARN] Missing asset for config {config_id}")
                    continue

                system_cfg = next((c for c in config_with_interval if c["asset_id"] == str(asset_id)), None)
                if not system_cfg:
                    print(f"[WARN] No system config for asset {asset_id}")
                    continue

                attribute = system_cfg["attribute"]
                asset_value = await get_attribute(client, token, asset_id, attribute)
                if asset_value is None:
                    continue

                if not is_trigger_met(asset_value, config):
                    continue

                print(f"[TRIGGER] Met: config {config_id} — asset {asset_id} → {attribute}={asset_value}")

                exists, existing_pm_id = await check_existing_active_pm(client, token, config_id)
                if exists:
                    print(f"[INFO] Existing PM found: ID {existing_pm_id}")
                    continue

                new_pm_id = await create_pm(client, token, config_id)
                if not new_pm_id:
                    continue

                print(f"[PM] Created: ID {new_pm_id}")
                if await advance_pm(client, token, new_pm_id):
                    print(f"[PM] Advanced to planning: ID {new_pm_id}")

            await asyncio.sleep(60)
