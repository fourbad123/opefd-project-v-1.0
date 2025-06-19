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
import requests
from dotenv import load_dotenv

from sistems import config_with_interval
from efd_monitor import monitor_subsystem
from pm_handler import maintenance_loop

# Load environment variables from .env file
load_dotenv()

# Environment variables
AUTH_URL = os.getenv("AUTH_URL")
USERNAME = os.getenv("CMMS_USERNAME")
PASSWORD = os.getenv("CMMS_PASSWORD")


def get_token():
    headers = {"Content-Type": "application/json"}
    credentials = {
        "username": USERNAME,
        "password": PASSWORD,
    }

    if not AUTH_URL:
        print("[ERROR] AUTH_URL is not defined in the .env file")
        return None

    try:
        response = requests.post(
            AUTH_URL, headers=headers, json=credentials, verify=False)
        response.raise_for_status()
        return response.json().get("data", {}).get("_id")
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Failed to obtain token: {e}")
        return None


async def main():
    site = "summit"
    db_name = "efd"
    token = get_token()

    if not token:
        print("[ERROR] Failed to obtain token.")
        return

    monitoring_tasks = [
        monitor_subsystem(token, site, db_name, cfg)
        for cfg in config_with_interval
    ]
    monitoring_tasks.append(maintenance_loop(token))

    await asyncio.gather(*monitoring_tasks)


if __name__ == "__main__":
    asyncio.run(main())
