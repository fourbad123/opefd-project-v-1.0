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

import ast
import os
import tkinter as tk
from tkinter import messagebox, ttk, filedialog
import requests  # type: ignore
import pandas as pd
from dotenv import load_dotenv  # type: ignore

# ========================== #
#       LOAD .ENV VALUES     #
# ========================== #

load_dotenv()

SYSTEMS_FILE = "/Users/fbustos/Documents/scipt/rubin_nights-main/rubin_nights/sistems.py"
AUTH_URL = os.getenv("AUTH_URL")
URL_ASSET_CARD = os.getenv("URL_ASSET_CARD")
CMMS_USERNAME = os.getenv("CMMS_USERNAME")
CMMS_PASSWORD = os.getenv("CMMS_PASSWORD")


# ========================== #
#     CONFIG FILE HANDLING   #
# ========================== #

def load_existing_config():
    """Load existing configurations from systems.py."""
    if not os.path.exists(SYSTEMS_FILE):
        with open(SYSTEMS_FILE, "w") as f:
            f.write("config_with_interval = []\n")
        return []

    with open(SYSTEMS_FILE, "r") as f:
        content = f.read()

    try:
        tree = ast.parse(content)
        for node in tree.body:
            if isinstance(node, ast.Assign) and node.targets[0].id == "config_with_interval":
                return ast.literal_eval(ast.unparse(node.value))
    except Exception as e:
        print(f"[ERROR] Failed to parse systems.py: {e}")
        return []


def save_all_config(config):
    """Overwrite systems.py with the new configuration list."""
    with open(SYSTEMS_FILE, "w") as f:
        f.write("config_with_interval = [\n")
        for item in config:
            f.write(f"    {item},\n")
        f.write("]\n")


# ========================== #
#       CORE OPERATIONS      #
# ========================== #

def save_entry():
    """Save new or updated entry to configuration file."""
    entry = {
        "name": field_vars["name_var"].get(),
        "measurement": field_vars["measurement_var"].get(),
        "field": field_vars["field_var"].get(),
        "asset_id": field_vars["asset_id_var"].get(),
        "attribute": field_vars["attribute_var"].get(),
        "db_name": field_vars["db_name_var"].get(),
        "time_interval": field_vars["time_interval_var"].get(),
    }

    sal_index = field_vars["sal_index_var"].get()
    if sal_index:
        try:
            entry["salIndex"] = int(sal_index)
        except ValueError:
            messagebox.showwarning("Input Error", "SAL Index must be an integer.")
            return

    config = load_existing_config()
    names = [c["name"] for c in config]

    if edit_index[0] is not None:
        config[edit_index[0]] = entry
    else:
        if entry["name"] in names:
            messagebox.showwarning("Duplicate Entry", f"'{entry['name']}' already exists.")
            return
        config.append(entry)

    save_all_config(config)
    messagebox.showinfo("Saved", "Configuration saved successfully.")
    clear_fields()


def clear_fields():
    """Reset all input fields."""
    edit_index[0] = None
    for var in field_vars.values():
        var.set("")


def show_entries():
    """Display saved entries and allow editing."""
    top = tk.Toplevel()
    top.title("Saved Entries")
    top.geometry("800x400")

    config = load_existing_config()
    listbox = tk.Listbox(top, width=100)
    listbox.pack(padx=10, pady=10, fill="both", expand=True)

    for idx, item in enumerate(config):
        display = f"{idx + 1}. {item.get('name', '[no name]')} | {item['measurement']} → {item['field']}"
        listbox.insert(tk.END, display)

    def edit_selected():
        sel = listbox.curselection()
        if not sel:
            messagebox.showwarning("Warning", "Select an entry to edit.")
            return
        load_for_editing(sel[0])

    tk.Button(top, text="Edit", command=edit_selected).pack(pady=5)


def load_for_editing(index):
    """Load a configuration entry into input fields for editing."""
    edit_index[0] = index
    config = load_existing_config()
    item = config[index]

    for key, var in field_vars.items():
        value = item.get(key.replace("_var", ""), "")
        var.set(str(value))


def import_from_excel():
    "Import configurations from Excel file. Avoid duplicates based on 'name'."
    filepath = filedialog.askopenfilename(
        title="Select Excel File",
        filetypes=[("Excel Files", "*.xlsx *.xls")]
    )

    if not filepath:
        return

    try:
        df = pd.read_excel(filepath)
        required = {"name", "measurement", "field", "asset_id", "attribute", "db_name", "time_interval"}
        if not required.issubset(df.columns):
            messagebox.showerror("Error", f"Excel must include: {', '.join(required)}")
            return

        imported = df.to_dict(orient="records")
        config = load_existing_config()
        names_existing = {entry["name"] for entry in config}
        skipped = []
        added = []

        for row in imported:
            if row["name"] in names_existing:
                skipped.append(row["name"])
                continue
            if "salIndex" in row and pd.notna(row["salIndex"]):
                try:
                    row["salIndex"] = int(row["salIndex"])
                except ValueError:
                    messagebox.showwarning("Warning", f"SAL Index must be integer for '{row['name']}'")
                    continue
            elif "salIndex" in row:
                del row["salIndex"]
            config.append(row)
            added.append(row["name"])

        save_all_config(config)

        msg = f"{len(added)} new configurations imported."
        if skipped:
            msg += "\n\nSkipped (already exist):\n" + "\n".join(skipped)
        messagebox.showinfo("Import Completed", msg)

    except Exception as e:
        messagebox.showerror("Import Error", f"Failed to import: {e}")


# ========================== #
#       CMMS INTERFACE       #
# ========================== #

def authenticate_cmms():
    """Authenticate with CMMS and return session token."""
    try:
        headers = {"Content-Type": "application/json"}
        credentials = {"username": CMMS_USERNAME, "password": CMMS_PASSWORD}

        if not AUTH_URL:
            messagebox.showerror("Error", "AUTH_URL not defined in .env")
            return None

        response = requests.post(AUTH_URL, headers=headers, json=credentials, timeout=10, verify=False)
        response.raise_for_status()

        token = response.json().get("data", {}).get("_id")
        if not token:
            messagebox.showerror("Error", "Token not found in response.")
        return token

    except Exception as e:
        messagebox.showerror("Error", f"Authentication failed: {e}")
        return None


def get_current_value(token, asset_id, attribute):
    """Fetch the current value of a specific asset attribute."""
    try:
        url = URL_ASSET_CARD.replace("{asset_id}", str(asset_id))
        headers = {"CMDBuild-Authorization": token}
        resp = requests.get(url, headers=headers, timeout=10, verify=False)
        resp.raise_for_status()
        return resp.json().get("data", {}).get(attribute)

    except Exception as e:
        print(f"[ERROR] {asset_id}.{attribute}: {e}")
        return None


def show_current_values():
    """Display current attribute values from CMMS."""
    token = authenticate_cmms()
    if not token:
        return

    config = load_existing_config()

    win = tk.Toplevel()
    win.title("Actual Values in CMMS")
    win.geometry("900x400")

    columns = ("name", "measurement", "field", "current_value")
    tree = ttk.Treeview(win, columns=columns, show="headings")
    for col in columns:
        tree.heading(col, text=col.replace("_", " ").capitalize())
        tree.column(col, width=200)

    tree.pack(fill="both", expand=True, padx=10, pady=10)

    for entry in config:
        val = get_current_value(token, entry["asset_id"], entry["attribute"])
        tree.insert("", "end", values=(
            entry.get("name", ""),
            entry.get("measurement", ""),
            entry.get("field", ""),
            val if val is not None else "N/A",
        ))


# ========================== #
#          GUI SETUP         #
# ========================== #

root = tk.Tk()
root.title("Configuration Manager")
root.geometry("500x620")

edit_index = [None]

field_vars = {
    "name_var": tk.StringVar(),
    "measurement_var": tk.StringVar(),
    "field_var": tk.StringVar(),
    "asset_id_var": tk.StringVar(),
    "attribute_var": tk.StringVar(),
    "db_name_var": tk.StringVar(),
    "time_interval_var": tk.StringVar(),
    "sal_index_var": tk.StringVar(),
}

fields_order = [
    ("Descriptive Name", "name_var"),
    ("Measurement", "measurement_var"),
    ("Field", "field_var"),
    ("Asset ID", "asset_id_var"),
    ("Attribute", "attribute_var"),
    ("Database Name", "db_name_var"),
    ("Time Interval", "time_interval_var"),
    ("SAL Index (optional)", "sal_index_var"),
]

for idx, (label, varname) in enumerate(fields_order):
    tk.Label(root, text=label).grid(row=idx, column=0, sticky="w", padx=10, pady=5)
    tk.Entry(root, textvariable=field_vars[varname], width=40).grid(row=idx, column=1, padx=10, pady=5)

tk.Button(root, text="Save", command=save_entry).grid(row=len(fields_order), column=0, padx=10, pady=20)
tk.Button(root, text="Show/Edit Configurations", command=show_entries).grid(row=len(fields_order),
                                                                            column=1, padx=10, pady=20)
tk.Button(root, text="See actual values", command=show_current_values).grid(row=len(fields_order)+1, column=0,
                                                                            columnspan=2, pady=10)
tk.Button(root, text="Import from Excel", command=import_from_excel).grid(row=len(fields_order)+2, column=0,
                                                                          columnspan=2, pady=10)

root.mainloop()
