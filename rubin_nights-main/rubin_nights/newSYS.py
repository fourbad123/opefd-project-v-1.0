import ast
import os
import json
import tkinter as tk
from tkinter import messagebox, ttk
import requests
from dotenv import load_dotenv

load_dotenv()

SYSTEMS_FILE = "/Users/fbustos/Documents/scipt/rubin_nights-main/rubin_nights/sistems.py"

AUTH_URL = os.getenv("AUTH_URL")
URL_ASSET_CARD = os.getenv("URL_ASSET_CARD")
CMMS_USERNAME = os.getenv("CMMS_USERNAME")
CMMS_PASSWORD = os.getenv("CMMS_PASSWORD")

# ========================== #
#        MAIN FUNCTIONS      #
# ========================== #

def load_existing_config():
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
        print(f"Error loading systems.py: {e}")
        return []

def save_all_config(config):
    with open(SYSTEMS_FILE, "w") as f:
        f.write("config_with_interval = [\n")
        for item in config:
            f.write(f"    {item},\n")
        f.write("]\n")

def save_entry():
    entry = {
        "name": name_var.get(),
        "measurement": measurement_var.get(),
        "field": field_var.get(),
        "asset_id": asset_id_var.get(),
        "attribute": attribute_var.get(),
        "db_name": db_name_var.get(),
        "time_interval": time_interval_var.get(),
    }

    sal_index = sal_index_var.get()
    if sal_index:
        try:
            entry["salIndex"] = int(sal_index)
        except ValueError:
            messagebox.showwarning("Input Error", "SAL Index must be an integer.")
            return

    config = load_existing_config()

    if edit_index is not None:
        config[edit_index] = entry
    else:
        config.append(entry)

    save_all_config(config)
    messagebox.showinfo("Saved", "Configuration saved successfully.")
    clear_fields()

def clear_fields():
    global edit_index
    edit_index = None
    for var in [
        name_var, measurement_var, field_var, asset_id_var,
        attribute_var, db_name_var, time_interval_var, sal_index_var
    ]:
        var.set("")

def show_entries():
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
            messagebox.showwarning("Warning", "Please select an entry to edit.")
            return
        idx = sel[0]
        load_for_editing(idx)

    edit_button = tk.Button(top, text="Edit", command=edit_selected)
    edit_button.pack(pady=5)

def load_for_editing(index):
    global edit_index
    edit_index = index
    config = load_existing_config()
    item = config[index]

    name_var.set(item.get("name", ""))
    measurement_var.set(item["measurement"])
    field_var.set(item["field"])
    asset_id_var.set(item["asset_id"])
    attribute_var.set(item["attribute"])
    db_name_var.set(item["db_name"])
    time_interval_var.set(item["time_interval"])
    sal_index_var.set(str(item.get("salIndex", "")))

# ========================== #
#   GET ACTUAL CMMS  DATA    #
# ========================== #

def authenticate_cmms():
    try:
        headers = {"Content-Type": "application/json"}
        credentials = {"username": CMMS_USERNAME, "password": CMMS_PASSWORD}

        if not AUTH_URL:
            messagebox.showerror("Error", "AUTH_URL no está definido en el archivo .env")
            return None

        response = requests.post(AUTH_URL, headers=headers, json=credentials, timeout=10, verify=False)

        if response.status_code == 200:
            token = response.json().get("data", {}).get("_id")
            if not token:
                messagebox.showerror("Error", "Token no encontrado en la respuesta.")
            return token

        messagebox.showerror("Error", f"Falló la autenticación. Código: {response.status_code}")
        return None
    except Exception as e:
        messagebox.showerror("Error", f"Error autenticando en CMMS: {e}")
        return None

def get_current_value(token, asset_id, attribute):
    try:
        url = URL_ASSET_CARD.replace("{asset_id}", str(asset_id))
        headers = {"CMDBuild-Authorization": token}
        resp = requests.get(url, headers=headers, timeout=10, verify=False)
        resp.raise_for_status()
        data = resp.json()

        print(f"\n[DEBUG] Asset ID: {asset_id}")
        print(json.dumps(data, indent=2))

        if not data.get("success"):
            print("⚠️ La respuesta no indica éxito.")
            return None

        data_section = data.get("data", {})

        if attribute in data_section:
            value = data_section[attribute]
            print(f"[DEBUG] Attribute '{attribute}' found in data: {value}")
            return value

        print(f"⚠️ El atributo '{attribute}' no se encontró en la respuesta.")
        return None

    except Exception as e:
        print(f"[ERROR] Fallo al obtener atributo '{attribute}' de asset {asset_id}: {e}")
        return None

def show_current_values():
    token = authenticate_cmms()
    if not token:
        return

    config = load_existing_config()

    win = tk.Toplevel()
    win.title("Valores Actuales en CMMS")
    win.geometry("900x400")

    columns = ("name", "measurement", "field", "current_value")
    tree = ttk.Treeview(win, columns=columns, show="headings")
    tree.heading("name", text="Nombre")
    tree.heading("measurement", text="Measurement")
    tree.heading("field", text="Field")
    tree.heading("current_value", text="Valor actual en CMMS")

    tree.column("name", width=200)
    tree.column("measurement", width=200)
    tree.column("field", width=150)
    tree.column("current_value", width=150)

    tree.pack(fill="both", expand=True, padx=10, pady=10)

    for entry in config:
        val = get_current_value(token, entry["asset_id"], entry["attribute"])
        tree.insert(
            "", "end",
            values=(
                entry.get("name", ""),
                entry.get("measurement", ""),
                entry.get("field", ""),
                val if val is not None else "N/A",
            ),
        )

# ========================== #
#        GUI INTERFACE       #
# ========================== #

edit_index = None

root = tk.Tk()
root.title("Configuration Manager")
root.geometry("500x550")

fields = [
    ("Descriptive Name", "name_var"),
    ("Measurement", "measurement_var"),
    ("Field", "field_var"),
    ("Asset ID", "asset_id_var"),
    ("Attribute", "attribute_var"),
    ("Database Name", "db_name_var"),
    ("Time Interval", "time_interval_var"),
    ("SAL Index (optional)", "sal_index_var"),
]

for idx, (label_text, var_name) in enumerate(fields):
    label = tk.Label(root, text=label_text)
    label.grid(row=idx, column=0, sticky="w", padx=10, pady=5)

    var = tk.StringVar()
    globals()[var_name] = var

    entry = tk.Entry(root, textvariable=var, width=40)
    entry.grid(row=idx, column=1, padx=10, pady=5)

# Botones
save_button = tk.Button(root, text="Save", command=save_entry)
save_button.grid(row=len(fields), column=0, padx=10, pady=20)

show_button = tk.Button(root, text="Show/Edit Configurations", command=show_entries)
show_button.grid(row=len(fields), column=1, padx=10, pady=20)

show_values_button = tk.Button(root, text="Ver valores actuales", command=show_current_values)
show_values_button.grid(row=len(fields) + 1, column=0, columnspan=2, pady=10)

root.mainloop()
