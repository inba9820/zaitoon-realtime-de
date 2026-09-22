# Databricks notebook source
from hdbcli import dbapi
import json
import datetime
import tempfile
import os
from databricks.sdk import WorkspaceClient

# COMMAND ----------
HANA_CONFIG = {
    "address": "375d0e7e-d619-4087-bd6d-5a2e4fd454a4.hana.prod-ap21.hanacloud.ondemand.com",
    "port": 443,
    "user": "DBADMIN",
    "password": "Zaitoon@2026",
    "encrypt": True,
    "sslValidateCertificate": True,
}
VOLUME_BASE = "/Volumes/zaitoon_catalog/bronze/raw_events/sap_hana"

TABLES = ["cost_centers", "vendor_master", "gl_entries"]

# COMMAND ----------
def json_default(value):
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    return str(value)

def extract_table(conn, w, table_name, timestamp):
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name}")
    columns = [desc[0] for desc in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    cursor.close()

    if not rows:
        print(f"{table_name}: no rows found, skipping.")
        return

    file_name = f"{table_name}_{timestamp}.json"
    local_path = os.path.join(tempfile.gettempdir(), file_name)

    with open(local_path, "w") as f:
        json.dump(rows, f, default=json_default, indent=2)

    remote_path = f"{VOLUME_BASE}/{table_name}/{file_name}"
    w.files.upload_from(remote_path, local_path, overwrite=True)

    print(f"{table_name}: uploaded {len(rows)} row(s) to {remote_path}")

# COMMAND ----------
def run_extraction():
    conn = dbapi.connect(**HANA_CONFIG)
    w = WorkspaceClient()
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    try:
        for table in TABLES:
            extract_table(conn, w, table, timestamp)
    finally:
        conn.close()

run_extraction()