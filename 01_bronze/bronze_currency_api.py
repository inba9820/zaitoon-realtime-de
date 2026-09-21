# Databricks notebook source
import requests
import json
import datetime
from databricks.sdk import WorkspaceClient

# COMMAND ----------
def get_rate_to_inr(currency_code):
    url = f"https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/{currency_code}.json"
    response = requests.get(url)
    data = response.json()
    return data[currency_code]["inr"]

# COMMAND ----------
aed_rate = get_rate_to_inr("aed")
qar_rate = get_rate_to_inr("qar")

print("1 AED =", aed_rate, "INR")
print("1 QAR =", qar_rate, "INR")

# COMMAND ----------
today = datetime.date.today().isoformat()

currency_rates = [
    {"currency_code": "INR", "rate_to_inr": 1.0, "rate_date": today},
    {"currency_code": "AED", "rate_to_inr": aed_rate, "rate_date": today},
    {"currency_code": "QAR", "rate_to_inr": qar_rate, "rate_date": today},
]

# COMMAND ----------
import tempfile
import os

file_name = f"currency_rates_{today}.json"
local_path = os.path.join(tempfile.gettempdir(), file_name)

with open(local_path, "w") as f:
    json.dump(currency_rates, f, indent=2)

w = WorkspaceClient()
w.files.upload_from(
    f"/Volumes/zaitoon_catalog/bronze/raw_events/api_currency/{file_name}",
    local_path,
    overwrite=True,
)

print(f"Uploaded {file_name}")