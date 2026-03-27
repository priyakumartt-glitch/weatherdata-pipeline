# Databricks notebook source
# MAGIC %pip install azure-identity azure-keyvault-secrets

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import requests
import os
from datetime import datetime
import json

key = dbutils.secrets.get(scope="weather-scope", key="weatherdata")

# Cities list
CITY =["Kochi","Bangalore","Chennai","Delhi","Mumbai","Hyderabad"]

# Bronze folder
bronze_path = "bronze"
os.makedirs(bronze_path, exist_ok=True)

for city in CITY:

    # API URL
    URL =f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={key}"

    # Request
    response = requests.get(URL)
    data = response.json()

    # Check if API returned error
    if data.get("cod") != 200:
        print(f"Error for {city}: {data.get('message')}")
        continue

    # Timestamp for unique file name
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # File name
    file_name = f"{bronze_path}/weather_{city}_{timestamp}.json"

    # Save JSON
    with open(file_name, "w") as f:
        json.dump(data, f, indent=4)

    print(f"{city} weather data saved")

print("All cities data collected")