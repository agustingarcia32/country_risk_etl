import requests
import os
import pandas as pd
import pyarrow as pa
from datetime import datetime, timedelta
from deltalake import write_deltalake, DeltaTable
from pprint import pprint

# Function to make requests to the static endpoint
def get_data_static(base_url, endpoint, data_field, params=None, headers=None):
    try:
        endpoint_url = f"{base_url}/{endpoint}"
        response = requests.get(endpoint_url, params=params, headers=headers)
        response.raise_for_status()

        try:
            data = response.json()
        except ValueError:
            print("The response format is not the expected")
            return None
        return data

    except requests.exceptions.RequestException as e:
        print(f"The request has failed. Error Code: {e}")
        return None

# Define the base URL
base_url = "https://api.argentinadatos.com/v1"

# Define the endpoints
endpoint_1 = "finanzas/indices/riesgo-pais"
endpoint_2 = "finanzas/indices/riesgo-pais/ultimo"

# Get data from the static endpoint
json_data_estatic = get_data_static(base_url, endpoint_1, "data")
if json_data_estatic:
    pprint(json_data_estatic)
else:
    print("No static data found.")

# Get data from the incremental endpoint
# Calculate the timestamp of the last 24 hours
last_24_hours = datetime.now() - timedelta(days=1)
last_24_hours_str = last_24_hours.strftime('%Y-%m-%dT%H:%M:%SZ')

# Define the URL for the second (incremental) endpoint
url_endpoint_2 = f"{base_url}/{endpoint_2}"

# Request parameters
params = {"updated_since": last_24_hours_str}

# Make requests to the incremental endpoint
response = requests.get(url_endpoint_2, params=params)
if response.status_code == 200:
    json_data_temporary = response.json()
    pprint(json_data_temporary)
else:
    print(f"Error getting data: {response.status_code}")
    json_data_temporary = []

# Configure the DeltaLake routes
rute_base_datalake = r"C:\Users\aguss\OneDrive\Desktop\country_risk_etl\data\raw"
rute_temporary = os.path.join(rute_base_datalake, "temporary")
rute_static = os.path.join(rute_base_datalake, "static")

# Create folders if they do not exist
os.makedirs(rute_temporary, exist_ok=True)
os.makedirs(rute_static, exist_ok=True)

# Temporary data (incremental)
if json_data_temporary:
    try:
        df_temporary = pd.json_normalize(json_data_temporary)
        table_temporary = pa.Table.from_pandas(df_temporary)
        write_deltalake(rute_temporary, table_temporary, mode="append")
        print(f"Temporary data saved in Delta Lake: {rute_temporary}")
    except Exception as e:
        print(f"Error processing temporary data: {e}")
else:
    print("No valid temporary data to save.")

# Static data (full load)
if json_data_estatic:
    try:
        df_static = pd.json_normalize(json_data_estatic)
        table_estatic = pa.Table.from_pandas(df_static)
        write_deltalake(rute_static, table_estatic, mode="overwrite")
        print(f"Static data saved in Delta Lake: {rute_static}")
    except Exception as e:
        print(f"Error processing static data: {e}")
else:
    print("No valid static data to save.")

