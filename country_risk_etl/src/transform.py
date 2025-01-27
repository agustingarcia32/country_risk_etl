import pandas as pd
from deltalake import DeltaTable, write_deltalake
import os
import pyarrow as pa

# raw data path
path_raw = r"C:\Users\aguss\OneDrive\Desktop\country_risk_etl\data\raw"

# Function to check if a path contains a Delta Lake table
def is_deltalake(path):
    """Check if the given path contains a Delta Lake table."""
    return os.path.exists(os.path.join(path, "_delta_log"))

# Verify if the raw data contains valid Delta tables
if not is_deltalake(path_raw + "/static") or not is_deltalake(path_raw + "/temporary"):
    print("One or both Delta tables do not exist. Exiting.")
    exit()

# Read the raw data
df_static = DeltaTable(path_raw + "/static").to_pandas()
df_temporary = DeltaTable(path_raw + "/temporary").to_pandas()

# Eliminate duplicates and replace null values
df_temporary = df_temporary.drop_duplicates(subset=["fecha"], keep="first")
df_static["valor"].fillna(0, inplace=True)
df_temporary["valor"].fillna(0, inplace=True)

# Rename columns
df_static = df_static.rename(columns={"fecha": "Date", "valor": "Country_Risk"})
df_temporary = df_temporary.rename(columns={"fecha": "Date", "valor": "Last_Country_Risk"})

# Classify country risk
def classify_risk(valor):
    if valor > 1500:
        return "Very High"
    elif valor > 1000:
        return "High"
    else:
        return "Moderate"

df_static["riesgo_clasificado"] = df_static["Country_Risk"].apply(classify_risk)
df_temporary["riesgo_clasificado"] = df_temporary["Last_Country_Risk"].apply(classify_risk)


# create new rows with the date missing in df_static
fechas_faltantes = ["2025-01-22", "2025-01-03"]
nuevas_filas = pd.DataFrame({
    "Country_Risk": [0] * len(fechas_faltantes),
    "Date": fechas_faltantes,
    "riesgo_clasificado": ["Moderate"] * len(fechas_faltantes)
})

# add new rows in df_static
df_static = pd.concat([df_static, nuevas_filas], ignore_index=True)

print("Datos de prueba agregados a df_static:")
print(df_static[df_static["Date"].isin(fechas_faltantes)])

# Data crossing
df_mixs = df_temporary.merge(
    df_static,
    on="Date",
    how="inner",
    suffixes=("_Risk", "_Old")
)

if df_mixs.empty:
    print("Error: El DataFrame 'df_mixs' está vacío. No hay datos para escribir.")
else:
    print(f"DataFrame 'df_mixs' tiene {len(df_mixs)} filas y estas columnas: {df_mixs.columns}")
    print(df_mixs.head())


# Save the processed data
route_processed = r"C:\Users\aguss\OneDrive\Desktop\country_risk_etl\data\processed"
os.makedirs(route_processed, exist_ok=True)

write_deltalake(
    os.path.join(route_processed, "mixs"),
    pa.Table.from_pandas(df_mixs),
    mode="overwrite",
)

print("Saved processed data.")
