import os
import pandas as pd
from deltalake import DeltaTable
from datetime import datetime


def is_deltalake(path):
    """Check if the given path contains a Delta Lake table."""
    return os.path.exists(os.path.join(path, "_delta_log"))


def create_folders(paths):
    """Create directories if they do not exist."""
    for path in paths:
        os.makedirs(path, exist_ok=True)


def classify_risk(value):
    """Classify country risk based on thresholds."""
    if value > 1500:
        return "Very High"
    elif value > 1000:
        return "High"
    else:
        return "Moderate"


def read_delta_table(path):
    """Read Delta Lake table and return as pandas DataFrame."""
    if not is_deltalake(path):
        raise FileNotFoundError(f"Delta table not found at: {path}")
    return DeltaTable(path).to_pandas()


def add_missing_dates(df, missing_dates, default_value=0, classification="Moderate"):
    """Add rows for missing dates to a DataFrame."""
    new_rows = pd.DataFrame({
        "Country_Risk": [default_value] * len(missing_dates),
        "Date": missing_dates,
        "riesgo_clasificado": [classification] * len(missing_dates),
    })
    return pd.concat([df, new_rows], ignore_index=True)

