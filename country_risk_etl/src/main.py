import os
from extract import main as extract_data
from transform import main as transform_data
from load import main as load_data

def setup_directories():
    """Ensure all necessary directories exist."""
    base_path = os.path.dirname(os.path.abspath(__file__))
    data_dirs = [
        os.path.join(base_path, "data/raw/static"),
        os.path.join(base_path, "data/raw/temporary"),
        os.path.join(base_path, "data/processed/mixs"),
    ]

    for directory in data_dirs:
        os.makedirs(directory, exist_ok=True)

if __name__ == "__main__":
    print("Setting up directories...")
    setup_directories()

    print("Starting ETL pipeline...")

    print("Step 1: Extracting data...")
    extract_data()

    print("Step 2: Transforming data...")
    transform_data()

    print("Step 3: Loading data...")
    load_data()

    print("ETL pipeline completed successfully!")
