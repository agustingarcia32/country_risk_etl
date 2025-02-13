# Country Risk ETL Project

## Overview
This ETL project processes country risk data using an API (https://argentinadatos.com). The pipeline extracts raw data, transforms it, and stores processed data in Delta Lake format for analysis.

## Project Structure
```
Country_Risk_ETL/
|-- data/
|   |-- raw/
|   |   |-- static/
|   |   |-- temporary/
|   |-- processed/
|       |-- mixs/
|-- src/
|   |-- extract.py
|   |-- transform.py
|   |-- load.py
|   |-- utils.py
|-- main.py
|-- README.md
```

## Installation
1. Clone this repository:
   ```
   git clone <repository_url>
   cd Country_Risk_ETL
   ```

2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

## How to Run
1. Place your API key in the `extract.py` script.
2. Run the main script:
   ```
   python main.py
   ```

## Scripts Description
- **main.py**: Coordinates the ETL pipeline.
- **extract.py**: Extracts raw data from the API.
- **transform.py**: Cleans and processes raw data.
- **load.py**: Saves processed data to Delta Lake.
- **utils.py**: Contains reusable helper functions.

## Output
The final processed data is saved in the `data/processed/` folder and exported as `final_data.csv` for analysis.

## Example Usage
Run the ETL pipeline and analyze the resulting CSV file:
```python
import pandas as pd

df = pd.read_csv("final_data.csv")
print(df.head())
```

## License
This project is licensed under the Apache License 2.0.
"""
