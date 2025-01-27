from deltalake import DeltaTable

# rute processed data
rute_processed = r"C:\Users\aguss\OneDrive\Desktop\country_risk_etl\data\processed\mixs"

# read processed data
df_mix = DeltaTable(rute_processed).to_pandas()

# show result or export
print("mix data ready for analysis:")
print(df_mix.head())

df_mix.to_csv("final_data.csv", index=False)
print("export data to 'final_data.csv'.")
