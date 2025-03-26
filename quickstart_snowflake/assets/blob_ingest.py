import pandas as pd
from dagster import asset
from dagster_azure.adls2 import ADLS2Resource

@asset(
    required_resource_keys={"adls2"}
)
def read_csv_from_adls(context):
    # Specify the file path in ADLS
    file_path = "Landing/VehicleYear-2024.csv"
    
    # Read CSV from ADLS using the ADLS2Resource
    adls2 = context.resources.adls2
    
    # Use ADLS2Resource to get the file stream
    file_stream = adls2.read(file_path)
    
    # Use Pandas to read the CSV file from the stream
    df = pd.read_csv(file_stream)
    
    return df