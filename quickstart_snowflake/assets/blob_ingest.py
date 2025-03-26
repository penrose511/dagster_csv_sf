import pandas as pd
import io
from dagster import asset, Output, MetadataValue
from dagster_azure.adls2 import ADLS2Resource

@asset(io_manager_key="io_manager")
def adls2_to_snowflake(context, adls2: ADLS2Resource):
    # Define the file path in ADLS2
    file_path = "Landing/VehicleYear-2024.csv"  # Replace with the correct path

    # Use the ADLS2Resource to get a file handle (ADLS2FileHandle) for the file
    file_handle = adls2.get_file_handle(file_path)
    
    # Read the file content using the file handle's method
    file_data = file_handle.read()
    
    # Convert the CSV content into a pandas DataFrame
    df = pd.read_csv(io.BytesIO(file_data))
    
    # Optional: Perform transformations on the data
    # Example: df.dropna(inplace=True)
    
    # Add metadata about the file (optional, useful for monitoring/debugging)
    context.add_output_metadata(
        {"file_path": MetadataValue.text(file_path), "num_records": len(df)}
    )
    
    # Log some useful information
    context.log.info(f"Successfully read {len(df)} records from {file_path}")
    
    # Return the DataFrame to be written to Snowflake (or another downstream process)
    return Output(df)
