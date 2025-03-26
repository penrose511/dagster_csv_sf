import pandas as pd
import io
from dagster import asset, Output, MetadataValue
from azure.storage.filedatalake import DataLakeServiceClient
from io import BytesIO

# Helper function to read the file from ADLS2
def read_file_from_adls2(storage_account_name: str, file_system_name: str, file_path: str, credential: str):
    service_client = DataLakeServiceClient(account_url=f"https://{storage_account_name}.dfs.core.windows.net", credential=credential)
    file_system_client = service_client.get_file_system_client(file_system_name)
    file_client = file_system_client.get_file_client(file_path)
    
    # Read the file into memory
    download = file_client.download_file()
    file_data = download.readall()
    
    return file_data

@asset(io_manager_key="io_manager")
def adls2_to_snowflake(context, adls2: ADLS2Resource):
    # Define the Azure storage account and file system
    storage_account_name = "my_storage_account"  # Your storage account name
    file_system_name = "my_file_system"  # Your file system name (container in ADLS2)
    file_path = "Landing/VehicleYear-2024.csv"  # File path in ADLS2
    credential = "your_sas_token"  # SAS Token or credential to access ADLS2
    
    # Use the helper function to read the file content from ADLS2
    file_data = read_file_from_adls2(storage_account_name, file_system_name, file_path, credential)
    
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
