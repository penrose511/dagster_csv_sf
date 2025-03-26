import pandas as pd
import dagster as dg
import io
from dagster_azure.blob import AzureBlobStorageResource

@dg.asset(io_manager_key="io_manager")
def read_csv_from_blob(adls2: AzureBlobStorageResource):
    # Define the correct file path in the Azure Blob Storage
    file_path = "Landing/VehicleYear-2024.csv"  # Your specific file path in Blob Storage
    
    # Get the Blob Storage Client
    with adls2.get_client() as blob_storage_client:
        # Get the blob client for the specific file in the container
        blob_client = blob_storage_client.get_blob_client(container='my-container', blob=file_path)
        
        # Download the blob content (this will return byte data)
        csv_data = blob_client.download_blob().readall()
        
        # Convert the byte data into a pandas DataFrame
        # Use io.StringIO for reading the byte data as a string
        df = pd.read_csv(io.StringIO(csv_data.decode('utf-8')))
        
        # Return the DataFrame
        return df
