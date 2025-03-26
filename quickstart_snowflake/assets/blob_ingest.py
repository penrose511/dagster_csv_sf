import pandas as pd
import dagster as dg
from dagster_azure.blob import AzureBlobStorageResource, AzureBlobStorageDefaultCredential

@dg.asset(io_manager_key="io_manager")
def read_csv_from_blob(adls2: AzureBlobStorageResource):
    with adls2.get_client() as blob_storage_client:
        # Replace 'my-container' and 'path/to/myfile.csv' with your actual container and file path
        blob_client = blob_storage_client.get_blob_client(container='my-container', blob='path/to/myfile.csv')
        
        # Download the blob content
        csv_data = blob_client.download_blob().readall()
        
        # Convert the byte data into a pandas DataFrame
        df = pd.read_csv(pd.compat.StringIO(csv_data.decode('utf-8')))
        
        # Return the DataFrame
        return df
