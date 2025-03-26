import pandas as pd
from azure.storage.blob import BlobServiceClient
from azure.core.credentials import AzureSasCredential  # Import the correct credential type
from dagster import asset, Output

@asset(
    required_resource_keys={"adls2"},
    name="read_vehicle_year_csv",
    description="Reads the 'Landing/VehicleYear-2024.csv' file from Azure Blob Storage and returns a DataFrame.",
)
def read_vehicle_year_csv(context) -> pd.DataFrame:
    # Retrieve the ADLS2 resource from the context
    adls2_resource = context.resources.adls2
    
    # Blob path (file name)
    blob_name = "Landing/VehicleYear-2024.csv"
    
    # Use AzureSasCredential for the SAS token
    sas_credential = AzureSasCredential(adls2_resource.credential.token)
    
    # Create the BlobServiceClient using the provided storage account and AzureSasCredential
    blob_service_client = BlobServiceClient(
        account_url=f"https://{adls2_resource.storage_account}.blob.core.windows.net",
        credential=sas_credential
    )
    
    # Get the blob client, using the container name from the resource
    blob_client = blob_service_client.get_blob_client(container=adls2_resource.container_name, blob=blob_name)
    
    # Download the blob content (CSV file)
    stream = blob_client.download_blob()
    
    # Load the CSV data into a pandas DataFrame
    csv_content = stream.readall()
    data = pd.read_csv(pd.compat.StringIO(csv_content.decode('utf-8')))
    
    # Return the DataFrame with metadata
    return Output(
        data,
        metadata={
            "num_rows": len(data),
            "blob_url": f"https://{adls2_resource.storage_account}.blob.core.windows.net/{adls2_resource.container_name}/{blob_name}"
        }
    )
