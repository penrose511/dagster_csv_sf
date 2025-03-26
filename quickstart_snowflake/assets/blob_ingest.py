import pandas as pd
from azure.storage.blob import BlobServiceClient
from dagster import asset, Output

@asset(
    required_resource_keys={"adls2"},
    name="read_vehicle_year_csv",
    description="Reads the 'Landing/VehicleYear-2024.csv' file from Azure Blob Storage and returns a DataFrame.",
)
def read_vehicle_year_csv(context) -> pd.DataFrame:
    # Retrieve the ADLS2 resource from the context
    adls2_resource = context.resources.adls2
    
    # Define the container and blob (file) name
    container_name = "your-container-name"  # Replace with your actual container name
    blob_name = "Landing/VehicleYear-2024.csv"  # Blob path
    
    # Create the BlobServiceClient using the provided storage account
    blob_service_client = BlobServiceClient(
        account_url=f"https://{adls2_resource.storage_account}.blob.core.windows.net",
        credential=adls2_resource.credential
    )
    
    # Get the blob client
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    
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
            "blob_url": f"https://{adls2_resource.storage_account}.blob.core.windows.net/{container_name}/{blob_name}"
        }
    )
