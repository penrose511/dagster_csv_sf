from dagster import asset
from azure.storage.blob import BlobServiceClient
from dagster_azure.adls2 import ADLS2Resource

@asset(required_resource_keys={"adls2"})
def azure_blob_file_list(context):
    # Get the adls2 resource from context
    adls2: ADLS2Resource = context.resources.adls2
    
    # Initialize the BlobServiceClient using the storage account URL and SAS token
    blob_service_client = BlobServiceClient(
        account_url=f"https://{adls2.storage_account}.blob.core.windows.net",
        credential=adls2.credential.token,
    )
    
    # Specify the container to list files from
    container_name = "storage"
    container_client = blob_service_client.get_container_client(container_name)
    
    # List all blobs in the container
    file_names = []
    blobs_list = container_client.list_blobs()
    
    for blob in blobs_list:
        file_names.append(blob.name)
    
    # Log the number of files found and return the file names
    context.log.info(f"Found {len(file_names)} files in container '{container_name}'")
    
    return file_names
