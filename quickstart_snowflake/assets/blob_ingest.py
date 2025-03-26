from dagster import asset
from dagster_azure.adls2 import ADLS2Resource
from azure.storage.blob import ContainerClient

@asset(required_resource_keys={"adls2"})
def azure_blob_file_list(context):
    # Get the adls2 resource from context
    adls2: ADLS2Resource = context.resources.adls2
    
    # Container name
    container_name = "storage"
    
    # Correct ContainerClient initialization with container_name as a separate argument
    container_client = ContainerClient(
        account_url=f"https://{adls2.storage_account}.blob.core.windows.net",
        credential=adls2.credential.token,
        container_name=container_name
    )
    
    # List all blobs in the container
    file_names = []
    blobs_list = container_client.list_blobs()
    
    for blob in blobs_list:
        file_names.append(blob.name)
    
    # Log the number of files found and return the file names
    context.log.info(f"Found {len(file_names)} files in container '{container_name}'")
    
    return file_names
