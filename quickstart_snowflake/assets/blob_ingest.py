from dagster import asset
import pandas as pd
from io import StringIO
from azure.storage.blob import BlobServiceClient
from dagster_azure.adls2 import ADLS2Resource

# Function to read CSV from ADLS2
def read_csv_from_adls2(storage_account, sas_token, container_name, blob_folder, file_name):
    """Reads a CSV file from Azure Data Lake Storage Gen2 into a Pandas DataFrame."""
    try:
        # Create a BlobServiceClient using the SAS token
        blob_service_client = BlobServiceClient(
            account_url=f"https://{storage_account}.blob.core.windows.net",
            credential=sas_token
        )

        # Get a reference to the blob
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=f"{blob_folder}{file_name}")

        # Download the blob content
        blob_data = blob_client.download_blob().readall()

        # Decode the content and read into a pandas DataFrame
        csv_data = blob_data.decode('utf-8')
        df = pd.read_csv(StringIO(csv_data))
        print("About to return df")
        return df

    except Exception as e:
        print(f"Error reading CSV from ADLS2: {e}")
        # Return None in case of an exception
        return None

# Function to move the file from Landing to Archive
def move_file_to_archive(storage_account, sas_token, container_name, blob_folder, file_name, archive_folder):
    """Moves the file from the Landing folder to the Archive folder."""
    try:
        # Create a BlobServiceClient using the SAS token
        blob_service_client = BlobServiceClient(
            account_url=f"https://{storage_account}.blob.core.windows.net",
            credential=sas_token
        )

        # Get references to the blobs
        source_blob_client = blob_service_client.get_blob_client(container=container_name, blob=f"{blob_folder}{file_name}")
        archive_blob_client = blob_service_client.get_blob_client(container=container_name, blob=f"{archive_folder}{file_name}")

        # Copy the file to the Archive folder
        archive_blob_client.start_copy_from_url(source_blob_client.url)
        print(f"Copying {file_name} from {blob_folder} to {archive_folder}.")

        # Delete the original file from the Landing folder
        source_blob_client.delete_blob()
        print(f"Deleted {file_name} from {blob_folder}.")

    except Exception as e:
        print(f"Error moving file to Archive: {e}")

# Asset to read CSV, print it, and move the file to Archive
@asset
def adls2_csv_asset(adls2: "ADLS2Resource"):
    """Dagster asset to read a CSV file from Azure Data Lake Storage, return a Pandas DataFrame, and move the file to Archive."""
    storage_account = adls2.storage_account
    sas_token = adls2.credential.token
    container_name = "storage"
    
    # Set the folder and file name separately
    blob_folder = "Landing/"
    file_name = "VehicleYear-2024.csv"
    archive_folder = "Archive/"  # Archive folder variable

    # Call the function to read the CSV
    df = read_csv_from_adls2(storage_account, sas_token, container_name, blob_folder, file_name)

    if df is not None:
        print(df.head())  # Print the first few rows of the DataFrame

        # Move the file to the Archive folder after reading
        move_file_to_archive(storage_account, sas_token, container_name, blob_folder, file_name, archive_folder)
        
        return df