from dagster import asset, Definitions
import pandas as pd
from io import StringIO
from dagster_azure.adls2 import ADLS2Resource
from azure.storage.blob import BlobServiceClient

# Function to read CSV from ADLS2
def read_csv_from_adls2(storage_account, sas_token, container_name, blob_name):
    """Reads a CSV file from Azure Data Lake Storage Gen2 into a Pandas DataFrame."""
    try:
        # Create a BlobServiceClient using the SAS token
        blob_service_client = BlobServiceClient(
            account_url=f"https://{storage_account}.blob.core.windows.net",
            credential=sas_token
        )

        # Get a reference to the blob
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        # Download the blob content
        blob_data = blob_client.download_blob().readall()

        # Decode the content and read into a pandas DataFrame
        csv_data = blob_data.decode('utf-8')
        df = pd.read_csv(StringIO(csv_data))

        return df

    except Exception as e:
        print(f"Error reading CSV from ADLS2: {e}")
        return None

# Asset to read CSV and return a dataframe
@asset
def adls2_csv_asset(adls2: "ADLS2Resource"):
    """Dagster asset to read a CSV file from Azure Data Lake Storage and return a Pandas DataFrame."""
    storage_account = adls2.storage_account
    sas_token = adls2.credential.token
    container_name = "storage"  # Replace with your container name
    blob_name = "Landing/VehicleYear-2024.csv"  # Replace with your blob name

    # Call the function to read the CSV
    df = read_csv_from_adls2(storage_account, sas_token, container_name, blob_name)

    if df is not None:
        print(df.head())  # Print the first few rows of the DataFrame
        return df
    else:
        raise ValueError("Failed to load CSV from ADLS2")