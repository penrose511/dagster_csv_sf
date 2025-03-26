@asset(io_manager_key="io_manager")
def adls2_to_snowflake(context):
    # Fetch the ADLS2 resource from context
    adls2_resource = context.resources.adls2

    # Define the file path in ADLS2
    file_path = "Landing/VehicleYear-2024.csv"  # Replace with the correct path
    
    # Read the file content from ADLS2 into memory
    file_data = adls2_resource.read_file(file_path)
    
    # Convert the CSV content into a pandas DataFrame
    df = pd.read_csv(io.BytesIO(file_data))
    
    # Optional: Transformations on the data
    # For example: df.dropna(inplace=True)
    
    # Add metadata about the file (optional)
    context.add_output_metadata(
        {"file_path": MetadataValue.text(file_path), "num_records": len(df)}
    )
    
    # Return the DataFrame to be written to Snowflake
    return Output(df)
