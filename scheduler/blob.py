import asyncio
from azure.storage.blob.aio import BlobServiceClient
from io import BytesIO
import pandas as pd
import os
from logger import create_logger
import configparser
import pandas as pd
from pathlib import Path
import traceback
import aiofiles
import chardet

logger = create_logger(__name__)
base_dir = str(Path(__file__).parent.resolve())

def load_config():
    config_path = base_dir+'/config.ini'
    try:
        config = configparser.ConfigParser()
        config.read(config_path)
        if not config.sections():
            logger.error(f"No sections found in the configuration file {config_path}")
            exit(1)
        logger.info("Configuration loaded successfully")
        return config
    except FileNotFoundError:
        logger.error(f"Configuration file {config_path} not found")
        exit(1)
    except configparser.Error as e:
        logger.error(f"Error parsing configuration file: {str(e)}")
        exit(1)


config = load_config()
connection_string = config['Blobs']['connection_string'].strip('"')
without_header_container = config['Blobs']['without_header_container']
with_header_container = config['Blobs']['with_header_container']

# Decorator for processing files
def process_file(func):
    async def wrapper(file_content, file_name, blob_name):
        # Call the specific processing function (for CSV/Excel)
        df = await func(file_content)

        # Add the file name as the header in the first cell (first row, first column)
        ent_list = file_name.split("_")
        CarrierID = ent_list[0]
        CustomerID = ent_list[1]
        FileType = ent_list[2]
        
        first_line = f"TSI01~{CarrierID}~{CustomerID}~{FileType}~~~"
        if len(ent_list) > 3:
            diff_code = ent_list[3]
            first_line += f"~{diff_code}"
        columns = df.columns.to_list()
        columns[0] = first_line
        df.columns = columns

        # Convert DataFrame back to the appropriate format and return the byte stream
        content = BytesIO()
        if blob_name.endswith('.csv'):
            df.to_csv(content, index=False)
        elif blob_name.endswith(('.xls', '.xlsx')):
            df.to_excel(content, index=False, engine='openpyxl')

        content.seek(0)
        return content

    return wrapper

# Function to process CSV files
@process_file
async def process_csv(file_content):
    result = chardet.detect(file_content)
    encoding = result['encoding']
    csv_stream = BytesIO(file_content)
    return pd.read_csv(csv_stream, header=None, encoding=encoding)

# Function to process XLS/XLSX files
@process_file
async def process_excel(file_content):
    excel_stream = BytesIO(file_content)
    return pd.read_excel(excel_stream, header=None)


async def process_txt(file_content, file_name, blob_name):
    result = chardet.detect(file_content)
    encoding = result['encoding']
    content = file_content.decode(encoding)
    
    # Add the file name as the header
    ent_list = file_name.split("_")
    CarrierID = ent_list[0]
    CustomerID = ent_list[1]
    FileType = ent_list[2]
    
    first_line = f"TSI01~{CarrierID}~{CustomerID}~{FileType}~~~"
    if len(ent_list) > 3:
        diff_code = ent_list[3]
        first_line += f"~{diff_code}"

    processed_content = first_line + "\n" + content
    
    # Convert back to byte stream
    content = BytesIO(processed_content.encode(encoding))
    return content

# Download, process, and upload files
async def download_and_process_blob(blob_service_client, blob_name):
    try:
        print(f"Processing file: {blob_name}")
        
        # Step 1: Get Blob Client and download the blob content
        async with blob_service_client.get_container_client(without_header_container) as container_client:
            async with container_client.get_blob_client(blob_name) as blob_client:
                # Download the blob
                blob_data = await blob_client.download_blob()
                blob_content = await blob_data.readall()

        # Extract file name without extension
        file_name = os.path.splitext(blob_name)[0]

        # Step 2: Process the file based on its type
        if blob_name.endswith('.csv'):
            content = await process_csv(blob_content, file_name, blob_name)
        elif blob_name.endswith(('.xls', '.xlsx')):
            content = await process_excel(blob_content, file_name, blob_name)
        elif blob_name.endswith('.txt'):
            content = await process_txt(blob_content, file_name, blob_name)
        else:
            print(f"Unsupported file type: {blob_name}")
            return

        # Step 3: Upload processed file to destination blob
        original_filename = ("_").join(file_name.split("_")[::-1][:2])+os.path.splitext(blob_name)[1]
        await upload_to_blob(blob_service_client, with_header_container, content, original_filename)
        await delete_blob_from_container(blob_service_client, without_header_container, blob_name)

        print(f"Finished processing {blob_name}")
        print("---------------------------------------")

    except Exception as e:
        print(f"Error processing file {blob_name}: {e}")

# Upload processed content to destination container
async def upload_to_blob(blob_service_client, container_name, content, blob_name):
    async with blob_service_client.get_container_client(container_name) as container_client:
        async with container_client.get_blob_client(blob_name) as blob_client:
            # Upload content to the destination blob
            await blob_client.upload_blob(content, overwrite=True)
            print(f"Uploaded file: {blob_name} to {container_name}")


async def upload_files_to_blob(blob_service_client, input_dir, container_name):
    """Upload files from local directory to Azure Blob Storage and delete them after upload."""
    
    # Iterate over each file in the directory
    for file_name in os.listdir(input_dir):
        file_path = os.path.join(input_dir, file_name)

        # Open the file asynchronously and read its content
        async with aiofiles.open(file_path, 'rb') as file_data:
            content = await file_data.read()

        # Upload the file to the blob
        await upload_to_blob(blob_service_client, container_name, content, file_name)

        # Delete the file after successful upload
        try:
            os.remove(file_path)
            logger.info(f"Deleted local file: {file_path}")
        except Exception as e:
            logger.error(f"Error deleting file {file_path}: {e}")

    print(f"Files from {input_dir} uploaded and deleted locally.")

# Download Blob
async def download_blob(blob_service_client, container_name, blob_name):
    """Download file from Azure Blob Storage."""
    async with blob_service_client.get_container_client(container_name) as container_client:
        blob_client = container_client.get_blob_client(blob_name)
        stream = await blob_client.download_blob()
        content = await stream.readall()
        logger.info(f"Downloaded file {blob_name} from {container_name}")
        return content

async def delete_blob_from_container(blob_service_client, container_name, blob_name):
    """
    Delete a file (blob) from an Azure Blob Storage container.
    """
    try:
        # Get the blob client for the specified blob
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        # Delete the blob
        await blob_client.delete_blob()
        print(f"Blob '{blob_name}' deleted successfully.")
    except Exception as e:
        print(f"Error deleting blob '{blob_name}': {str(traceback.format_exc())}")


# List and process all files asynchronously
async def list_and_process_files():
    # Use async with to ensure proper closure of BlobServiceClient
    async with BlobServiceClient.from_connection_string(connection_string) as blob_service_client:
        async with blob_service_client.get_container_client(without_header_container) as container_client:
            # List all the blobs in the container
            async for blob in container_client.list_blobs():
                await download_and_process_blob(blob_service_client, blob.name)

# Main function to execute
async def main():
    await list_and_process_files()

# Run the main function
if __name__ == "__main__":
    asyncio.run(main())
