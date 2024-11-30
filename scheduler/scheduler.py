import os
import random
from datetime import datetime, timezone
import asyncio
import schedule
import time
import pymongo
import aiohttp
from pymongo import ReturnDocument
from enum import Enum
import logging
from io import BytesIO
from motor.motor_asyncio import AsyncIOMotorClient
import aiofiles
from azure.storage.blob.aio import BlobServiceClient
import pandas as pd
import chardet
import configparser
from logger import create_logger
from pathlib import Path
import traceback
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__))))
from blob import *

# Setup logger
logger = create_logger(__name__)
base_dir = str(Path(__file__).parent.resolve())

# Load configuration from file
def load_config():
    config_path = base_dir + '/config.ini'
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
with_header_input_dir = config['Directories']["with_header_input_dir"]
without_header_input_dir = config['Directories']["without_header_input_dir"]
host = config['Directories']['host']
db_connection_string = config['Database']['connection_string']
db_str = config['Database']['db']

# Blob containers and connection string
connection_string = config['Blobs']['connection_string'].strip('"')
without_header_container = config['Blobs']['without_header_container']
with_header_container = config['Blobs']['with_header_container']
apifeed_container = config['Blobs']['apifeed_container']
local_files = config.getboolean('Blobs', 'local_files')

# Connection to blob
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# MongoDB connection
try:
    client = AsyncIOMotorClient(db_connection_string)
    db = client[db_str]
    FileLog = db["FileLog"]
    JobDetail = db["JobDetail"]
    LogDetails = db["LogDetail"]
    counters = db["Counter"]
    Jobs = db["Jobs"]
    logger.info("Connected to MongoDB")
except Exception as e:
    logger.error(f"Failed to connect to MongoDB: {str(e)}")
    exit()

# File type to JobID mapping
file_type_to_job_id = {
    "BOL": 1001,  # 1
    "BL": 1001, # 1
    "PO": 1002,  # 2
    "210": 1003,  # 3
    "110": 1004,  # 4
    "WEB": 1005,  # 5
    "BOL_OUT": 1006   # 6
}

# API Endpoints
api_endpoints = {
    "BOL": f"http://{host}:8005/process_bol",
    "BL": f"http://{host}:8005/process_bol",
    "PO": f"http://{host}:8003/process_po",
    "WEB": f"http://{host}:8004/process_web",
    "210": f"http://api_endpoint/API_210",
    "110": f"http://{host}:8006/process_110",
    "BOL_OUT": f"http://api_endpoint/BOLOUT_API"
}

# Define Enum for LogType
class LogTypeEnum(int, Enum):
    SOURCE = 1
    TRANSFORMATION = 2
    TARGET = 3

# Define Enum for Status
class StatusEnum(int, Enum):
    SUCCESS = 2
    IN_PROGRESS = 1
    FAILED = 3

# Function to process a single file type
async def process_file_type(session, file_type, files, api_url):
    try:
        jobdetail_id = await get_next_sequence_value("JobDetail")
        job_id = file_type_to_job_id[file_type]
        job_details = {
            "_id": jobdetail_id,
            "JobID": job_id,
            "StartOn": datetime.now(timezone.utc),
            "Status": "P"
        }

        await JobDetail.insert_one(job_details)
        files.append({'JobDetailID': jobdetail_id})

        async with session.post(api_url, json=files) as response:
            if response.status == 200:
                logger.info(f"Successfully processed files: {files} for file type: {file_type}")
            else:
                logger.error(f"Failed API call with response code {response.status} for file type: {file_type} and files: {files}")
    except aiohttp.ClientError as e:
        logger.error(f"Error processing files: {files} for file type: {file_type}: {str(e)}")

# Function to process a group of files
async def process_files_group(files_by_type):
    logger.info(f"Starting process_files_group with files: {files_by_type}")

    if not files_by_type:
        logger.error("No files provided for processing")
        return

    async with aiohttp.ClientSession() as session:
        tasks = []
        for file_type, files in files_by_type.items():
            api_url = api_endpoints.get(file_type)
            if not api_url:
                logger.error(f"Invalid file type {file_type} for files: {files}")
                continue
            tasks.append(process_file_type(session, file_type, files, api_url))

        if tasks:
            await asyncio.gather(*tasks)


async def get_next_sequence_value(sequence_name):
    """Ensure the sequence document exists with an initial value."""
    await counters.update_one(
        {'_id': sequence_name},
        {'$setOnInsert': {'sequence_value': 0}},
        upsert=True
    )

    """Get the next sequence value for the specified sequence name."""
    sequence_document = await counters.find_one_and_update(
        {'_id': sequence_name},
        {'$inc': {'sequence_value': 1}},
        return_document=ReturnDocument.AFTER
    )
    return sequence_document['sequence_value']

# File renaming and processing
async def rename_file(blob_service_client, with_header_container, apifeed_container, blob_name):
    try:
        logger.info(f"Starting rename_file for blob: {blob_name}")
        
        # Download the file from the blob
        raw_data = await download_blob(blob_service_client, with_header_container, blob_name)
        encoding = chardet.detect(raw_data)['encoding']
        
        if blob_name.endswith(('.xls', '.xlsx')):
            df = pd.read_excel(BytesIO(raw_data), encoding=encoding)
            first_line = df.columns[0]  # Get the first row as a dictionary
            file_extension = os.path.splitext(blob_name)[1]
        else:
            first_line = raw_data.decode(encoding).splitlines()[0].split(',')[0]
            file_extension = '.txt'

        # Process the file name based on the first line
        parts = first_line.split('~')
        if len(parts) >= 3:
            FileType = parts[3]
            CarrierID = parts[1]
            CustomerID = parts[2]
        else:
            logger.error("Seems like file doesn't have a header!!!")
            return None

        diff_code = ""
        if len(parts) > 7:
            diff_code = "_" + parts[7]

        datetime_str = datetime.now().strftime('%Y%m%d%H%M%S%f')[:-2]

        # Determine file type
        if FileType == 'BL':
            FileType = "BOL"
        elif FileType in ['IA', 'IM', 'IO'] and CarrierID not in ['ZFFILE', 'AP1', 'U1P']:
            FileType = "WEB"

        new_file_name = f"{FileType}_{CustomerID}_{CarrierID}{diff_code}_{datetime_str}{file_extension}"

        # Log the renaming details in MongoDB
        filelog_id = await get_next_sequence_value("FileLog")
        file_details = {
            "_id": filelog_id,
            "CustomerId": CustomerID,
            "CarrierId": CarrierID,
            "OriginalFileName": blob_name,
            "NewFileName": new_file_name,
            "Status": 0,  # Initial Status 0
            "FileType": FileType,
            "OutputFileName": ""
        }

        check_strings = [
            'PO_NVISION_TSI5673',
            'IM_NVISION_ZFFILE',
            'IA_TSI5753_U1P',
            'IM_TSI5753_AP1',
            'IM_TSI5753_U1P',
        ]

        # Use any() to check if any string in the list is in NewFileName
        if any(s in file_details['NewFileName'] for s in check_strings):
            file_details['FileType'] = 'BOL'
            if "PO" not in new_file_name:
                new_file_name = new_file_name.replace(new_file_name.split('_')[0], 'BOL')
                file_details['NewFileName'] = new_file_name
        
        # Upload renamed file to the apifeed container
        await FileLog.insert_one(file_details)
        await upload_to_blob(blob_service_client, apifeed_container, raw_data, new_file_name)
        
        logger.info(f"Renamed and uploaded file: {blob_name} to {new_file_name}")
        return file_details
    except Exception as e:
        logger.info(f"Error Occured while renaming the file {blob_name}: {str(traceback.format_exc())}")
    finally:
        await delete_blob_from_container(blob_service_client, with_header_container, blob_name)


# Main function to process and rename files
async def main():
    if local_files:
        # Move files from local to blob (cloud)
        await upload_files_to_blob(blob_service_client, with_header_input_dir, with_header_container)
        await upload_files_to_blob(blob_service_client, without_header_input_dir, without_header_container)

    # Add header to non header files from 'without header container' and move to 'with header container'
    await list_and_process_files()

    logging.info(f"Fetching files from blob container: {with_header_container}")

    files_by_type = {}
    files_to_update = []

    # List blobs in the source container
    async with blob_service_client.get_container_client(with_header_container) as container_client:
        blob_list = container_client.list_blobs()
        
        tasks = [
            rename_file(blob_service_client, with_header_container, apifeed_container, blob.name)
            async for blob in blob_list
        ]

    results = await asyncio.gather(*tasks)

    for file_details in results:
        if file_details:
            file_type = file_details["FileType"]
            if file_type not in files_by_type:
                files_by_type[file_type] = []
            files_by_type[file_type].append(file_details)
            files_to_update.append(file_details['_id'])

    if files_to_update:
        logging.info("Updating file status in FileLog")
        await FileLog.update_many(
            {"_id": {"$in": files_to_update}},
            {"$set": {"Status": 1}}
        )
        logging.info(f"FileLog updated for ids: {files_to_update}")
        await process_files_group(files_by_type)

    await asyncio.sleep(60)

# Schedule the main function
def run_scheduler():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

# Entry point
if __name__ == "__main__":
    schedule.every(10).seconds.do(run_scheduler)

    while True:
        schedule.run_pending()
        time.sleep(1)
