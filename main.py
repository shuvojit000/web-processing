import pandas as pd
import os
import time
import math
from process_data import *
from logger import create_logger
import traceback
from pathlib import Path
import numpy as np
from custom_rule.BlackHorse_custom_rule import *
from custom_rule.BomiGroup_custom_rule import *
from custom_rule.DHLChina_custom_rule import *
from custom_rule.Sarcona_custom_rule import *
from custom_rule.OrianExport_custom_rule import *
from custom_rule.OrianImport_custom_rule import *
from custom_rule.Manifest_custom_rule import *
from custom_rule.Teradyne_NXP_Taiwan_custom_rule import *
from custom_rule.DHL_NXP_Hongkong_custom_rule import *
from custom_rule.NXP_Thailand_custom_rule import *
from custom_rule.ManifestIN_custom_rule import *
from custom_rule.GlobalFlatFile_custom_rule import *
import sys
import configparser
from fastapi import FastAPI, UploadFile, HTTPException, Request
from fastapi.responses import FileResponse, StreamingResponse
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne
from datetime import datetime, timezone
import aiofiles
import aiofiles.os
import requests
# Add the parent directory to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utilities.ErrorMessages import ErrorMessage, Status
from azure.storage.blob.aio import BlobServiceClient
import io
from scheduler.blob import download_blob
from utilities.validation_code import *
from utilities.common import *

app = FastAPI()
base_dir = str(Path(__file__).parent.resolve())
logger = create_logger(__name__)

start = time.time()

# Load configuration from file
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

db_connection_string = config['Database']['connection_string']
input_path = config['Directories']['input_dir']
archive_path = config['Directories']['archive_dir']
output_path = config['Directories']['output_dir']
error_log_url = config['Directories']['error_log_url']

# Blob containers and connection string
connection_string = config['Blobs']['connection_string'].strip('"')
without_header_container = config['Blobs']['without_header_container']
with_header_container = config['Blobs']['with_header_container']
apifeed_container = config['Blobs']['apifeed_container']
output_container = config['Blobs']['output_container']
local_download = config.getboolean('Blobs', 'local_download')

# Initialize BlobServiceClient and container clients
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
apifeed_container_client = blob_service_client.get_container_client(apifeed_container)
output_container_client = blob_service_client.get_container_client(output_container)

log_details = {
"Source": {},  # Initialize Source to avoid KeyError
"Transformation": [],
"Target": {}
}


try:
    client = AsyncIOMotorClient(db_connection_string)
    db = client["FlatFileMigrationQA"]
    FileLog = db["FileLog"]
    JobDetail = db["JobDetail"]
    LogDetails = db["LogDetail"]
    counters = db["Counter"]
    Jobs = db["Jobs"]
    logger.info("Connected to MongoDB")
except Exception as e:
    logger.error(f"Failed to connect to MongoDB: {str(e)}")
    exit()

@app.post("/process_web")
async def process_file_web(request: Request):
    try:
        # Read the JSON data from the request body
        files = await request.json()
        operations = []
        
        jobdetail_id = next((file['JobDetailID'] for file in files if 'JobDetailID' in file), None)

        for file in files:
            if 'NewFileName' in file:
                filename = file['NewFileName']

                # Download the file from Blob Storage
                blob_client = apifeed_container_client.get_blob_client(filename)
                blob_data = await blob_client.download_blob()
                raw_data = await blob_data.readall()

                # Process the file (assuming process_file is your processing function)
                output_file = await process_file(raw_data, filename, jobdetail_id)
                
                update_data = {
                    "Status": 2,
                    "OutputFileName": output_file
                }

                operations.append(UpdateOne({"_id": file['_id']}, {"$set": update_data}))
            # else:
            #     jobdetail_id = file['JobDetailID']

        # Execute all operations in a single bulk request
        logger.info("Updating file status to 2 in FileLog and Job Details after files has been processed")
        await JobDetail.update_one({"_id": jobdetail_id}, {"$set": {"CompletedOn": datetime.now(timezone.utc), "Status": "C"}})
        result = await FileLog.bulk_write(operations)
        logger.info(f"Matched count: {result.matched_count} \n Modified count: {result.modified_count}")

        return {"message": "All files processed successfully"}
    except Exception as e:
        logger.error(f"Error occurred: {str(traceback.format_exc())}")
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")


async def process_file(raw_data, fileName, jobdetail_id):
    try:
        log_details = {
            "Source": {},
            "Transformation": [],
            "Target": {}
        }
        start_time = time.time()

        # Call the process_source_file function to process the source file
        # To Do -- Caching to be implmented
        customer_mapping_file = base_dir + r"/web-mapping.xlsx" 
        df_new = await process_source(jobdetail_id, fileName, customer_mapping_file, log_details, raw_data)

        # df_new.to_csv(output_path + '/excel.txt', index=False, encoding='utf-8')

        # Call the new process_transformation_file function to process the rows
        formatted_rows = await process_transformation(df_new, log_details)

        # Call the new process_target_file function to write the output

        # validate length of formatted_rows -- Dibyendu

        output_file_path = await process_target(fileName, output_path, formatted_rows, log_details)

        end_time = time.time()
        time_taken = end_time - start_time
        print(f"Time taken to process the file {fileName}: {time_taken:.2f} seconds")
        return output_file_path

    except Exception as e:
        logger.error(f"Error processing file {fileName}: {e}")
        logger.error(traceback.format_exc())
        error_log_path = os.path.join(base_dir, 'io', 'error_logs.txt')
        with open(error_log_path, 'a') as error_file:
            error_file.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Error processing file {fileName}: {e}\n")
            error_file.write(traceback.format_exc() + '\n')
            error_file.write('-' * 80 + '\n')




async def process_source(jobdetail_id, file_name, customer_mapping_file, log_details, raw_data):
    # Initialize a blank DataFrame
    df_new = pd.DataFrame()
    try:
        started_on = get_current_utc_iso()

        # For testing encoding
        # result = chardet.detect(raw_data)
        # encoding = result['encoding']
        # Create file path

        # input_file_path = os.path.join(input_path, file_name)
        
        # Split file name to extract IDs
        arr_file_name = file_name.split('_')
        DocumentID = arr_file_name[0]
        ReceiverID = arr_file_name[1]
        SenderID = arr_file_name[2]

        FileID = f"{DocumentID}_{ReceiverID}_{SenderID}"
        
        # Read the file to get the second line as column names
        if file_name.endswith((".csv", ".txt")):
            # Skip the first line and read the second line for headers
            # f.readline()  # Skip the first line
            second_line = raw_data.decode("UTF-8").splitlines()[1].split('\t')
        else:  # For .xlsx and .xls files
            # with pd.ExcelFile(io.BytesIO(raw_data), engine='openpyxl' if file_name.endswith('.xlsx') else 'xlrd') as xls:
            # Read the first sheet and get headers
            second_line = pd.read_excel(io.BytesIO(raw_data), header=None, skiprows=1).iloc[0].tolist()

        # Convert headers to lowercase and handle duplicates
        header_counts = {}
        lowercase_headers = []

        for header in second_line:
            lower_header = header.lower()
            if lower_header in header_counts:
                header_counts[lower_header] += 1
                # Create a new header name like "iva code.1", "iva code.2", etc.
                new_header = f"{lower_header}.{header_counts[lower_header]}"
                lowercase_headers.append(new_header)
            else:
                header_counts[lower_header] = 0
                lowercase_headers.append(lower_header)

        # Load the text file into a DataFrame
        if file_name.endswith((".csv", ".txt")):
            df_input = pd.read_csv(io.BytesIO(raw_data), delimiter='\t', skiprows=2, names=lowercase_headers, keep_default_na=False, dtype=str)
        elif file_name.endswith((".xlsx", ".xls")):
            df_input = pd.read_excel(io.BytesIO(raw_data), skiprows=1, names=lowercase_headers, keep_default_na=False, dtype=str)

        
        # # Rename duplicate columns after converting to lowercase
        # df_input = rename_duplicate_columns(df_input)
        
        # Add SenderID and ReceiverID columns to the DataFrame
        df_input['senderid'] = SenderID
        df_input['receiverid'] = ReceiverID

        # Loop through all columns to check if 'TSI99' exists in any cell in the last row
        if df_input.apply(lambda x: 'TSI99' in str(x.iloc[-1]), axis=0).any():
            df_input = df_input.iloc[:-1]

        
        # Strip spaces from column names
        df_input.columns = df_input.columns.str.strip()

        # Strip spaces from data values
        df_input = df_input.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        # Logging
        logger.info(f"Processing partner to standard columns for file {file_name}...")

        # Identify partner and function based on FileID
        match FileID:
            case 'WEB_TSI5092_PNSK':
                partner_name = 'BLACKHORSE'
                function_name = "BlackHorse_Custom_Rule"
            case 'WEB_TSI5748_BG07':
                partner_name = 'BOMI GROUP'
                function_name = "BomiGroup_Custom_Rule"
            case 'WEB_TSI5920_DH78':
                partner_name = 'DHL CHINA'
                function_name = "DhlChina_Custom_Rule"
            case 'WEB_TSI5810_OS08OUT':
                partner_name = 'ORIAN EXPORT'
                function_name = "OrianExport_Custom_Rule"
            case 'WEB_TSI5810_OS08IN':
                partner_name = 'ORIAN IMPORT'
                function_name = "OrianImport_Custom_Rule"
            case 'WEB_AGFA_SARCONA' | 'WEB_ECO3_SARCONA':
                partner_name = 'SARCONA'
                function_name = "Sarcona_Custom_Rule"
            case 'WEB_NVGIND_NVGIND':
                ReceiverID = 'NVG5810'
                partner_name = 'MANIFEST NVGIND'
                function_name = "Manifest_Custom_Rule"
            case (
                'WEB_TSI5918_CW10' | 'WEB_TSI01_OS08' | 'WEB_TSI5918_EA23' | 
                'WEB_TSI5918_CW10' | 'WEB_TSI5918_DC21' | 'WEB_TSI5918_DG19' | 'WEB_TSI5918_EA23' |'WEB_TSI5918_HM04' | 
                'WEB_TSI5918_PC19' | 'WEB_TSI5918_PE17' | 'WEB_TSI5918_PT28' | 'WEB_TSI3960_AG12' | 
                'WEB_TSI3960_JS10' | 'WEB_TSI3960_KL12' | 'WEB_TSI3960_TN29' | 'WEB_TSI3960_UP49'
            ):
                partner_name = 'TERADYNE AND NXP TAIWAN ALL'
                function_name = "Teradyne_NXP_Taiwan_Custom_Rule"
            case 'WEB_TSI3960_DS54':
                partner_name = 'TERADYNE DS54'
                function_name = "Teradyne_NXP_Taiwan_Custom_Rule"
            case 'WEB_TSI3960_GW02':
                partner_name = 'TERADYNE GW02'
                function_name = "Teradyne_NXP_Taiwan_Custom_Rule"
            case 'WEB_TSI5918_UP19' | 'WEB_TSI5918_UP49':
                partner_name = 'NXP TAIWAN UP19'
                function_name = "Teradyne_NXP_Taiwan_Custom_Rule"
            case 'WEB_TSI5920_DG21':
                partner_name = 'DHL NXP HONGKONG'
                function_name = "DHL_NXP_Hongkong_Custom_Rule"
            case (
                'WEB_TSI5920_HT32' | 'WEB_TSI5920_PW12'
            ):
                partner_name = 'NXP THAILAND'
                function_name = "NXP_Thailand_Custom_Rule"
            case (
                'WEB_TSI5673_CW28' | 'WEB_TSI5673_JH10' | 'WEB_TSI5673_SN07' | 'WEB_TSI5673_TD22'
            ):
                partner_name = 'GLOBAL FLAT FILE 2'
                function_name = "NXP_Thailand_Custom_Rule"
            case (
                'WEB_TSI5822_SC36' | 'WEB_TSI5822_SE20' | 'WEB_TSI5822_US09' | 'WEB_TSI5822_US11' |
                'WEB_TSI5037_SL34' | 'WEB_TSI5822_DH73' | 'WEB_TSI5810_BHS1'
            ):
                partner_name = 'MANIFEST IN'
                function_name = "ManifestIN_Custom_Rule"
            case 'WEB_TSI5810_EA30':
                partner_name = 'MANIFEST_IN_EA30'
                function_name = "ManifestIN_Custom_Rule"
            case 'WEB_TSI5810_KWEI':
                partner_name = 'MANIFEST_IN_KWEI_TSI5810'
                function_name = "ManifestIN_Custom_Rule"
            case 'WEB_TSI5822_SC38':
                partner_name = 'MANIFEST_IN_SC38_TSI5822'
                function_name = "ManifestIN_Custom_Rule"
            case 'WEB_TSI5822_NE21':
                partner_name = 'MANIFEST_IN_NE21_TSI5822'
                function_name = "ManifestIN_Custom_Rule"
            case 'WEB_TSI5822_DS54' | 'WEB_TSI5822_EA15' | 'WEB_TSI3975_SC23' | 'WEB_TSI3975_SC36' | 'WEB_TSI5810_YL04':
                partner_name = 'GlobalFile_DS54_TSI5822'
                function_name = "GlobalFlatFile_Custom_Rule"
            case 'WEB_TSI5694_GSON' | 'WEB_TSI5092_LDVY' | 'WEB_TSI5694_MA05' | 'WEB_TSI5694_NEED' | 'WEB_TSI5694_OTDI':
                partner_name = 'GlobalFile_GSON_TSI5694'
                function_name = "GlobalFlatFile_Custom_Rule"
            case 'WEB_TSI5694_MKED':
                partner_name = 'GlobalFile_MKED_TSI5694'
                function_name = "GlobalFlatFile_Custom_Rule"
            case _:
                partner_name = 'Unknown'
                raise ValueError(f"Unknown FileID: {FileID}")
            

        # Initialize column mapping
        column_mapping = process_partner_columns(customer_mapping_file, partner_name)

        # Create a dictionary to hold the new columns
        new_columns_dict = {}
        for new_col, actual_col in column_mapping.items():
            actual_col = actual_col.strip()  # Trim the column name
            if actual_col in df_input.columns:
                new_columns_dict[new_col] = df_input[actual_col].astype(str)  # Map the column
            else:
                new_columns_dict[new_col] = pd.Series([''] * len(df_input), index=df_input.index)  # Fill missing columns with empty strings

        # Create the new DataFrame from the dictionary
        # df_new = pd.DataFrame(new_columns_dict, dtype=str)
        df_new = pd.concat([df_new, pd.DataFrame(new_columns_dict)], ignore_index=True)
        df_new['senderid'] = SenderID
        df_new['receiverid'] = ReceiverID
        

        # Apply the custom rule function
        df_new = getattr(sys.modules[__name__], function_name)(df_new, df_input)

        df_new['billed_amount'] = pd.to_numeric(df_new['billed_amount'], errors='coerce')

        # Filter out rows where 'billed_amount' is 0
        df_new = df_new[df_new['billed_amount'] != 0]


        # uncomment for testing
        # in_output_file_path = os.path.join(output_path, file_name) + '_in.xlsx'
        # new_output_file_path = os.path.join(output_path, file_name) + '_new.xlsx'
        # df_input.to_excel(in_output_file_path, index=False)
        # df_new.to_excel(new_output_file_path, index=False)

        

        # To Do - implement at utility to access all type of file process
        completed_on = get_current_utc_iso()
        log_summary = f"{ErrorMessage.SOURCE_SUCCESS.value} {file_name}"

        # Add values to the log_details dictionary
        CustomerID = df_new['customer_id'].iloc[0]

        log_details["JobDetailId"] = jobdetail_id
        log_details["FileName"] = file_name
        log_details["CustomerId"] = CustomerID
        log_details["CarrierScacCode"] = SenderID
        log_details["Source"]["StartedOn"] = started_on
        log_details["Source"]["Status"] = Status.SUCCESS.value
        log_details["Source"]["CompleteOn"] = completed_on
        log_details["LogSummary"] = log_summary
        return df_new

    except Exception as e:
        # To Do - implement in log utility, enum creation for log messages
        logger.error(f"{ErrorMessage.SOURCE_FAILURE.value} {file_name}: {e}")
        log_summary = f"{ErrorMessage.SOURCE_FAILURE.value} {file_name}: {e}"
        completed_on = get_current_utc_iso()
        status = Status.FAILURE
        # Add values to the log_details dictionary
        CustomerID = df_new['customer_id'].iloc[0]

        log_details["JobDetailId"] = jobdetail_id
        log_details["FileName"] = file_name
        log_details["CustomerId"] = CustomerID
        log_details["CarrierScacCode"] = SenderID
        log_details["Source"]["StartedOn"] = started_on
        log_details["Source"]["Status"] = Status.FAILURE.value
        log_details["Source"]["CompletedOn"] = completed_on
        log_details["LogSummary"] = log_summary
        response = requests.post(error_log_url, json=log_details)
        if response.status_code == 200:
            print("Test passed: Log entry inserted successfully.")
        else:
            print(f"Test failed: {response.status_code} - {response.text}")

async def process_transformation(df_new, log_details):
    # Initialize an empty list to hold formatted rows and logs
    formatted_rows = []

    validation_dictionary = get_validation_dict(base_dir+"/utilities/EDIFlatFileReader.Config", "web")
    validated_df = validate_dataframe(df_new, validation_dictionary)
    # Initialize SequenceId counter
    sequence_id = 1

    # Loop through the DataFrame and process each row
    for _, row in validated_df.iterrows():
        started_on = get_current_utc_iso()
        pro_no = str(row['pro_no'])

        try:
            if row['validation_status'] == False:
                raise ValueError(row['validation_message'])
            # Process the row with the process_web function
            formatted_rows = process_web(row, row.name, formatted_rows)
            logger.info("Row processing done")

            # Success case: set status and log completion time
            completed_on = get_current_utc_iso()
            log_summary = f"{ErrorMessage.TRANSFORMATION_SUCCESS.value} {pro_no}"

            # Append success log record to the Transformation list
            log_details["Transformation"].append({
                "SequenceId": sequence_id,
                "ReferenceNumber": pro_no,
                "StartedOn": started_on,
                "Status": Status.SUCCESS.value,
                "CompletedOn": completed_on,
                "Log": log_summary
            })

        except Exception as e:
            # Log the error and update the log details for the failed row
            logger.error(f"{ErrorMessage.TRANSFORMATION_FAILURE.value} {pro_no}: {str(e)}")

            # Failed case: set status and log completion time
            completed_on = get_current_utc_iso()
            log_summary = f"{ErrorMessage.TRANSFORMATION_FAILURE.value} {pro_no}: {str(e)}"

            # Append failed log record to the Transformation list
            log_details["Transformation"].append({
                "SequenceId": sequence_id,
                "ReferenceNumber": str(row['pro_no']),
                "StartedOn": started_on,
                "Status": Status.FAILURE.value,
                "CompletedOn": completed_on,
                "Log": log_summary
            })

        # Increment SequenceId for the next row
        sequence_id += 1
    return formatted_rows

async def process_target(file_name, output_path, formatted_rows, log_details):
    started_on = get_current_utc_iso()
    try:
        if len(formatted_rows) == 0:
            raise ValueError("The formatted_rows is empty. Cannot proceed with process_target.")
        # Create the output file name and path
        output_file_name = os.path.splitext(os.path.basename(file_name))[0] + '.txt'
        output_file_path = os.path.join(output_path, output_file_name)
        
        logger.info(f"Writing the data to the file {output_file_path}...")

        # Prepare the formatted rows for upload (joining them as a single string)
        processed_content = '\n'.join(formatted_rows)

        # Upload the file to the Azure Blob container asynchronously
        blob_client = output_container_client.get_blob_client(output_file_name)

        await blob_client.upload_blob(data=processed_content.encode('latin1', errors='ignore'), overwrite=True)
        logger.info(f"Successfully uploaded the file: {output_file_name}")

        if local_download:
            content = await download_blob(blob_service_client, output_container, output_file_name)
            content = content.decode('latin1', errors='ignore')

            async with aiofiles.open(output_path+"/"+output_file_name, mode='w', encoding='latin1', errors="ignore") as file:
                await file.write(content)

        logger.info(f"{ErrorMessage.TARGET_SUCCESS.value} {output_file_path}")
        completed_on = get_current_utc_iso()
        log_summary = f"{ErrorMessage.TARGET_SUCCESS.value} {output_file_path}"

        # Add values to the log_details dictionary
        log_details["Target"]["StartedOn"] = started_on
        log_details["Target"]["Status"] = Status.SUCCESS.value
        log_details["Target"]["CompletedOn"] = completed_on
        log_details["LogSummary"] = log_summary

        # Send log details
        response = requests.post(error_log_url, json=log_details)
        if response.status_code == 200:
            print("Log entry inserted successfully.")
        else:
            print(f"Failed to insert log entry: {response.status_code} - {response.text}")

        return output_file_path
    except Exception as e:
        logger.error(f"{ErrorMessage.TARGET_FAILURE.value} {file_name}: {e}")
        log_summary = f"{ErrorMessage.TARGET_FAILURE.value} {file_name}: {e}"
        completed_on = get_current_utc_iso()

        # Add failure details to the log_details dictionary
        log_details["Target"]["StartedOn"] = started_on
        log_details["Target"]["Status"] = Status.FAILURE.value
        log_details["Target"]["CompletedOn"] = completed_on
        log_details["LogSummary"] = log_summary

        # Send failure log details
        response = requests.post(error_log_url, json=log_details)
        if response.status_code == 200:
            print("Log entry inserted successfully.")
        else:
            print(f"Failed to insert log entry: {response.status_code} - {response.text}")



def rename_duplicate_columns(df):
    # Convert all columns to lowercase
    df.columns = df.columns.str.lower()
    
    # Dictionary to keep track of the occurrence count of each column name
    col_counts = {}
    
    # List to store the new column names
    new_columns = []
    
    for col in df.columns:
        if col in col_counts:
            base_name = re.sub(r'\.\d+$', '', col)
            greatest_suffix = col_counts[base_name]
            newCol = base_name + greatest_suffix
            # If the column name already exists, increment the count and append the number to the name
            col_counts[col] += 1
            new_columns.append(f"{col}.{col_counts[col]}")
        else:
            # First occurrence of the column name, set count to 1 and use the original name
            col_counts[col] = 1
            new_columns.append(col)
    
    # Assign the new column names to the DataFrame
    df.columns = new_columns
    return df


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8004, log_level="info")

# Main execution logic
# input_directory = r'web-processing\io\input'  # Directory containing input files

# for filename in os.listdir(input_directory):
#     if filename.endswith('.txt'):
#         input_file_path = os.path.join(input_directory, filename)
#         process_file(input_file_path)

# fileNames = [ 'PORSCHEFLATFILE050424.txt' ]
# process_file_web(fileNames)