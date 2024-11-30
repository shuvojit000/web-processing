import pandas as pd
import pyodbc
import asyncio
import schedule
import configparser
import logging
import os
import datetime
from pathlib import Path
from process_data import *
from custom_rule.WebCustomer_custom_rule import *
from custom_rule.WebCarrier_custom_rule import *

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

base_dir = str(Path(__file__).parent.resolve())
customer_mapping_file = os.path.join(base_dir, "web-mapping.xlsx")

# Load configuration from config.ini
def load_config():
    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    config = configparser.ConfigParser()
    try:
        config.read(config_path)
        if not config.sections():
            raise ValueError("No sections found in the configuration file.")
        logger.info("Configuration loaded successfully.")
        return config
    except (FileNotFoundError, configparser.Error, ValueError) as e:
        logger.error(f"Configuration loading error: {e}")
        exit(1)

# Initialize configuration
config = load_config()
output_path = config['Directories']['output_dir']

# Database connection setup
def get_sql_connection():
    try:
        db_config = config['Database']
        return pyodbc.connect(
            f"Driver={{SQL Server}};"
            f"Server={db_config.get('server')};"
            f"Database={db_config.get('database')};"
            f"UID={db_config.get('username')};"
            f"PWD={db_config.get('password')};"
        )
    except pyodbc.Error as e:
        logger.error(f"Database connection error: {e}")
        return None


# Main function to execute SQL query and load data into DataFrame
async def main():
    # process_customer()
    process_carrier()

def process_customer():
    query = "select * from vw_WebCustomer (nolock) where customer_freight_bill_key in (18678,18677,18676,18675,18674,18673,18672,18671,18670,18669)"
    connection = get_sql_connection()
    if connection:
        try:
            inputdf = pd.read_sql(query, connection, dtype=str).fillna('')
            logger.info("Data loaded from database.")

            # Load the customer mapping file and get required columns
            mapping_df = pd.read_excel(customer_mapping_file, usecols=["Fields"])
            required_columns = mapping_df["Fields"].tolist()

            # Find missing columns and add them with blank values
            missing_columns = [col for col in required_columns if col not in inputdf.columns]
            for col in missing_columns:
                inputdf[col] = ""

            logger.info(f"Added missing columns: {missing_columns}")

            processed_rows = process_web_customer(inputdf)

            # Convert processed rows to DataFrame
            processed_df = pd.DataFrame(processed_rows)
            logger.info("Distinct accessorial charges, taxes, equipment details, and PO details processed.")

            # Process each row in the processed DataFrame
            for _, row in processed_df.iterrows():
                try:
                    formatted_rows = []
                    formatted_rows = process_web(row, row.name, formatted_rows)

                    # Prepare the output file name
                    current_datetime = datetime.now().strftime("%Y%m%d%H%M%S%f")
                    output_file_name = f"WebWeb_customer_Web_customer_{current_datetime}_processed.txt"
                    output_file_path = os.path.join(output_path, output_file_name)

                    # Write processed content to the output file
                    processed_content = '\n'.join(formatted_rows)
                    with open(output_file_path, 'w') as f:
                        f.write(processed_content)
                    logger.info(f"Processed content written to file: {output_file_path}")

                    # Update the status for the corresponding customer_freight_bill_key
                    customer_freight_bill_key = row['customer_freight_bill_key']
                    # update_freight_bill_status_customer(connection, customer_freight_bill_key)

                except Exception as e:
                    logger.error(f"Data processing error: {e}")
        except Exception as e:
            logger.error(f"SQL query or processing error: {e}")
        finally:
            connection.close()

def process_carrier():
    query = "select * from vw_WebCarrier (nolock) where carrier_freight_bill_key in (531990)"
    connection = get_sql_connection()
    if connection:
        try:
            inputdf = pd.read_sql(query, connection, dtype=str)
            logger.info("Data loaded from database.")

            # Load the customer mapping file and get required columns
            mapping_df = pd.read_excel(customer_mapping_file, usecols=["Fields"])
            required_columns = mapping_df["Fields"].tolist()

            # Find missing columns and add them with blank values
            missing_columns = [col for col in required_columns if col not in inputdf.columns]
            for col in missing_columns:
                inputdf[col] = ""

            logger.info(f"Added missing columns: {missing_columns}")

            processed_rows = process_web_carrier(inputdf)

            # Convert processed rows to DataFrame
            processed_df = pd.DataFrame(processed_rows)
            logger.info("Distinct accessorial charges, taxes, equipment details, and PO details processed.")

            # Process each row in the processed DataFrame
            for _, row in processed_df.iterrows():
                try:
                    formatted_rows = []
                    formatted_rows = process_web(row, row.name, formatted_rows)

                    # Prepare the output file name
                    current_datetime = datetime.now().strftime("%Y%m%d%H%M%S%f")
                    output_file_name = f"WebWeb_carrier_Web_carrier_{current_datetime}.txt"
                    output_file_path = os.path.join(output_path, output_file_name)

                    # Write processed content to the output file
                    processed_content = '\n'.join(formatted_rows)
                    with open(output_file_path, 'w') as f:
                        f.write(processed_content)
                    logger.info(f"Processed content written to file: {output_file_path}")

                    # Update the status for the corresponding customer_freight_bill_key
                    carrier_freight_bill_key = row['carrier_freight_bill_key']
                    # update_freight_bill_status_carrier(connection, carrier_freight_bill_key)

                except Exception as e:
                    logger.error(f"Data processing error: {e}")
        except Exception as e:
            logger.error(f"SQL query or processing error: {e}")
        finally:
            connection.close()

# Function to update the status of a customer_freight_bill_key in the database
def update_freight_bill_status_customer(connection, customer_freight_bill_key):
    try:
        update_status_query = """
            UPDATE customer_freight_bill
            SET status = 'A'
            WHERE customer_freight_bill_key = ?
        """
        cursor = connection.cursor()
        cursor.execute(update_status_query, (customer_freight_bill_key,))
        connection.commit()
        logger.info(f"Updated status to 'A' for customer_freight_bill_key: {customer_freight_bill_key}")
    except Exception as e:
        logger.error(f"Error updating status for customer_freight_bill_key {customer_freight_bill_key}: {e}")

# Function to update the status of a carrier_freight_bill_key in the database
def update_freight_bill_status_carrier(connection, carrier_freight_bill_key):
    try:
        update_status_query = """
            UPDATE carrier_freight_bill
            SET status = 'A'
            WHERE carrier_freight_bill_key = ?
        """
        cursor = connection.cursor()
        cursor.execute(update_status_query, (carrier_freight_bill_key,))
        connection.commit()
        logger.info(f"Updated status to 'A' for carrier_freight_bill_key: {carrier_freight_bill_key}")
    except Exception as e:
        logger.error(f"Error updating status for carrier_freight_bill_key {carrier_freight_bill_key}: {e}")


# Scheduler wrapper for asynchronous main function
def run_scheduler():
    asyncio.run(main())

# Entry point to initiate schedule
if __name__ == "__main__":
    schedule.every(10).seconds.do(run_scheduler)

    while True:
        schedule.run_pending()
        time.sleep(1)
