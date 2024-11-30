import pandas as pd
import numpy as np
from datetime import datetime
from logger import create_logger
from lookup.ManifestIN_lookup import *
from web_utilities import *
# import pyodbc
import configparser
from contextlib import closing



logger=create_logger(__name__)
#region ManifestIN Custom Rule
def ManifestIN_Custom_Rule(df_new, df_input):
    try:
        # HDR
        df_new['system'] = 'E'
        df_new['customer_id'] = df_new['customer_id'].str[-4:]
        df_new['pro_no'] = np.where(df_new['pro_no'] != '', df_new['pro_no'], df_new['master_bill_number'].where(df_new['master_bill_number'] != '', ''))
        df_new['ship_date'] = np.where(df_new['ship_date'] != '', df_new['ship_date'], df_new['invoice_date'])
        df_new['prepaid_collect'] = np.where(df_new['prepaid_collect'].str.upper() == "PREPAID", "PP", 
                                        np.where(df_new['prepaid_collect'].str.upper() == "COLLECT", "CC", df_new['prepaid_collect']))
        df_new['prepaid_collect'] = df_new['prepaid_collect'].str[:1]
        df_new['service_level'] = df_new.apply(map_service_level, axis=1)
        df_new['trailer_size'] = df_new.apply(map_trailer_size, axis=1)
        df_new['shipment_signed'] = df_new['shipment_signed'].str[:20]
        df_new['image_name'] = df_new.apply(map_image_name, axis=1)
        df_new['vat_reg_number'] = np.where(df_new['carrier_scac'] == 'BHS1', '', df_new['vat_reg_number'])
        
        createdby_carrier_scacs = ["SL34", "BHS1", "US09", "US11", "EA30", "DS38", "UP19", "KW16", "KWEI", "NE21", "SE20"]

        # Apply the conditional assignment
        df_new['created_by'] = np.where(df_new['carrier_scac'].isin(createdby_carrier_scacs), 'EDI', 'TRANISTICS')

        # SHP
        # First logic: If 'shipper_name' is not empty, convert to uppercase, otherwise map 'shipper_misc'
        df_new['shipper_name'] = np.where(df_new['shipper_name'] != '', 
                                        df_new['shipper_name'].str.upper(), 
                                        df_new['shipper_misc'].str.upper())

        # Second logic: If 'carrier_scac' is "SC38" and 'customer_id' is 5822, map 'shipper_misc' to 'shipper_name'
        df_new['shipper_name'] = np.where((df_new['carrier_scac'] == 'SC38') & (df_new['customer_id'] == '5822'), 
                                        df_new['shipper_misc'], 
                                        df_new['shipper_name'])

        df_new['shipper_address'] = df_new['shipper_address'].str.upper()
        df_new['shipper_city'] = df_new['shipper_city'].str.upper()
        df_new['shipper_state'] = df_new['shipper_state'].replace('', 'NS').str.upper()
        df_new['shipper_zip'] = df_new['shipper_zip'].replace('', 'NS').str.upper()
        df_new['shipper_country'] = df_new['shipper_country'].replace('', 'NS').str.upper()

        # CON
        # First logic: If 'consignee_name' is not empty, convert to uppercase, otherwise map 'consignee_misc'
        df_new['consignee_name'] = np.where(df_new['consignee_name'] != '', 
                                        df_new['consignee_name'].str.upper(), 
                                        df_new['consignee_misc'].str.upper())

        # Second logic: If 'carrier_scac' is "SC38" and 'customer_id' is 5822, map 'consignee_misc' to 'consignee_name'
        df_new['consignee_name'] = np.where((df_new['carrier_scac'] == 'SC38') & (df_new['customer_id'] == '5822'), 
                                        df_new['consignee_misc'], 
                                        df_new['consignee_name'])

        df_new['consignee_address'] = df_new['consignee_address'].str.upper()
        df_new['consignee_city'] = df_new['consignee_city'].str.upper()
        df_new['consignee_state'] = df_new['consignee_state'].replace('', 'NS').str.upper()
        df_new['consignee_zip'] = df_new['consignee_zip'].replace('', 'NS').str.upper()
        df_new['consignee_country'] = df_new['consignee_country'].replace('', 'NS').str.upper()

        # Replace empty strings in 'po_number' with 'NS'
        df_new['po_number'] = np.where(df_new['po_number'] == '', 'NS', df_new['po_number'])

        # Split 'po_number' based on both commas and colons
        po_split = df_new['po_number'].str.split(r'[,;:]', expand=True)

        # Assign each split value to a new column dynamically
        for idx, col in enumerate(po_split.columns):
            if idx == 0:
                df_new['po_number'] = po_split[col]  # First column remains as 'po_number'
                df_new['po_consignee_stop_sequence'] = '0'  # Default stop sequence
            else:
                df_new[f'po_number_{idx}'] = po_split[col]  # Additional columns as 'po_number_1', 'po_number_2', etc.
                df_new[f'po_consignee_stop_sequence_{idx}'] = '0'  # Default stop sequence for additional columns

        # BOL
        df_new['bol_number'] = df_new['po_number'].replace('', 'NS').str.upper()
        # Split 'bill_of_lading_number' based on both commas and colons
        bol_split = df_new['bill_of_lading_number'].str.split(r'[,;:]', expand=True)
        # Assign each split value to a new column dynamically
        for idx, col in enumerate(bol_split.columns):
            if idx == 0:
                df_new['bill_of_lading_number'] = bol_split[col]  # First column remains as 'bill_of_lading_number'
                df_new['bol_shipper_stop_sequence'] = '0'  # Default stop sequence               
            else:
                df_new[f'bill_of_lading_number_{idx}'] = bol_split[col]  # Additional columns as 'bill_of_lading_number_1', 'bill_of_lading_number_2', etc.
                df_new[f'bol_shipper_stop_sequence_{idx}'] = '0'  # Default stop sequence for additional columns   

        # ITM
        df_new['line_item_number'] = '1'

        df_new['no_pieces'] = pd.to_numeric(df_new['no_pieces'], errors='coerce').fillna(0).astype(int)
        df_new['no_pieces'] = np.where(df_new['no_pieces'] > 1, np.round(df_new['no_pieces']), '1').astype(str)

        df_new['item_weight'] = pd.to_numeric(df_new['item_weight'], errors='coerce')
        df_new['actual_weight'] = pd.to_numeric(df_new['actual_weight'], errors='coerce')

        df_new['item_weight'] = np.where(
            df_new['item_weight'] > 1,
            df_new['item_weight'].apply(lambda x: str(x).rstrip('0').rstrip('.')),
            np.where(
                df_new['actual_weight'] > 1,
                df_new['actual_weight'].apply(lambda x: str(x).rstrip('0').rstrip('.')),
                '1'
            )
        )
        
        df_new['bill_qty'] = pd.to_numeric(df_new['bill_qty'], errors='coerce').fillna(0)
        df_new['bill_qty'] = df_new['bill_qty'].apply(lambda x: str(x).rstrip() if x > 0 else x)

        df_new['bill_qty_uom'] = np.where(
            df_new['bill_qty_uom'] == 'CBM', 
            'CM', 
            df_new['bill_qty_uom']
        )

        df_new['uom'] = np.where(df_new['uom'] == 'K', 'KG',
                                np.where(df_new['uom'] == 'KGS', 'KG',
                                        np.where(df_new['uom'] == 'LBS', 'LB',
                                                df_new['uom'].str.upper().fillna(''))))
        
        df_new['weight_uom'] = np.where(df_new['weight_uom'] == 'K', 'KG',
                                np.where(df_new['weight_uom'] == 'KGS', 'KG',
                                        np.where(df_new['weight_uom'] == 'LBS', 'LB',
                                                df_new['weight_uom'].str.upper().fillna(''))))
                                                
        df_new['actual_weight'] = pd.to_numeric(df_new['actual_weight'], errors='coerce')
        df_new['item_weight'] = pd.to_numeric(df_new['item_weight'], errors='coerce')

        df_new['actual_weight'] = np.where(
            df_new['actual_weight'] > 1,
            df_new['actual_weight'].apply(lambda x: str(x).rstrip('0').rstrip('.')),
            np.where(
                df_new['item_weight'] > 1,
                df_new['item_weight'].apply(lambda x: str(x).rstrip('0').rstrip('.')),
                '1'
            )
        )
        

        # MSC
        msc_carrier_scacs = ["BHS1", "KW16", "KWEI"]
        df_new['udf_1'] = np.where(df_new['carrier_scac'].isin(msc_carrier_scacs), 'M90009887L', df_new['udf_1'])

        df_new['udf_2'] = np.where(df_new['carrier_scac'] == 'EA30', df_new['shipper_port_code'] + '-' + df_new['consignee_port_code'], df_new['udf_2'])


        # Accessorials
        if df_new['carrier_scac'].isin(['BHS1', 'EA30', 'KWEI']).any():
            acc_names = ["tax code", "accessorial charge", "accessorial type"]
            df_rearrange_acc = rearrange_col("AU", "DB", acc_names, df_input)
            process_accessorials(df_new, df_rearrange_acc)
        else:
            process_accessorials(df_new, df_input)




        # Tax
        process_tax(df_new, df_input)

        # FBM

        # Calculate total_invoice_amount
        df_new['billed_amount'] = pd.to_numeric(df_new['billed_amount'], errors='coerce')
        df_new['total_invoice_amount'] = df_new.groupby('invoice_number')['billed_amount'].transform('sum').round(2)

        # Calculate total_shipment_count
        df_new['total_shipment_count'] = df_new.groupby('invoice_number')['invoice_number'].transform('count')
        
        # EQT
        df_new['equipment_type'] = df_new.apply(map_equipment_type, axis=1)


        # logger.info(df_new['total_invoice_amount'])

        # Format Dates
        df_new['ship_date'] = df_new['ship_date'].apply(lambda x: convert_to_yyyymmdd(x, '%m/%d/%Y'))
        df_new['delivery_date'] = df_new['delivery_date'].str.strip().apply(lambda x: convert_to_yyyymmdd(x, '%m/%d/%Y'))
        df_new['pro_date'] = df_new['pro_date'].apply(lambda x: convert_to_yyyymmdd(x, '%m/%d/%Y'))
        df_new['interline_date'] = df_new['interline_date'].apply(lambda x: convert_to_yyyymmdd(x, '%m/%d/%Y'))
        df_new['guaranteed_date'] = df_new['guaranteed_date'].apply(lambda x: convert_to_yyyymmdd(x, '%m/%d/%Y'))
        df_new['invoice_date'] = df_new['invoice_date'].apply(lambda x: convert_to_yyyymmdd(x, '%m/%d/%Y'))
        df_new['master_bill_date'] = df_new['master_bill_date'].apply(lambda x: convert_to_yyyymmdd(x, '%m/%d/%Y'))



        return df_new
    except Exception as e:
        # Re-throw the exception to propagate it to the calling function
        raise


def map_service_level(row):
    # First check for the concatenation of Carrier_Code and Service_Level in Cross Ref 1
    key = f"{row['carrier_scac']}*{row['service_level']}"
    if key in service_level_cross_ref1:
        return service_level_cross_ref1[key]
    
    # If not found, check for Service_Level in Cross Ref 2
    if row['service_level'] in service_level_cross_ref2:
        return service_level_cross_ref2[row['service_level']]
    
    # If no match is found, return Service_Level in uppercase
    return row['service_level'].upper()

def map_trailer_size(row):
    # First check for the concatenation of Carrier_Code and EquipmentType in Trailer_Size_Cross_Ref
    key = f"{row['carrier_scac']}*{row['equipment_type']}"
    if key in trailer_size_cross_ref:
        return trailer_size_cross_ref[key]
    
    # If not found, check for EquipmentType in Trailer_Size_Cross_Ref2
    if row['equipment_type'] in trailer_size_cross_ref2:
        return trailer_size_cross_ref2[row['equipment_type']]
    
    # If no match is found, return EquipmentType in uppercase
    return row['equipment_type'].upper()

def map_image_name(row):
    # Handle SenderId-specific cases
    # if row['carrier_scac'] == "EA30":
    #     return row['image_name'] + ".pdf"
    if row['carrier_scac'] in ["DS38", "US09", "UP19", "NE21", "SE20"]:
        return ''    
    # Check for Image_Name prefixes and trim accordingly
    elif row['image_name'].startswith("TSI_"):
        return row['image_name'][4:]  # Trim "TSI_"
    elif row['image_name'].startswith("NVG_"):
        return row['image_name'][4:]  # Trim "NVG_"
    elif row['image_name'].startswith("FPI_"):
        return row['image_name'][4:]  # Trim "FPI_"
    elif row['image_name'].startswith("TEK_"):
        return row['image_name'][4:]  # Trim "TEK_"
    
    # Default case: return the original Image_Name
    return row['image_name']

# Define a custom function for the equipment_type mapping logic
def map_equipment_type(row):
    # Check for specific Carrier_Code and EquipmentType conditions
    if row['carrier_scac'] == 'BHS1':
        if row['equipment_type'] == 'VAN':
            return '98'
        elif row['equipment_type'] == '10':
            return '499'
        elif row['equipment_type'] == '24':
            return '232'
        elif row['equipment_type'] == '40':
            return '393'
    
    # If Carrier_Code is not "BHS1", check for EquipmentType in the Equipment Cross Ref table
    if row['equipment_type'] in equipment_cross_ref:
        return equipment_cross_ref[row['equipment_type']]
    
    # Default case: return EquipmentType as it is
    return row['equipment_type']


def process_accessorials(df_new, df_input):
    # Iterate through all columns of df_input
    tax_code_counter = 1  # For accessorial_carrier_tax_code
    for col in df_input.columns:
        if col.startswith('accessorial type'):
            # Determine corresponding charge column by replacing 'type' with 'charge'
            charge_col = col.replace('type', 'charge')
            acc_carr_tax_code_col = col.replace(col, f'tax code.{tax_code_counter}')
            
            if charge_col in df_input.columns:
                # Determine suffix for new columns based on index
                if col == 'accessorial type':
                    suffix = ''
                else:
                    # Extract the number from the column name if it exists
                    suffix = f"_{col.split('.')[-1]}" 
                
                # Assign new columns in df_new
                # First condition: Map based on "400", "AIR", "MIN"
                df_new[f'accessorial_charge{suffix}'] = np.where(df_input[col].isin(["400", "AIR", "MIN"]), "", df_input[col])

                # Second condition: If carrier_scac is 'BHS1' and accessorial is "ADM" and map to "LAA"
                df_new[f'accessorial_charge{suffix}'] = np.where(
                    (df_new['carrier_scac'] == 'BHS1') & (df_new[f'accessorial_charge{suffix}'] == "ADM"),
                    "LAA",
                    np.where(
                        df_new[f'accessorial_charge{suffix}'] == '405',
                        "FUE",
                        df_new[f'accessorial_charge{suffix}']
                    )
                )


                # Third condition: If accessorial is 'STR' then map to "TST"
                df_new[f'accessorial_charge{suffix}'] = np.where(df_new[f'accessorial_charge{suffix}'] == "STR", "TST", df_new[f'accessorial_charge{suffix}'])

                df_new[f'accessorial_charge_amount{suffix}'] = df_input[charge_col]    
                df_new[f'accessorial_carrier_tax_code{suffix}'] = df_input[acc_carr_tax_code_col]

                # Increment the tax code counter for the next accessorial type
                tax_code_counter += 1

    return df_new


def process_tax(df_new, df_input):
    # Iterate through all columns of df_input
    for col in df_input.columns:
        if col.startswith('tax type'):
            # Determine corresponding charge column by replacing 'type' with 'amount'
            tax_amount_col = col.replace('type', 'amount')
            iva_code_col = col.replace('tax type', 'iva code')
            taxable_base_amount_col = col.replace('tax type', 'taxable base amount')
            
            if tax_amount_col in df_input.columns:
                # Determine suffix for new columns based on index
                if col == 'tax type':
                    suffix = ''
                else:
                    # Extract the number from the column name if it exists
                    suffix = f"_{col.split('.')[-1]}" 
                
                # Assign new columns in df_new
                df_new[f'tax_type_code{suffix}'] = df_input[col]
                df_new[f'billed_tax_amount{suffix}'] = df_input[tax_amount_col]    
                df_new[f'base_tax_code{suffix}'] = df_input[iva_code_col]
                df_new[f'base_tax_amount{suffix}'] = df_input[taxable_base_amount_col]  

    return df_new



def fetch_customer_key_from_db(image_name, cursor):
    """
    Fetch customer_key from the database based on image_filename.
    """
    query = "SELECT customer_key FROM freight_bill_imaging (nolock) WHERE image_filename = ?"
    cursor.execute(query, (image_name,))
    row = cursor.fetchone()
    
    # Return the customer_key if found, else return None
    return row.customer_key if row else None

def map_receiver_id(df_new):
    if 'image name' in df_new.columns and not df_new.empty:
        image_name = df_new.iloc[0]['image name']
        customer_id = df_new.iloc[0]['customer_id']
        carrier_scac = df_new.iloc[0]['carrier_scac']

    # Read the configuration file
    config = configparser.ConfigParser()
    config.read('config.ini')

    print(f"Sections found in config file: {config.sections()}")
    
    # Get the connection parameters
    server = config['database']['server']
    database = config['database']['database']
    username = config['database']['username']
    password = config['database']['password']

    # Create a connection string
    connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

    # Connect to the database using context manager
    try:
        with pyodbc.connect(connection_string) as connection:
            with closing(connection.cursor()) as cursor:
                # Handle specific customer_id and carrier_scac cases
                if customer_id == "TSI5037" and carrier_scac == "SL34":
                    return "5037"
                elif customer_id == "TSI5822":
                    return "5822"
                elif customer_id == "TSI5810":
                    return "5810"

                # List of prefixes to trim
                prefixes = ["NVG_", "FPI_", "TSI_", "TEK_"]

                # Trim the prefix if it exists and query the database
                for prefix in prefixes:
                    if image_name.startswith(prefix):
                        trimmed_image_name = image_name[len(prefix):]  # Trim the prefix
                        return fetch_customer_key_from_db(trimmed_image_name, cursor)

                # Default case: query using the original image_name
                return fetch_customer_key_from_db(image_name, cursor)

    except pyodbc.Error as e:
        print(f"Database error occurred: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None


def fetch_carrier_short_name_from_db(image_name, cursor):
    """
    Fetch carrier_short_name from the database based on image_filename.
    """
    query = """
        SELECT carrier_short_name 
        FROM carrier 
        WHERE carrier_key IN (
            SELECT carrier_key 
            FROM freight_bill_imaging (nolock) 
            WHERE image_filename = ?
        )
    """
    cursor.execute(query, (image_name,))
    row = cursor.fetchone()
    
    # Return the carrier_short_name if found, else return None
    return row.carrier_short_name if row else None

def map_sender_id(df_new):
    if 'SenderId' in df_new.columns and not df_new.empty:
        sender_id = df_new.iloc[0]['SenderId']
        image_name = df_new.iloc[0]['image name']

    # Read the configuration file
    config = configparser.ConfigParser()
    config.read('config.ini')

    print(f"Sections found in config file: {config.sections()}")
    
    # Get the connection parameters
    server = config['database']['server']
    database = config['database']['database']
    username = config['database']['username']
    password = config['database']['password']

    # Create a connection string
    connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

    # Connect to the database using context manager
    try:
        with pyodbc.connect(connection_string) as connection:
            with closing(connection.cursor()) as cursor:
                # If SenderId is not NVGIND, return SenderId
                if sender_id != "NVGIND":
                    return sender_id

                # If SenderId is NVGIND, check for prefixes and trim the image_name before querying
                prefixes = ["NVG_", "FPI_", "TSI_", "TEK_"]
                for prefix in prefixes:
                    if image_name.startswith(prefix):
                        trimmed_image_name = image_name[len(prefix):]  # Trim the prefix
                        return fetch_carrier_short_name_from_db(trimmed_image_name, cursor)

                # Default case: query using the original image_name
                return fetch_carrier_short_name_from_db(image_name, cursor)

    except pyodbc.Error as e:
        print(f"Database error occurred: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None
 #endregion