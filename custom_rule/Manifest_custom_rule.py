import pandas as pd
import numpy as np
from datetime import datetime
from logger import create_logger
from lookup.Manifest_lookup import *
from web_utilities import *
# import pyodbc
import configparser
from pathlib import Path


base_dir = str(Path(__file__).parent.parent.resolve())

logger=create_logger(__name__)
#region Manifest Custom Rule
def Manifest_Custom_Rule(df_new, df_input):
    try:    
        # HDR
        df_new['system'] = 'E'
        df_new['customer_id'] = '5810'
        df_new['pro_no'] = np.where(df_new['pro_no'].notna() & (df_new['pro_no'] != ''), 
                                            df_new['pro_no'], 
                                            df_new['master_bill_number'])
        df_new['ship_date'] = np.where(df_new['ship_date'] != '', df_new['ship_date'], df_new['invoice_date'])
        df_new['billed_amount'] = df_new['billed_amount']
        df_new['discount_amount'] = '0'
        df_new['discount_percent'] = '0'
        df_new['prepaid_collect'] = df_new['prepaid_collect'].map(Manifest_Payment_CrossRef_lookup).fillna(df_new['prepaid_collect'])
        df_new['prepaid_collect'] = df_new['prepaid_collect'].str[:1]
        df_new['cod_amount'] = '0'
        df_new['service_level'] = df_new['service_level'].map(Manifest_service_lookup).fillna(df_new['service_level'])
        df_new['equipment_type'] = np.where(df_new['equipment_type'] != '', df_new['equipment_type'].str.upper(), '0')
        df_new['exchange_rate'] = '0'
        df_new['shipment_signed'] = df_new['shipment_signed'].str[:20]

        
        edi_carrier_scacs = ["SL34", "US09", "DS38", "UP19", "NE21", "SE20"]

        # Apply the conditional assignment
        df_new['created_by'] = np.where(df_new['carrier_scac'].isin(edi_carrier_scacs), 'EDI', 'TRANISTICS')

        # SHP
        df_new['shipper_name'] = np.where(df_new['shipper_name'] != '', df_new['shipper_name'].str.upper(), df_new['shipper_misc'].str.upper())
        df_new['shipper_address'] = df_new['shipper_address'].str.upper()
        df_new['shipper_city'] = df_new['shipper_city'].str.upper()
        df_new['shipper_state'] = df_new['shipper_state'].replace('', 'NS').str.upper()
        df_new['shipper_zip'] = df_new['shipper_zip'].replace('', 'NS').str.upper()
        df_new['shipper_country'] = df_new['shipper_country'].replace('', 'NS').str.upper()

        # CON
        df_new['consignee_name'] = np.where(
        df_new['consignee_name'] != '', df_new['consignee_name'].str.upper(), df_new['consignee_misc'])
        df_new['consignee_address'] = df_new['consignee_address'].str.upper()
        df_new['consignee_city'] = df_new['consignee_city'].str.upper()
        df_new['consignee_state'] = df_new['consignee_state'].replace('', 'NS').str.upper()
        df_new['consignee_zip'] = df_new['consignee_zip'].replace('', 'NS').str.upper()
        df_new['consignee_country'] = df_new['consignee_country'].replace('', 'NS').str.upper()

        # PUR
        df_new['po_number'] = df_new['po_number'].replace('', 'NS').str.upper()

        # BOL
        df_new['bol_number'] = df_new['po_number'].replace('', 'NS').str.upper()

        # ITM
        df_new['line_item_number'] = '1'

        df_new['no_pieces'] = pd.to_numeric(df_new['no_pieces'], errors='coerce').fillna(0).astype(int)
        df_new['no_pieces'] = np.where(df_new['no_pieces'] > 1, np.round(df_new['no_pieces']), '1').astype(str)

        df_new['item_weight'] = pd.to_numeric(df_new['item_weight'], errors='coerce')
        df_new['item_weight'] = np.where(
            df_new['item_weight'] > 1,
            df_new['item_weight'].astype(str).str.rstrip('0').str.rstrip('.'),
            '0'
        )
        
        df_new['bill_qty'] = df_new['bill_qty'].fillna('0')

        df_new['bill_qty_uom'] = np.where(
            df_new['bill_qty_uom'] == 'CBM', 
            'CM', 
            df_new['bill_qty_uom']
        )

        df_new['liquid_volume']  ='0'

        df_new['length'] = np.where(df_new['length'] != '', df_new['length'], '0')
        df_new['length'] = np.abs(pd.to_numeric(df_new['length'], errors='coerce').fillna(0))

        df_new['width'] = np.where(df_new['width'] != '', df_new['width'], '0')
        df_new['width'] = np.abs(pd.to_numeric(df_new['width'], errors='coerce').fillna(0))

        df_new['height'] = np.where(df_new['height'] != '', df_new['height'], '0')
        df_new['height'] = np.abs(pd.to_numeric(df_new['height'], errors='coerce').fillna(0))

        df_new['dimensional_weight'] = df_new['dimensional_weight'].replace('', '0')

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
        df_new['item_weight'] = np.where(
            df_new['actual_weight'] > 1,
            df_new['actual_weight'].apply(lambda x: str(x).rstrip('0').rstrip('.') if not pd.isna(x) else x),
            np.where(
                df_new['item_weight'] > 1,
                df_new['item_weight'].apply(lambda x: str(x).rstrip('0').rstrip('.') if not pd.isna(x) else x),
                '1'
            )
        )


        # Accessorials
        process_accessorials(df_new, df_input)

        # Tax
        process_tax(df_new, df_input)

        # FBM

        # Calculate total_invoice_amount
        df_new['total_invoice_amount'] = df_new.groupby('invoice_number')['billed_amount'].transform('sum').round(2)

        # Calculate total_shipment_count
        df_new['total_shipment_count'] = df_new.groupby('invoice_number')['invoice_number'].transform('count')
        
        # EQT
        df_new['equipment_type'] = df_new['equipment_type'].map(Manifest_Equipment_Xref_lookup).fillna('')


        # logger.info(df_new['total_invoice_amount'])

        # Format Dates
        df_new['ship_date'] = df_new['ship_date'].apply(lambda x: convert_to_yyyymmdd(x, '%m/%d/%Y'))
        df_new['delivery_date'] = df_new['delivery_date'].apply(lambda x: convert_to_yyyymmdd(x, '%m/%d/%Y'))
        df_new['pro_date'] = df_new['pro_date'].apply(lambda x: convert_to_yyyymmdd(x, '%m/%d/%Y'))
        df_new['interline_date'] = df_new['interline_date'].apply(lambda x: convert_to_yyyymmdd(x, '%m/%d/%Y'))
        df_new['guaranteed_date'] = df_new['guaranteed_date'].apply(lambda x: convert_to_yyyymmdd(x, '%m/%d/%Y'))
        df_new['invoice_date'] = df_new['invoice_date'].apply(lambda x: convert_to_yyyymmdd(x, '%m/%d/%Y'))
        df_new['master_bill_date'] = df_new['master_bill_date'].apply(lambda x: convert_to_yyyymmdd(x, '%m/%d/%Y'))

        return df_new
    except Exception as e:
        # Re-throw the exception to propagate it to the calling function
        raise

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
                df_new[f'accessorial_charge{suffix}'] = np.where(df_input[col].isin(["400", "AIR", "MIN"]),"", df_input[col])
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


def get_customer_id(df_input):

    if 'image name' in df_input.columns and not df_input.empty:
        image_name = df_input.iloc[0]['image name']

    # Read the configuration file
    config = configparser.ConfigParser()
    config.read(base_dir + '/config.ini')

    print(f"Sections found in config file: {config.sections()}")
    
    # Get the connection parameters
    server = config['database']['server']
    database = config['database']['database']
    username = config['database']['username']
    password = config['database']['password']

    # Create a connection string
    connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

    # Connect to the database
    connection = pyodbc.connect(connection_string)

    try:
        
        # Create a cursor object
        cursor = connection.cursor()

        query = "SELECT customer_key FROM freight_bill_imaging (nolock) WHERE image_filename = ?"
        cursor.execute(query, (image_name,))

        # Fetch the first result
        row = cursor.fetchone()

        # Extract and return the customerid
        if row:
            return row.customer_key
        else:
            return None

    except Exception as e:
        print(f"An error occurred: {e}")
        return None

    finally:
        # Close the connection
        cursor.close()
        connection.close()

def get_carrier_scac(df_input):

    if 'image name' in df_input.columns and not df_input.empty:
        image_name = df_input.iloc[0]['image name']

    # Read the configuration file
    config = configparser.ConfigParser()
    config.read(base_dir + '/config.ini')

    # Get the connection parameters
    server = config['database']['server']
    database = config['database']['database']
    username = config['database']['username']
    password = config['database']['password']

    # Create a connection string
    connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

    # Connect to the database
    connection = pyodbc.connect(connection_string)

    try:
        
        # Create a cursor object
        cursor = connection.cursor()

        query = (
            "SELECT carrier_short_name FROM carrier "
            "WHERE carrier_key IN (SELECT carrier_key FROM freight_bill_imaging WHERE image_filename = ?)"
        )

        cursor.execute(query, (image_name,))

        # Fetch the first result
        row = cursor.fetchone()

        # Extract and return the customerid
        if row:
            return row.carrier_short_name
        else:
            return None

    except Exception as e:
        print(f"An error occurred: {e}")
        return None

    finally:
        # Close the connection
        cursor.close()
        connection.close()
 #endregion