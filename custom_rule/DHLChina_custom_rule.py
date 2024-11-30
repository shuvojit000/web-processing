import pandas as pd
import numpy as np
from datetime import datetime
from logger import create_logger
from lookup.DHLChina_lookup import *
from web_utilities import *


logger=create_logger(__name__)
#region DHL China Custom Rule
def DhlChina_Custom_Rule(df_new, df_input):
    try:
        # HDR
        df_new['system'] = 'E'
        df_new['customer_id'] = '5920'
        df_new['carrier_scac'] = 'DH78'
        df_new['pro_no'] = np.where(df_new['pro_no'] != '', df_new['pro_no'], df_new['invoice_number'])
        df_new['billed_amount'] = df_new['billed_amount']
        df_new['prepaid_collect'] = 'P'
        df_new['service_level'] = 'TL'
        df_new['currency_code'] = 'USD'
        df_new['created_by'] = 'EDI'

        # SHP
        df_new['shipper_name'] = np.where(df_new['shipper_name'] != '', df_new['shipper_name'].str.upper(), 'NS')
        df_new['shipper_address'] = np.where(df_new['shipper_address'] != '', df_new['shipper_address'].str.upper(), 'NS')
        df_new['shipper_city'] = df_new['shipper_city'].map(DHLChina_Shipper_City_Xref_lookup).fillna(df_new['shipper_city']).fillna('NS')
        df_new['shipper_state'] = np.where(df_new['shipper_state'] != '', df_new['shipper_state'].str.upper(), 'NS')
        df_new['shipper_zip'] = np.where(df_new['shipper_zip'] != '', df_new['shipper_zip'], 'NS')
        df_new['shipper_stop_sequence'] = '01'
        df_new['shipper_country'] = df_new['shipper_country'].map(DHLChina_Shipper_Country_Xref_lookup).fillna(df_new['shipper_country']).fillna('NS')

        # CON
        df_new['consignee_name'] = np.where(df_new['consignee_name'] != '', df_new['consignee_name'].str.upper(), 'NS')
        df_new['consignee_address'] = np.where(df_new['consignee_address'] != '', df_new['consignee_address'].str.upper(), 'NS')
        df_new['consignee_city'] = df_new['consignee_city'].map(DHLChina_Shipper_City_Xref_lookup).fillna('NS')
        df_new['consignee_state'] = np.where(df_new['consignee_state'] != '', df_new['consignee_state'].str.upper(), 'NS')
        df_new['consignee_zip'] = np.where(df_new['consignee_zip'] != '', df_new['shipper_zip'], 'NS')
        df_new['consignee_stop_sequence'] = '01'
        df_new['consignee_country'] = df_new['consignee_country'].map(DHLChina_Shipper_City_Xref_lookup).fillna(df_new['consignee_country']).fillna('NS')

        # ITM
        df_new['line_item_number'] = '1'
        df_new['no_pieces'] = pd.to_numeric(df_new['no_pieces'], errors='coerce').fillna(0).astype(int)
        df_new['no_pieces'] = np.where(df_new['no_pieces'] > 1, df_new['no_pieces'], '1').astype(str)
        # df_new['item_weight'] = pd.to_numeric(df_new['item_weight'], errors='coerce').fillna(0).astype(float)
        df_new['bill_qty'] = np.where(pd.to_numeric(df_new['bill_qty'], errors='coerce').fillna(0).astype(float) < 50, 
                                pd.to_numeric(df_new['bill_qty'], errors='coerce').fillna(0).astype(float) + 1, 
                                pd.to_numeric(df_new['bill_qty'], errors='coerce').fillna(0).astype(float))
        
        df_new['bill_qty'] = df_new['bill_qty'].apply(format_decimal_two_places)

        df_new['bill_qty_uom'] = 'KG'
        df_new['uom'] = 'KG'
        df_new['actual_weight'] = df_new['actual_weight'].fillna(1)
        df_new['container_size'] = df_new['container_size'].map(DHLChina_Equipment_Xref_lookup).fillna('')



        # FBM
        df_new['fbm_equipment_number'] = df_new['fbm_equipment_number'].map(DHLChina_Equipment_Xref_lookup).fillna('')

        # FSP
        df_new['shipper_port_country_code'] = df_new['shipper_port_country_code'].map(DHLChina_Shipper_Country_Xref_lookup).fillna('')

        # EQT
        df_new['equipment_type'] = df_new['equipment_type'].map(DHLChina_Equipment_Xref_lookup).fillna('')

        # PO
        df_new['po_number'] = 'NS'
        # BOL
        process_bol(df_new)

        # Calculate total_invoice_amount
        df_new['total_invoice_amount'] = df_new.groupby('invoice_number')['billed_amount'].transform('sum').round(2)

        # Calculate total_shipment_count
        df_new['total_shipment_count'] = df_new.groupby('invoice_number')['invoice_number'].transform('count')

        # logger.info(df_new['total_invoice_amount'])

        # Accessorials
        process_accessorials(df_new, df_input)

        # TAX
        process_tax(df_new, df_input)

        # Format Dates
        df_new['ship_date'] = df_new['ship_date'].apply(lambda x: convert_to_yyyymmdd(x, '%m/%d/%Y'))
        df_new['delivery_date'] = df_new['delivery_date'].apply(lambda x: convert_to_yyyymmdd(x, '%m/%d/%Y'))
        df_new['pro_date'] = df_new['pro_date'].apply(lambda x: convert_to_yyyymmdd(x, '%m/%d/%Y'))
        df_new['interline_date'] = df_new['interline_date'].apply(lambda x: convert_to_yyyymmdd(x, '%m/%d/%Y'))
        df_new['guaranteed_date'] = df_new['guaranteed_date'].apply(lambda x: convert_to_yyyymmdd(x, '%m/%d/%Y'))
        df_new['invoice_date'] = df_new['invoice_date'].apply(lambda x: convert_to_yyyymmdd(x, '%m/%d/%Y'))
        df_new['master_bill_date'] = df_new['master_bill_date'].apply(lambda x: convert_to_yyyymmdd(x, '%m/%d/%Y'))

        df_new['billed_amount'] = pd.to_numeric(df_new['billed_amount'], errors='coerce')

        # Filter out rows where 'billed_amount' is 0
        df_new = df_new[df_new['billed_amount'] != 0]


        return df_new
    except Exception as e:
        # Re-throw the exception to propagate it to the calling function
        raise

# Function to split bill of lading numbers and reassign them
def process_bol(df):
    # Split the column values by comma
    split_values = df['bill_of_lading_number'].apply(lambda x: x.split(','))

    # Assign the first value back to the original column
    df['bill_of_lading_number'] = split_values.apply(lambda x: x[0] if len(x) > 0 else '')

    # Assign subsequent values to new columns
    for i in range(1, max(split_values.apply(len))):
        df[f'bill_of_lading_number_{i}'] = split_values.apply(lambda x: x[i] if len(x) > i else '')
        df[f'shipper_stop_sequence_{i}'] = '01'

    return df


def process_accessorials(df_new, df_input):
    # Iterate through each row of df_input using df_input.iterrows()
    for idx, row in df_input.iterrows():
        # Initialize suffix counter
        suffix_counter = 0

        # Iterate through all columns in the row
        for col in df_input.columns:
            col_cleaned = clean_column_header_name(col)

            # Find the matching key in the lookup dictionary
            matching_key = next((key for key in DHLChina_Accessorials_lookup if col_cleaned == key.lower()), None)

            if matching_key:
                # Determine corresponding accessorial code from the lookup dictionary
                accessorial_code = DHLChina_Accessorials_lookup[matching_key]
                
                # Set suffix to indicate the index for additional accessorial charges
                suffix = f"_{suffix_counter}" if suffix_counter > 0 else ''

                # Assign new columns in df_new
                df_new.loc[idx, f'accessorial_charge{suffix}'] = accessorial_code
                df_new.loc[idx, f'accessorial_charge_amount{suffix}'] = row[col]

                # Increment suffix counter for the next charge in the same row
                suffix_counter += 1

    return df_new


def process_tax(df_new, df_input):
    # Iterate through each row of df_input using df_input.iterrows()
    for idx, row in df_input.iterrows():
        # Initialize suffix counter
        suffix_counter = 0

        # Iterate through all columns in the row
        for col in df_input.columns:
            col_cleaned = clean_column_header_name(col)

            # Find the matching key in the lookup dictionary
            matching_key = next((key for key in DHL_China_Tax_lookup if col_cleaned == key.lower()), None)

            if matching_key:
                # Determine corresponding accessorial code from the lookup dictionary
                tax_code = DHL_China_Tax_lookup[matching_key]
                
                # Set suffix to indicate the index for additional accessorial charges
                suffix = f"_{suffix_counter}" if suffix_counter > 0 else ''

                # Assign new columns in df_new
                df_new.loc[idx, f'tax_type_code{suffix}'] = tax_code
                df_new.loc[idx, f'billed_tax_amount{suffix}'] = row[col]
                df_new.loc[idx, f'base_tax_amount{suffix}'] = 0

                # Increment suffix counter for the next charge in the same row
                suffix_counter += 1

    return df_new



 #endregion