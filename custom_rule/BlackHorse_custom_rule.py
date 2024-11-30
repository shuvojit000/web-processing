import pandas as pd
import numpy as np
from datetime import datetime
from web_utilities import *

#region BlackHorse Customer Rule
def BlackHorse_Custom_Rule(df_new, df_input):
    try:
        # HDR
        df_new['system'] = 'E'
        df_new['customer_id'] = '5092'
        df_new['carrier_scac'] = 'PNSK'
        df_new['pro_no'] = np.where(df_new['pro_no'] != '', df_new['pro_no'], df_new['master_bill_number'])
        df_new['ship_date'] = np.where(df_new['ship_date'] != '', df_new['ship_date'], df_new['invoice_date'])
        df_new['discount_amount'] = '0'
        df_new['prepaid_collect'] = 'P'
        df_new['cod_amount'] = '0'
        df_new['service_level'] = 'LTL'
        df_new['hdr_accounting_code'] = '0'

        # SHP
        df_new['shipper_name'] = df_new['shipper_name'].str[:30].str.upper()
        df_new['shipper_address'] = df_new['shipper_address'].str[:35].str.upper()
        df_new['shipper_city'] = np.where(df_new['shipper_city'] == 'ST. LOUIS', 'SAINT LOUIS', df_new['shipper_city'].str[:25].str.upper())
        df_new['shipper_state'] = df_new['shipper_state'].str.upper() 
        df_new['shipper_country'] = df_new['shipper_country'].str.upper() 

        # CON
        df_new['consignee_name'] = df_new['consignee_name'].str.upper()
        df_new['consignee_address'] = df_new['consignee_address'].str[:35].str.upper()
        df_new['consignee_city'] = np.where(df_new['consignee_city'] == 'ST. LOUIS', 'SAINT LOUIS', df_new['consignee_city'].str[:25].str.upper())
        df_new['consignee_state'] = df_new['consignee_state'].str.upper() 
        df_new['consignee_country'] = df_new['consignee_country'].str[:3].str.upper() 

        # PUR
        df_new['po_number'] = 'NS' 

        # BOL
        df_new['bill_of_lading_number'] = np.where(df_new['bill_of_lading_number'] != '', df_new['bill_of_lading_number'].str[:18], 'NS')

        # ITM
        df_new['line_item_number'] = '1'
        df_new['item_weight'] = np.where(df_new['item_weight'] != '', df_new['item_weight'], '0')
        df_new['bill_qty'] = np.where(df_new['bill_qty'] != '', df_new['bill_qty'], '0')
        df_new['liquid_volume'] = '0' 
        df_new['length'] = '00000000' 
        df_new['width'] = '00000000' 
        df_new['height'] = '00000000'
        df_new['dim_uom'] = np.where((df_new['weight_uom'] == 'KG') | (df_new['weight_uom'] == 'K'), 'C', 'N')
        df_new['dimensional_weight'] = '0'

        # ACC

        # Update df_new with accessorials from df_input
        # df_new['accessorial_charge'] = np.where(df_new['accessorial_charge'] == 'FFS', 'FUE', df_new['accessorial_charge'])
        df_new = process_accessorials(df_new, df_input)

        # Trim spaces and convert to float
        df_new['billed_amount'] = df_new['billed_amount'].str.rstrip().astype(float)

        # Calculate total_invoice_amount
        df_new['total_invoice_amount'] = df_new.groupby('invoice_number')['billed_amount'].transform('sum').round(2)

        # Calculate total_shipment_count
        df_new['total_shipment_count'] = df_new.groupby('invoice_number')['invoice_number'].transform('count')

        # Format Dates
        df_new['ship_date'] = df_new['ship_date'].apply(lambda x: convert_to_yyyymmdd(x, '%m/%d/%Y'))
        df_new['delivery_date'] = df_new['delivery_date'].apply(lambda x: convert_to_yyyymmdd(x, '%m/%d/%Y'))
        df_new['pro_date'] = df_new['pro_date'].apply(lambda x: convert_to_yyyymmdd(x, '%m/%d/%Y'))
        df_new['interline_date'] = df_new['interline_date'].apply(lambda x: convert_to_yyyymmdd(x, '%m/%d/%Y'))
        df_new['guaranteed_date'] = df_new['guaranteed_date'].apply(lambda x: convert_to_yyyymmdd(x, '%m/%d/%Y'))
        df_new['invoice_date'] = df_new['invoice_date'].apply(lambda x: convert_to_yyyymmdd(x, '%m/%d/%Y'))
        df_new['master_bill_date'] = df_new['master_bill_date'].apply(lambda x: convert_to_yyyymmdd(x, '%m/%d/%Y'))

        # Remove rows where billed_amount is 0
        df_new = df_new[df_new['billed_amount'] != 0]

        return df_new
    except Exception as e:
        # Re-throw the exception to propagate it to the calling function
        raise
def process_accessorials(df_new, df_input):
    # Iterate through all columns of df_input
    for col in df_input.columns:
        if col.startswith('accessorial type'):
            # Determine corresponding charge column by replacing 'type' with 'charge'
            charge_col = col.replace('type', 'charge')
            
            if charge_col in df_input.columns:
                # Determine suffix for new columns based on index
                if col == 'accessorial type':
                    suffix = ''
                else:
                    # Extract the number from the column name if it exists
                    suffix = f"_{col.split('.')[-1]}" 
                
                # Assign new columns in df_new
                df_new[f'accessorial_charge{suffix}'] = np.where(df_input[col] == "FFS", "FUE", df_input[col])
                df_new[f'accessorial_charge_amount{suffix}'] = df_input[charge_col]
    
                df_new[f'accessorial_charge_amount{suffix}'] = np.where(df_new[f'accessorial_charge{suffix}'].str.strip().isin(["400", "AIR", "MIN", "DTS"]), 0, df_new[f'accessorial_charge_amount{suffix}'])

    return df_new




#endregion