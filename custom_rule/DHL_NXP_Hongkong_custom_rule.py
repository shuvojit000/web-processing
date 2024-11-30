import pandas as pd
import numpy as np
from datetime import datetime
from web_utilities import *
from lookup.DHL_NXP_Hongkong_lookup import *

#region DHL_NXP_Hongkong Customer Rule
def DHL_NXP_Hongkong_Custom_Rule(df_new, df_input):
    try:
        # HDR   
        df_new['system'] = 'E'
        df_new['customer_id'] = df_new['customer_id'].str[-4:]
        df_new['carrier_scac'] = df_new['carrier_scac'].str[:4]
        df_new['pro_no'] = np.where(
            df_new['pro_no'].notna() & (df_new['pro_no'] != ''), 
            df_new['pro_no'], 
            np.where(
                (df_new['bill_of_lading_number'] != '') & (df_new['bill_of_lading_number'] != 'NS'), 
                df_new['bill_of_lading_number'],
                df_new['invoice_number']
            )
        )


        # df_new['billed_amount'] = df_new['billed_amount']
        df_new['prepaid_collect'] = 'P'
        df_new['service_level'] = df_new['service_level'].map(DHL_NXP_HKG_Service_CrossRef).fillna('AIR')
        df_new['currency_code'] = 'USD'
        df_new['cargo_type'] = df_new['cargo_type'].map(DHL_NXP_HKG_Cargo_CrossRef).fillna('')
        df_new['image_name'] =  df_new['image_name'] + '.pdf'
        df_new['created_by'] = 'EDI'

        # SHP
        df_new['shipper_name'] = np.where(df_new['shipper_name'] != "", df_new['shipper_name'].str.upper(), "NS")
        df_new['shipper_address'] = df_new['shipper_address'].str.upper()
        df_new['shipper_city'] = df_new['shipper_city'].map(DHL_NXP_HKG_City_CrossRef).fillna(df_new['shipper_city'])
        df_new['shipper_state'] = np.where(df_new['shipper_state'] != "", df_new['shipper_state'].str.upper(), "NS") 
        df_new['shipper_zip'] = np.where(df_new['shipper_zip'] != "", df_new['shipper_zip'].str.upper(), "NS") 
        df_new['shipper_stop_sequence'] = '01'
        df_new['shipper_country'] = np.where(df_new['shipper_country'] != "", df_new['shipper_country'].str.upper(), "NS")

        # CON
        df_new['consignee_name'] = np.where(df_new['consignee_name'] != "", df_new['consignee_name'].str.upper(), "NS")
        df_new['consignee_address'] = df_new['consignee_address'].str.upper()
        df_new['consignee_city'] = np.where(df_new['consignee_city'] != "", df_new['consignee_city'].str.upper(), "NS")
        df_new['consignee_state'] = np.where(df_new['consignee_state'] != "", df_new['consignee_state'].str.upper(), "NS") 
        df_new['consignee_zip'] = np.where(df_new['consignee_zip'] != "", df_new['consignee_zip'].str.upper(), "NS") 
        df_new['consignee_stop_sequence'] = '01'
        df_new['consignee_country'] = np.where(df_new['consignee_country'] != "", df_new['consignee_country'].str.upper(), "NS")

        # PUR
        df_new['po_number'] = 'NS' 
        df_new['po_consignee_stop_sequence'] = '00'

        # BOL
        df_new['bol_shipper_stop_sequence'] = '00'

        # ITM
        df_new['line_item_number'] = '1'
        df_new['no_pieces'] = np.where(df_new['no_pieces'] != "", df_new['no_pieces'].str.upper(), "1")
        df_new['item_weight'] = np.where(df_new['item_weight'].astype(float) > 1, df_new['item_weight'], "1")
        df_new['bill_qty'] = np.where(df_new['bill_qty'].astype(float) > 1, df_new['bill_qty'], "1")
        df_new['bill_qty_uom'] = 'KG'
        df_new['uom'] = 'KG' 
        df_new['weight_uom'] = 'KG'
        df_new['actual_weight'] = np.where(df_new['actual_weight'].astype(float) > 1, df_new['actual_weight'], "1")

        # ACC
        process_accessorials(df_new, df_input)

        # TAX
        process_tax(df_new, df_input)

        df_new['master_bill_number'] = np.where(df_new['master_bill_number'] != "", df_new['master_bill_number'].str.upper(), "")
        # Calculate total_invoice_amount
        df_new['total_invoice_amount'] = pd.to_numeric(df_new['total_invoice_amount'], errors='coerce')
        # Group by 'invoice_number' and sum the 'total_invoice_amount'
        df_new['total_invoice_amount'] = df_new.groupby('invoice_number')['total_invoice_amount'].transform('sum').round(2)
        # Calculate total_shipment_count
        df_new['total_shipment_count'] = df_new.groupby('invoice_number')['invoice_number'].transform('count')

    
        # FSP
        df_new['consignee_port_country_code'] = np.where(df_new['consignee_port_country_code'] == "SIN", 'SG', df_new['consignee_port_code'])

        # Format Dates
        df_new['ship_date'] = df_new['ship_date'].apply(lambda x: convert_to_yyyymmdd(x, '%d-%b-%Y'))
        df_new['delivery_date'] = df_new['delivery_date'].apply(lambda x: convert_to_yyyymmdd(x, '%d-%b-%Y'))
        df_new['pro_date'] = df_new['pro_date'].apply(lambda x: convert_to_yyyymmdd(x, '%d-%b-%Y'))
        df_new['interline_date'] = df_new['interline_date'].apply(lambda x: convert_to_yyyymmdd(x, '%d-%b-%Y'))
        df_new['guaranteed_date'] = df_new['guaranteed_date'].apply(lambda x: convert_to_yyyymmdd(x, '%d-%b-%Y'))
        df_new['invoice_date'] = df_new['invoice_date'].apply(lambda x: convert_to_yyyymmdd(x, '%d-%b-%Y'))
        df_new['master_bill_date'] = df_new['master_bill_date'].apply(lambda x: convert_to_yyyymmdd(x, '%d-%b-%Y'))

        df_new = df_new[df_new['billed_amount'] != 0]
        return df_new
    except Exception as e:
        # Re-throw the exception to propagate it to the calling function
        raise
def process_accessorials(df_new, df_input):
    # Iterate through each row of df_input using df_input.iterrows()
    for idx, row in df_input.iterrows():
        # Initialize suffix counter
        suffix_counter = 0

        # Iterate through all columns in the row
        for col in df_input.columns:
            col_cleaned = clean_column_header_name(col)

            # Find the matching key in the lookup dictionary
            matching_key = next((key for key in DHL_NXP_HKG_Accessorials_lookup if col_cleaned == key.lower()), None)

            if matching_key:
                # Determine corresponding accessorial code from the lookup dictionary
                accessorial_code = DHL_NXP_HKG_Accessorials_lookup[matching_key]
                
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
            matching_key = next((key for key in DHL_NXP_HKG_Tax_lookup if col_cleaned == key.lower()), None)

            if matching_key:
                # Determine corresponding accessorial code from the lookup dictionary
                tax_code = DHL_NXP_HKG_Tax_lookup[matching_key]
                
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