import pandas as pd
import numpy as np
from datetime import datetime
from lookup.OrianImport_lookup import *
from web_utilities import *


#region OrianExport Custom Rule
def OrianImport_Custom_Rule(df_new, df_input):
    try:
        # HDR
        df_new['system'] = 'E'
        df_new['customer_id'] = "5810"
        df_new['carrier_scac'] = 'OS08'
        df_new['pro_no'] = np.where(df_new['pro_no'].notna() & (df_new['pro_no'] != ''), 
                                            df_new['pro_no'], 
                                            df_new['master_bill_number'])

        df_new['discount_amount'] = '0'
        df_new['discount_percent'] = '0'
        df_new['prepaid_collect'] = 'P'
        df_new['cod_amount'] = '0'
        df_new['trailer_size'] = "0"
        df_new['currency_code'] = "ILS"
        df_new['exchange_rate'] = "0"
        df_new['accounting_code'] = "0"
        df_new['image_name'] = df_new['invoice_number'] + ".pdf"
        df_new['created_by'] = "EDI"

        # SHP
        df_new['shipper_name'] = df_new['shipper_name'].str.upper()
        df_new['shipper_address'] = df_new['shipper_name'].map(lambda x: Suppliers_Address_Lookup.get(x, {}).get('address_1')).fillna('NS')
        df_new['shipper_address2'] = df_new['shipper_name'].map(lambda x: Suppliers_Address_Lookup.get(x, {}).get('address_2')).fillna('NS')
        df_new['shipper_city'] = df_new['shipper_name'].map(lambda x: Suppliers_Address_Lookup.get(x, {}).get('city')).fillna('NS')
        df_new['shipper_state'] = df_new['shipper_name'].map(lambda x: Suppliers_Address_Lookup.get(x, {}).get('state')).fillna('NS')
        df_new['shipper_zip'] = df_new['shipper_name'].map(lambda x: Suppliers_Address_Lookup.get(x, {}).get('zip_code')).fillna('NS')

        # CON
        df_new['consignee_name'] = df_new['consignee_name'].str.upper()
        df_new['consignee_address'] = df_new['consignee_misc'].map(Address1_Lookup).fillna('NS')
        df_new['consignee_address2'] = df_new['consignee_misc'].map(Address2_Lookup).fillna('NS')
        df_new['consignee_city'] = df_new['consignee_misc'].map(City_Lookup).fillna('NS')
        df_new['consignee_state'] = 'NS'
        df_new['consignee_zip'] = df_new['consignee_misc'].map(Postal_Lookup).fillna('NS')
        df_new['consignee_country'] = df_new['consignee_country'].map(Country_Lookup).fillna('IL')

        df_new['consignee_name'] = np.where(df_new['udf_1'] == '1317178', 'APPLIED MATERIALS SOUTH EAST', df_new['consignee_name'])
        df_new['consignee_address'] = np.where(df_new['udf_1'] == '1317178', '4 BERGMAN', df_new['consignee_address'])
        df_new['consignee_address2'] = np.where(df_new['udf_1'] == '1317178', 'NS', df_new['consignee_address2'])
        df_new['consignee_city'] = np.where(df_new['udf_1'] == '1317178', 'REHOVOT', df_new['consignee_city'])
        df_new['consignee_state'] = np.where(df_new['udf_1'] == '1317178', 'NS', df_new['consignee_state'])
        df_new['consignee_zip'] = np.where(df_new['udf_1'] == '1317178', '76100', df_new['consignee_zip'])
        df_new['consignee_country'] = np.where(df_new['udf_1'] == '1317178', 'IL', df_new['consignee_country'])
        
        # ITM
        df_new['line_item_number'] = '0001'

        df_new['no_pieces'] = pd.to_numeric(df_new['no_pieces'], errors='coerce')
        df_new['no_pieces'] = np.where(df_new['no_pieces'] > 1, df_new['no_pieces'], 1)

        df_new['item_weight'] = pd.to_numeric(df_new['item_weight'], errors='coerce')
        df_new['item_weight'] = np.where(df_new['item_weight'] > 1, df_new['item_weight'], 1)

        df_new['bill_qty'] = pd.to_numeric(df_new['bill_qty'], errors='coerce')
        df_new['bill_qty'] = np.where(df_new['bill_qty'] > 1, df_new['bill_qty'], 1)

        df_new['liquid_volume'] = '0'
        df_new['length'] = '0'
        df_new['width'] = '0'
        df_new['height'] = '0'
        df_new['dimensional_weight'] = '0'
        df_new['uom'] = 'KG'
        df_new['weight_uom'] = 'KG'

        df_new['actual_weight'] = pd.to_numeric(df_new['actual_weight'], errors='coerce')
        df_new['actual_weight'] = np.where(df_new['actual_weight'] > 1, df_new['actual_weight'], 1)    

        # ACC
        process_accessorials(df_new, df_input)

        # MSC
        df_new['udf_1'] = np.where(df_new['account_number'] == '1317178', '560031387', '511166373')
        df_new['udf_2'] = df_new['shipper_port_code'] + '-' + 'TLV'
        df_new['udf_9'] = df_new['udf_9'].apply(lambda x: f"{x[0]}-{x[1:]}" if pd.notna(x) and len(x) > 1 else x)
        df_new['udf_19'] = np.where(
            df_new['account_number'].isin(["1317178", "1310136", "02024490"]), 
            df_new['udf_19'], 
            ''
        )

        # TAX
        df_new['tax_type_code'] = 'VAT'
        df_new['base_tax_amount'] = '0'

        # FBM
        # Calculate total_invoice_amount
        df_new['total_invoice_amount'] = pd.to_numeric(df_new['total_invoice_amount'], errors='coerce')
        df_new['total_invoice_amount'] = df_new.groupby('invoice_number')['total_invoice_amount'].transform('sum').round(2)

        # Calculate total_shipment_count
        df_new['total_shipment_count'] = df_new.groupby('invoice_number')['invoice_number'].transform('count')

        # FCP
        df_new['consignee_port_country_code'] = 'IL'
        df_new['consignee_port_code'] = 'TLV'

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

def process_accessorials(df_new, df_input):
    # Iterate through each row of df_input using df_input.iterrows()
    for idx, row in df_input.iterrows():
        # Initialize suffix counter
        suffix_counter = 0

        # Iterate through all columns in the row
        for col in df_input.columns:
            col_cleaned = clean_column_header_name(col)

            # Find the matching key in the lookup dictionary
            matching_key = next((key for key in OrianImport_Accessorials_lookup if col_cleaned == key.lower()), None)

            if matching_key:
                # Determine corresponding accessorial code from the lookup dictionary
                accessorial_code = OrianImport_Accessorials_lookup[matching_key][0]
                # Determine corresponding accessorial carrier tax code from the lookup dictionary
                accessorial_carrier_tax_code = OrianImport_Accessorials_lookup[matching_key][1]
                
                # Set suffix to indicate the index for additional accessorial charges
                suffix = f"_{suffix_counter}" if suffix_counter > 0 else ''

                # Assign new columns in df_new  
                df_new.loc[idx, f'accessorial_charge{suffix}'] = accessorial_code
                df_new.loc[idx, f'accessorial_charge_amount{suffix}'] = row[col]
                df_new.loc[idx, f'accessorial_carrier_tax_code{suffix}'] = accessorial_carrier_tax_code

                # Increment suffix counter for the next charge in the same row
                suffix_counter += 1

    return df_new





 #endregion