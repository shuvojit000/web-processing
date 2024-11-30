import pandas as pd
import numpy as np
from datetime import datetime
from lookup.OrianExport_lookup import *
from web_utilities import *


#region OrianExport Custom Rule
def OrianExport_Custom_Rule(df_new, df_input):
    try:
        # HDR
        df_new['system'] = 'E'
        df_new['customer_id'] = "5810"
        df_new['carrier_scac'] = 'OS08'
        df_new['pro_no'] = np.where(df_new['pro_no'].notna() & (df_new['pro_no'] != ''), 
                                            df_new['pro_no'], 
                                            df_new['master_bill_number'])

        df_new['discount_amount'] = '000000000000'
        df_new['discount_percent'] = '000'
        df_new['prepaid_collect'] = 'P'
        df_new['cod_amount'] = '000000000000'
        df_new['trailer_size'] = "00000"
        df_new['service_level'] = df_new['service_level'].map(OrianExport_ServiceLevel).fillna(df_new['service_level'])
        df_new['currency_code'] = "ILS"
        df_new['exchange_rate'] = "0000000000"
        df_new['image_name'] = df_new['invoice_number'] + ".pdf"
        df_new['created_by'] = "EDI"

        # SHP
        df_new['shipper_name'] = df_new['shipper_name'].str.upper()

        df_new['shipper_address'] = (
        df_new['shipper_misc']
        .map(OrianExport_ShipperAddress_lookup)
        .fillna(df_new['account_number']
        .map(OrianExport_ShipperAddress_lookup))
        .fillna('NS')
        )

        df_new['shipper_address2'] = df_new['consignee_name'].map(OrianExport_ShipperAddress2_lookup).fillna('NS')

        df_new['shipper_city'] = (
        df_new['shipper_misc']
        .map(OrianExport_ShipperCity_lookup)
        .fillna(df_new['account_number']
        .map(OrianExport_ShipperCity_lookup))
        .fillna('NS')
        )

        df_new['shipper_state'] = 'NS'

        df_new['shipper_zip'] = (
        df_new['shipper_misc']
        .map(OrianExport_ShipperZip_lookup)
        .fillna(df_new['account_number']
        .map(OrianExport_ShipperZip_lookup))
        .fillna('NS')
        )

        df_new['shipper_country'] = 'IL'  


        # CON
        df_new['consignee_name'] = df_new['consignee_name'].str.upper()
        
        df_new['consignee_address'] = (
        df_new['consignee_address']
        .fillna(df_new['consignee_name']
        .map(OrianExport_ConsigneeAddress_lookup))
        .fillna('NS')
        )

        df_new['consignee_address2'] = df_new['consignee_name'].map(OrianExport_ConsigneeAddress2_lookup).fillna('NS')

        df_new['consignee_city'] = (
        df_new['consignee_city']
        .fillna(df_new['consignee_name']
        .map(OrianExport_ConsigneeCity_lookup))
        .fillna('NS')
        )

        df_new['consignee_state'] = df_new['consignee_name'].map(OrianExport_ConsigneeState_lookup).fillna('NS')

        df_new['consignee_zip'] = df_new['consignee_zip'].str.upper()
        
        # ITM
        df_new['line_item_number'] = '1'

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

        df_new['bill_qty'] = df_new['item_weight']

        df_new['bill_qty_uom'] = 'KG'
        df_new['liquid_volume'] = '00000000'
        df_new['length'] = '00000000'
        df_new['width'] = '00000000'
        df_new['height'] = '00000000'
        df_new['dimensional_weight'] = '00000000'

        df_new['uom'] = 'KG'
        df_new['weight_uom'] = 'KG'
        

        # ACC
        process_accessorials(df_new, df_input)

        # TAX
        process_tax(df_new, df_input)

        # MSC
        df_new['udf_1'] = np.where(df_new['udf_1'] == '1317178', '560031387', '511166373')
        df_new['udf_2'] = df_new['shipper_port_code'] + '-' + df_new['consignee_port_code']
        df_new['udf_9'] = df_new['invoice_number'].str[0] + '-' + df_new['invoice_number'].str.extract(r'(\d+)')[0]


        # FBM
        df_new['invoice_number'] = np.where(df_new['invoice_number'] != '', df_new['invoice_number'], df_new['pro_no'])
        df_new['bill_to_state'] = 'NS'
        # Calculate total_invoice_amount
        df_new['total_invoice_amount'] = pd.to_numeric(df_new['total_invoice_amount'], errors='coerce')
        df_new['total_invoice_amount'] = df_new.groupby('invoice_number')['total_invoice_amount'].transform('sum').round(2)


        # Calculate total_shipment_count
        df_new['total_shipment_count'] = df_new.groupby('invoice_number')['invoice_number'].transform('count')

        # FSP
        df_new['shipper_port_country_code'] = 'IL'

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
            matching_key = next((key for key in OrianExport_Accessorials_lookup if col_cleaned == key.lower()), None)

            if matching_key:
                # Determine corresponding accessorial code from the lookup dictionary
                accessorial_code = OrianExport_Accessorials_lookup[matching_key]
                
                # Set suffix to indicate the index for additional accessorial charges
                suffix = f"_{suffix_counter}" if suffix_counter > 0 else ''

                # Assign new columns in df_new
                df_new.loc[idx, f'accessorial_charge{suffix}'] = accessorial_code
                df_new.loc[idx, f'accessorial_charge_amount{suffix}'] = row[col]

                # Assign 'V' to 'accessorial_carrier_tax_code' only for specified matching keys
                if accessorial_code in ['PUC', '370', 'TERO', '586', 'LAD']:
                    df_new.loc[idx, f'accessorial_carrier_tax_code{suffix}'] = 'V'


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
            matching_key = next((key for key in OrianExport_Tax_lookup if col_cleaned == key.lower()), None)

            if matching_key:
                # Determine corresponding accessorial code from the lookup dictionary
                tax_code = OrianExport_Tax_lookup[matching_key]
                
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