import pandas as pd
import numpy as np
from datetime import datetime
from lookup.DHLChina_lookup import *
from web_utilities import *


#region Sarcona Custom Rule
def Sarcona_Custom_Rule(df_new, df_input):
    try:
        df_new['pro_no'] = df_new['pro_no'].apply(lambda x: clean_string(x, removeSpace=True))
        df_copy = df_new.copy()
        df_new = df_new.drop_duplicates(subset=['pro_no'])

        # HDR
        df_new['system'] = 'E'
        df_new['customer_id'] = np.where(df_new['customer_id'].isin(['AGFAOFF', 'ECO3']), '6045', '5753')
        df_new['carrier_scac'] = 'SARJ'
        # df_new['billed_amount'] = df_new[accessorial_columns].sum(axis=1)
        df_new['discount_amount'] = '0'
        df_new['discount_percent'] = '0'
        df_new['prepaid_collect'] = df_new['prepaid_collect'].str[:1]
        df_new['cod_amount'] = np.where(df_new['cod_amount'] != '', df_new['cod_amount'], '0')
        df_new['account_number'] = df_new['account_number'][:30]
        df_new['equipment_type'] = "14"
        df_new['trailer_size'] = "14"
        df_new['currency_code'] = "USD"
        df_new['exchange_rate'] = "0"
        df_new['shipment_signed'] = df_new['shipment_signed'][:20]
        df_new['accounting_code'] = "0"
        df_new['created_by'] = "EDI"

        # SHP
        df_new['shipper_name'] = df_new['shipper_name'].str.upper()
        df_new['shipper_address'] = df_new['shipper_address'].str.upper()
        df_new['shipper_city'] = df_new['shipper_city'].str.upper()
        df_new['shipper_state'] = np.where(df_new['shipper_state'] != '', df_new['shipper_state'].str.upper(), 'NS')

        df_new['shipper_country'] = (
            df_new['shipper_country']
            .apply(lambda x: 'US' if x == 'USA' else x)
            .replace('', 'NS')
            .fillna('NS')
        )

        # CON
        df_new['consignee_name'] = df_new['consignee_name'].str.upper()
        df_new['consignee_address'] = df_new['consignee_address'].str.upper()
        df_new['consignee_city'] = df_new['consignee_city'].str.upper()
        df_new['consignee_state'] = df_new['consignee_state'].str.upper()
        df_new['consignee_zip'] = df_new['consignee_zip'].str.upper()
        
        df_new['consignee_country'] = (
            df_new['consignee_country']
            .apply(lambda x: 'US' if x == 'USA' else x)
            .replace('', 'NS')
            .fillna('NS')
        )

        # PUR
        df_new['po_number'] = 'NS'

        # BOL
        df_new['bill_of_lading_number'] = df_new['bill_of_lading_number'].str.replace(' - ', ' ')
            
        # ITM
        df_new['line_item_number'] = '1'
        df_new['no_pieces'] = pd.to_numeric(df_new['no_pieces'], errors='coerce').fillna(0).astype(int)
        df_new['no_pieces'] = np.where(df_new['no_pieces'] > 1, df_new['no_pieces'], '1')

        df_new['item_weight'] = pd.to_numeric(df_new['item_weight'], errors='coerce').fillna(0).astype(int)
        df_new['actual_weight'] = pd.to_numeric(df_new['actual_weight'], errors='coerce').fillna(0).astype(int)

        df_new['item_weight'] = np.where(df_new['item_weight'] > 1, 
                                    df_new['item_weight'].astype(str).str.rstrip().replace(',', ''), 
                                    np.where(df_new['actual_weight'] > 1, 
                                            df_new['actual_weight'].astype(str).str.rstrip().replace(',', ''), 
                                            '1'))

        df_new['bill_qty'] = np.where(df_new['bill_qty'] != '', df_new['bill_qty'].str.upper(), '0')
        df_new['liquid_volume'] = '0'
        df_new['length'] = np.where(df_new['length'] != '', df_new['length'].str.upper(), '0')
        df_new['width'] = np.where(df_new['width'] != '', df_new['width'].str.upper(), '0')
        df_new['height'] = np.where(df_new['height'] != '', df_new['height'].str.upper(), '0')
        df_new['dim_uom'] = np.where(df_new['dim_uom'] == 'KG|K', 'C', 'N')
        df_new['dimensional_weight'] = np.where(df_new['dimensional_weight'] != '', df_new['dimensional_weight'].str.upper(), '0')
        df_new['uom'] = np.where(df_new['uom'] == 'K', 'KG', df_new['uom'].str.upper())
        df_new['weight_uom'] = np.where(df_new['weight_uom'] == 'K', 'KG', df_new['weight_uom'].str.upper())

        df_new['item_weight'] = pd.to_numeric(df_new['item_weight'], errors='coerce').fillna(0).astype(int)
        df_new['actual_weight'] = pd.to_numeric(df_new['actual_weight'], errors='coerce').fillna(0).astype(int)
        df_new['actual_weight'] = np.where(df_new['actual_weight'] > 1, 
                                    df_new['actual_weight'].astype(str).str.rstrip().replace(',', ''), 
                                    np.where(df_new['item_weight'] > 1, 
                                            df_new['item_weight'].astype(str).str.rstrip().replace(',', ''), 
                                            '1'))

    
        process_accessorials(df_new, df_copy)
    
        # Count the occurrences for the specified grouping and create a new column 'total_shipment_count'
        df_new['total_shipment_count'] = df_new.groupby(['bill_of_lading_number', 'pro_no'])['pro_no'].transform('count')

        # EQT
        process_equipment(df_new, df_input)

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

def process_accessorials(df_new, df_copy):
    for index, row in df_new.iterrows():
        pro_no = row['pro_no']
        
        # Filter the rows in df_copy that match the current pro_no
        accessorial_rows = df_copy[df_copy['pro_no'] == pro_no]
        # Convert 'accessorial_charge_amount' to numeric, coercing errors to NaN
        accessorial_rows['accessorial_charge_amount'] = pd.to_numeric(accessorial_rows['accessorial_charge_amount'], errors='coerce')

        # Now calculate the sum
        total_accessorial_charge = accessorial_rows['accessorial_charge_amount'].sum()
        
        # Filter out the charge types that are not "CTG", "SHU", or "SHT"
        accessorial_rows = accessorial_rows[~accessorial_rows['accessorial_charge'].isin(['CTG', 'SHU', 'SHT'])]

        # Extract charge types and charges for the current pro_no
        charge_types = accessorial_rows['accessorial_charge'].tolist()
        charges = accessorial_rows['accessorial_charge_amount'].tolist()
        
        # Add charge types and charges as new columns in df_new
        for idx, (charge_type, charge) in enumerate(zip(charge_types, charges)):
            if idx == 0:
                df_new.at[index, 'accessorial_charge'] = charge_type
                df_new.at[index, 'accessorial_charge_amount'] = charge
            else:
                df_new.at[index, f'accessorial_charge_{idx}'] = charge_type
                df_new.at[index, f'accessorial_charge_amount_{idx}'] = charge

        df_new.at[index, 'billed_amount'] = total_accessorial_charge
        df_new.at[index, 'total_invoice_amount'] = total_accessorial_charge
    return df_new



def process_equipment(df_new, df_input):
    # Initialize a counter for accessorial charges
    idx = 1
    
    # Process columns in pairs (ACCESSORIAL TYPE.n, ACCESSORIAL CHARGE.n)
    while True:
        equipment_number = f'container number{idx}'
        
        if equipment_number in df_input.columns:
            df_new[f'aequipment_type_{idx}'] = '14'
            df_new[f'equipment_number_{idx}'] = df_input[equipment_number]
            idx += 1
        else:
            break
    
    return df_new






 #endregion