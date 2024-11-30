import pandas as pd
import numpy as np
from datetime import datetime
from logger import create_logger
from lookup.Teradyne_NXP_Taiwan_lookup import *
from web_utilities import *
import configparser
import traceback
from lookup.ColumnHeader.Accessorial_Header import *
from lookup.ColumnHeader.Tax_Header import *
from lookup.ColumnHeader.Equipment_Header import *
import math

logger=create_logger(__name__)


#region Teradyne NXP Taiwan Custom Rule
def Teradyne_NXP_Taiwan_Custom_Rule(df_new, df_input):
    try:
        # HDR
        df_new['system'] = 'E'
        df_new['customer_code'] = df_new['customer_id']
        df_new['customer_id'] = df_new['customer_id'].str[-4:]
        df_new['carrier_scac'] = np.where(df_new['carrier_scac'] == 'TN29', 'TN36', df_new['carrier_scac'])
        df_new['pro_no'] = np.where(df_new['pro_no'].notna() & (df_new['pro_no'] != ''), 
                                            df_new['pro_no'], 
                                            df_new['master_bill_number'])
        
        df_new['billed_amount'] = df_new['billed_amount']
        df_new['prepaid_collect'] = df_new['prepaid_collect'].str[:1]
        df_new['service_level'] = df_new.apply(map_service_level, axis=1)
        df_new['trailer_size'] = df_new.apply(map_header_equipment_type, axis=1)
        df_new['currency_code'] = df_new.apply(map_currency_code, axis=1)
        df_new['shipment_signed'] = df_new['shipment_signed'].str[:20].str.upper()

        df_new['invoice_number'] = np.where(df_new['invoice_number'] != '', df_new['invoice_number'], df_new['pro_no'])
        df_new['image_name'] = df_new.apply(map_image_name, axis=1)   

        df_new['created_by'] = 'EDI'

        # SHP
        df_new['shipper_name'] = df_new.apply(map_shipper_name, axis=1)
        df_new['shipper_address'] = df_new['shipper_address'].str.upper() 
        df_new['shipper_city'] = df_new.apply(map_shipper_city, axis=1)
        df_new['shipper_state'] = df_new.apply(map_shipper_state, axis=1)
        df_new['shipper_postal'] = df_new.apply(map_shipper_postal, axis=1)
        df_new['shipper_country'] = df_new.apply(map_shipper_country, axis=1)


        # CON
        df_new['consignee_name'] = df_new.apply(map_consignee_name, axis=1)
        df_new['consignee_address'] = df_new['consignee_address'].str.upper() 
        df_new['consignee_city'] = df_new.apply(map_consignee_city, axis=1)
        df_new['consignee_state'] = df_new.apply(map_consignee_state, axis=1)
        df_new['consignee_postal'] = df_new.apply(map_consignee_postal, axis=1)
        df_new['consignee_country'] = df_new.apply(map_consignee_country, axis=1)

        # PUR
        # Replace empty strings in 'po_number' with 'NS'
        df_new['po_number'] = np.where(df_new['po_number'] == '', 'NS', df_new['po_number'])

        # Split 'po_number' based on commas
        po_split = df_new['po_number'].str.split(',', expand=True)

        # Assign each split value to a new column dynamically
        for idx, col in enumerate(po_split.columns):
            if idx == 0:
                df_new['po_number'] = po_split[col]  # First column remains as 'po_number'
                df_new['po_consignee_stop_sequence'] = '0'  # Default stop sequence
            else:
                df_new[f'po_number_{idx}'] = po_split[col]  # Additional columns as 'po_number_1', 'po_number_2', etc.
                df_new[f'po_consignee_stop_sequence_{idx}'] = '0'  # Default stop sequence for additional columns


        # BOL
        df_new['bill_of_lading_number'] = df_new['bill_of_lading_number'].replace('', 'NS').str.upper()

        # ITM
        df_new['line_item_number'] = '1'
        df_new['no_pieces'] = df_new.apply(map_pieces, axis=1)
        df_new['item_weight'] = df_new.apply(map_weight, axis=1)
        df_new['item_desc'] = df_new['item_desc'].str[:50].str.upper()
        df_new['bill_qty_uom'] = df_new['bill_qty_uom'].str[:4].str.upper()

        df_new['length'] = np.where(df_new['length'] != '', df_new['length'], '0')
        df_new['length'] = np.abs(pd.to_numeric(df_new['length'], errors='coerce').fillna(0))

        df_new['width'] = np.where(df_new['width'] != '', df_new['width'], '0')
        df_new['width'] = np.abs(pd.to_numeric(df_new['width'], errors='coerce').fillna(0))

        df_new['height'] = np.where(df_new['height'] != '', df_new['height'], '0')
        df_new['height'] = np.abs(pd.to_numeric(df_new['height'], errors='coerce').fillna(0))

        df_new['dim_uom'] = df_new.apply(map_dim_uom, axis=1)

        df_new['dimensional_weight'] = np.where(df_new['dimensional_weight'] != '', df_new['dimensional_weight'], '0')
        df_new['dimensional_weight'] = np.abs(pd.to_numeric(df_new['dimensional_weight'], errors='coerce').fillna(0))

        df_new['uom'] = df_new['uom'].apply(map_uom)
        df_new['weight_uom'] = df_new['weight_uom'].apply(map_uom)
        df_new['actual_weight'] = df_new.apply(map_actual_weight, axis=1)

        df_new['liquid_volume'] = df_new.apply(lambda x: map_liquid_volume(x) if x['customer_id'] == '3960' else 0, axis=1)
        df_new['liquid_volume_uom'] = df_new.apply(lambda x: map_liquid_volume_uom(x) if x['customer_id'] == '3960' else '', axis=1)

        

        # Accessorials
        process_accessorials(df_new, df_input)

        # Tax
        process_tax(df_new, df_input)

        # FBM

        # Calculate total_invoice_amount
        df_new['billed_amount'] = df_new['billed_amount'].str.rstrip().astype(float)
        df_new['total_invoice_amount'] = df_new.groupby('invoice_number')['billed_amount'].transform('sum').round(2)

        # Calculate total_shipment_count
        df_new['total_shipment_count'] = df_new.groupby('invoice_number')['invoice_number'].transform('count')

        df_new['bill_to_name'] = df_new['bill_to_name'].str.upper()
        df_new['bill_to_addr1'] = df_new['bill_to_addr1'].str.upper()
        df_new['bill_to_addr2'] = df_new['bill_to_addr2'].str.upper()
        df_new['bill_to_city'] = df_new['bill_to_city'].str.upper()
        df_new['bill_to_state'] = df_new['bill_to_state'].str.upper()
        df_new['bill_to_postal'] = df_new['bill_to_postal'].str.upper()
        df_new['bill_to_country'] = df_new.apply(map_bill_to_country, axis=1)
        
        # EQT
        if df_new['customer_id'].iloc[0] == '5918':
            process_equipments_Taiwan(df_new, df_input)
        elif df_new['customer_id'].iloc[0] == '3960':
            process_equipments_Teradyne(df_new, df_input)
            
        # FSP
        df_new['shipper_port_code'] = df_new.apply(map_shipper_port, axis=1)

        # FCP
        df_new['consignee_port_code'] = df_new.apply(map_consignee_port, axis=1)
        # logger.info(df_new['total_invoice_amount'])

        for column in date_columns:
            df_new[column] = df_new.apply(lambda row: apply_date_format(row, column), axis=1)

        # Remove rows where billed_amount is 0
        df_new = df_new[df_new['billed_amount'] != 0]

        return df_new
    except Exception as e:
        # Capture detailed error and stack trace
        error_message = f"Error in Teradyne_NXP_Taiwan_Custom_Rule: {str(e)}\n" + traceback.format_exc()
        raise Exception(error_message)
    
    
#region HDR
def map_service_level(row):
    if row['customer_id'] == "3960" and row['service_level'] in tera_nxp_taiwan_service_level_cross_ref_3:
        return tera_nxp_taiwan_service_level_cross_ref_3[row['service_level']]
    
    concatenated_key = f"{row['carrier_scac']}*{row['service_level']}"
    if concatenated_key in tera_nxp_taiwan_service_level_cross_ref_1:
        return tera_nxp_taiwan_service_level_cross_ref_1[concatenated_key]
    
    if row['service_level'] in tera_nxp_taiwan_service_level_cross_ref_2:
        return tera_nxp_taiwan_service_level_cross_ref_2[row['service_level']]
    
    return row['service_level'].upper()

def map_header_equipment_type(row):
    if row['customer_id'] == "3960" and row['trailer_size'] in tera_nxp_taiwan_equipment_cross_ref:
        return tera_nxp_taiwan_equipment_cross_ref[row['trailer_size']]
    
    if row['customer_id'] == "3960":
        return "14"
    
    if row['customer_id'] == "5918":
        return row['trailer_size']
    
    return row['trailer_size']  # Default case, if any

def map_currency_code(row):
    if row['currency_code'] in tera_nxp_taiwan_currency_cross_ref:
        return tera_nxp_taiwan_currency_cross_ref[row['currency_code']]
    else:
        return row['currency_code']
    
def map_image_name(row):
    if row['customer_id'] == "5918":
        if row['image_name'].startswith("TSI_"):
            return row['image_name'][4:] + ".tif"
        elif row['image_name'].startswith("NVG_"):
            return row['image_name'][4:] + ".tif"
        elif row['image_name'].startswith("FPI_"):
            return row['image_name'][4:] + ".tif"
        elif row['image_name'].startswith("TEK_"):
            return row['image_name'][4:] + ".tif"
    
    if row['customer_id'] == "3960":
        if row['carrier_scac'] == "GW02":
            return f"{row['carrier_scac']}_{row['customer_code']}_{row['pro_no']}_{row['invoice_number']}.pdf"
        elif pd.notna(row['image_name']):
            return row['image_name']
    
    return ""
    
#endregion

#region Shipper
def map_shipper_name(row):
    if pd.notna(row['shipper_name']) and row['shipper_name'].strip() != '':
        return row['shipper_name'].upper()
    elif pd.notna(row['shipper_misc']) and row['shipper_misc'].strip() != '':
        return row['shipper_misc'].upper()
    else:
        return 'NS'

def map_shipper_city(row):
    if row['customer_id'] == "3960" and row['shipper_city'] in tera_nxp_taiwan_city_cross_ref:
        return tera_nxp_taiwan_city_cross_ref[row['shipper_city']]
    elif row['shipper_city'] != '':
        return row['shipper_city']
    else:
        return 'NS'

def map_shipper_state(row):
    if row['shipper_state'].strip() != '':
        return row['shipper_state'].upper()
    else:
        return 'NS'
    
def map_shipper_postal(row):
    # Extract and strip postal code
    postal = row['shipper_zip'].strip() if pd.notna(row.get('shipper_zip')) else ''
    
    # Check conditions and map accordingly
    if row['customer_id'] == '3960':
        if postal == '272000':
            return '272100'
        elif row['carrier_scac'] == 'KL12' and '-' in postal:
            parts = postal.split('-')
            return f"{parts[0][:3]}-{parts[1][-4:]}"
        elif row['shipper_country'].upper() == 'CA':
            return f"{postal[:3]} {postal[-3:]}"
        elif len(postal) >= 5 and postal[4] == '':
            return f"0{postal}"
        else:
            return postal if postal else 'NS'
    return 'NS'

def map_shipper_country(row):
    if row['shipper_country'].strip() != '':
        return row['shipper_country'].upper()
    else:
        return 'NS'

#endregion

#region Consignee
def map_consignee_name(row):
    if pd.notna(row['consignee_name']) and row['consignee_name'].strip() != '':
        return row['consignee_name'].upper()
    elif pd.notna(row['consignee_misc']) and row['consignee_misc'].strip() != '':
        return row['consignee_misc'].upper()
    else:
        return 'NS'

def map_consignee_city(row):
    if row['customer_id'] == "3960" and row['consignee_city'] in tera_nxp_taiwan_city_cross_ref:
        return tera_nxp_taiwan_city_cross_ref[row['consignee_city']]
    elif row['consignee_city'] != '':
        return row['consignee_city']
    else:
        return 'NS'

def map_consignee_state(row):
    if row['consignee_state'].strip() != '':
        return row['consignee_state'].upper()
    else:
        return 'NS'
    
def map_consignee_postal(row):
    postal = row['consignee_zip'].strip() if pd.notna(row.get('consignee_zip')) else ''
    
    if row['customer_id'] == '3960':
        if postal == '272000':
            return '272100'
        elif row['carrier_scac'] == 'KL12' and '-' in postal:
            parts = postal.split('-')
            return f"{parts[0][:3]}-{parts[1][-4:]}"
        elif row['consignee_country'].upper() == 'CA':
            return f"{postal[:3]} {postal[-3:]}"
        elif len(postal) >= 5 and postal[4] == '':
            return f"0{postal}"
        else:
            return postal if postal else 'NS'
    return 'NS'

def map_consignee_country(row):
    if row['consignee_country'].strip() != '':
        return row['consignee_country'].upper()
    else:
        return 'NS'

#endregion

#region Item
def map_pieces(row):
    # Ensure 'pieces' is treated as numeric
    pieces = float(row['no_pieces'])
    if pieces > 1:
        return round(pieces)
    else:
        return 1

def clean_weight(weight):
    # Trim spaces from right and replace "," with "."
    weight = weight.strip().replace(',', '.')
    # Replace special characters using regex
    weight = re.sub(r'((?:(\.\d*?[1-9]+)|\.)0*$)', '', weight)
    return weight

def map_weight(row):
    # Convert chargeable_weight and actual_weight to strings and replace commas with dots
    chargeable_weight = str(row['item_weight']).strip().replace(',', '.') if str(row['item_weight']).strip() != '' else '0'
    actual_weight = str(row['actual_weight']).strip().replace(',', '.') if str(row['actual_weight']).strip() != '' else '0'


    # Convert to float for comparison
    chargeable_weight_float = float(chargeable_weight)
    actual_weight_float = float(actual_weight)

    # Return the appropriate weight based on the conditions
    return chargeable_weight if chargeable_weight_float > 1 else (actual_weight if actual_weight_float > 1 else '1')

    
def map_liquid_volume(row):
    if row['customer_id'] == '3960' and row['liquid_volume_uom'].lower() == 'cbm':
        return round(float(row['liquid_volume']))
    else:
        return '0'
    
    
def map_liquid_volume_uom(row):
    if row['customer_id'] == '3960' and row['liquid_volume_uom'].lower() == 'cbm':
        return 'CM'
    else:
        return row['liquid_volume_uom']


def map_volume_uom(row):
    if row['customer_id'] == '3960' and row['billed_uom'].lower() == 'cbm':
        return 'CM'
    elif row['customer_id'] == '3960':
        return row['billed_uom']
    else:
        return row['billed_uom']

def map_dim_uom(row):
    receiver_id = row['customer_id']
    billed_uom = row['bill_qty_uom'].upper()
    weight_uom = row['weight_uom'].upper()
    
    if receiver_id == '3960' and billed_uom == 'CBM':
        return 'CM'
    elif receiver_id == '3960' and billed_uom in ['KG', 'K']:
        return 'C'
    elif receiver_id == '5918' and weight_uom in ['KG', 'K']:
        return 'C'
    else:
        return 'N'
    
def map_uom(weight_uom):
    if weight_uom.upper() == 'K':
        return 'KG'
    else:
        return weight_uom.upper()
    
def map_actual_weight(row):
    chargeable_weight = str(row['item_weight']).strip().replace(',', '.') if str(row['item_weight']).strip() != '' else '0'
    actual_weight = str(row['actual_weight']).strip().replace(',', '.') if str(row['actual_weight']).strip() != '' else '0'


    # Convert to float for comparison
    actual_weight_float = float(actual_weight)
    chargeable_weight_float = float(chargeable_weight)

    # Return the appropriate weight based on the conditions
    if actual_weight_float > 1:
        return actual_weight
    elif chargeable_weight_float > 1:
        return chargeable_weight
    else:
        return '1'


    
def map_receiver_port_code(row):
    # Check if Receiver_Country matches and assign hardcoded value
    if row['receiver_country'].upper() in ['HK', 'HK']:
        return 'HKG'
    
    # Check if Destination_Port_Code is in the cross reference dictionary
    if row['destination_port_code'] in tera_nxp_taiwan_portcode_cross_ref:
        return tera_nxp_taiwan_portcode_cross_ref[row['destination_port_code']]
    
    # Default to the first 5 characters of Destination_Port_Code
    return row['destination_port_code'][:5]

def map_bill_to_country(row):
    bill_to_country = row['bill_to_country']
    receiver_id = row['customer_id']
    
    # Check if the Bill_To_Country is in the mapping dictionary
    if receiver_id == '3960' and bill_to_country in tera_nxp_taiwan_country_cross_ref:
        return tera_nxp_taiwan_country_cross_ref[bill_to_country]
    elif receiver_id == '3960' and bill_to_country in ['GERMANY', 'Germany']:
        return 'DE'
    
    # Default case: Map first 2 characters of the Bill_To_Country in uppercase
    return bill_to_country[:2].upper()


#endregion

#region Shipper Port
def map_shipper_port(row):
    if row['shipper_city'].strip().upper() in ['INCHON', 'INCHEON']:
        return 'ICH'
    elif row['shipper_port_code'] in tera_nxp_taiwan_portcode_cross_ref:
        return tera_nxp_taiwan_portcode_cross_ref[row['shipper_port_code']]
    else:
        return row['shipper_port_code'][:5]
#endregion

#region Consignee Port
def map_consignee_port(row):
    if row['consignee_port_country_code'].strip().upper() in ['HK', 'HK']:
        return 'KKG'
    elif row['consignee_port_code'] in tera_nxp_taiwan_portcode_cross_ref:
        return tera_nxp_taiwan_portcode_cross_ref[row['consignee_port_code']]
    else:
        return row['consignee_port_code'][:5]
#endregion

#endregion

#region Process Tax
def process_tax(df_new, df_input):
    tax_type_result = np.where(
        (df_new['customer_id'] == '5918') & (df_new['carrier_scac'].isin(['UP19', 'UP49'])),
        'taxtype',
        'tax type'  # Default to 'tax type' for all other cases
        )

    tax_type_result = tax_type_result[0]

    taxable_base_amount_result = np.where(
        (df_new['customer_id'] == '5918') & (df_new['carrier_scac'].isin(['UP19', 'UP49'])),
        'taxablebaseamount',
        'taxable base amount'  # Default to 'taxable base amount' for all other cases
        )

    taxable_base_amount_result = taxable_base_amount_result[0]
   
    # Iterate through all columns of df_input
    for col in df_input.columns:
        if col.startswith(tax_type_result):
            billed_tax_amount_col = col.replace('type', 'amount')
            base_tax_code_col = col.replace(tax_type_result, 'iva_code')
            base_tax_amount_col = col.replace(tax_type_result, taxable_base_amount_result)
            
            if billed_tax_amount_col in df_input.columns:
                # Determine suffix for new columns based on index
                if col == tax_type_result:
                    suffix = ''
                else:
                    # Extract the number from the column name if it exists
                    suffix = f"_{col.split('.')[-1]}" 
                
                # Assign new columns in df_new
                df_new[f'tax_type_code{suffix}'] = df_input[col]
                df_new[f'billed_tax_amount{suffix}'] = df_input[billed_tax_amount_col]
                df_new[f'base_tax_code{suffix}'] = df_input[base_tax_code_col].apply(
                    lambda x: '' if pd.isna(x) else str(x)[:10]  # Limit to 10 characters
                )
                df_new[f'base_tax_amount{suffix}'] = df_input[base_tax_amount_col]

    if df_new['customer_id'].iloc[0] == '5918':
        df_new['base_tax_code'] = 'V'
    return df_new
#endregion Tax

#region Process Accessorials
def process_accessorials(df_new, df_input):
    accessorial_type_result = np.where(
        (df_new['customer_id'] == '5918') & (df_new['carrier_scac'].isin(['UP19', 'UP49'])), 
        'accessorialtype', 
        'accessorial type'  # Default to 'accessorial type' for all other cases
        )


    accessorial_type_result = accessorial_type_result[0]

    # Iterate through all columns of df_input
    for col in df_input.columns:
        if col.startswith(accessorial_type_result):
            # Determine corresponding charge column by replacing 'type' with 'charge'
            charge_col = col.replace('type', 'charge')
            
            if charge_col in df_input.columns:
                # Determine suffix for new columns based on index
                if col == accessorial_type_result:
                    suffix = ''
                else:
                    # Extract the number from the column name if it exists
                    suffix = f"_{col.split('.')[-1]}" 
                
                # Assign new columns in df_new
                df_new[f'accessorial_charge{suffix}'] = df_input[col]
                df_new[f'accessorial_charge_amount{suffix}'] = df_input[charge_col]

    # Apply the mapping logic and the 'FFS' -> 'FUE' transformation to all accessorial_charge columns at once
    accessorial_charge_cols = [col for col in df_new.columns if col.startswith('accessorial_charge') and not col.startswith('accessorial_charge_amount')]

    for col in accessorial_charge_cols:
        # Check if 'customer_id' is '3960'
        if (df_new['customer_id'] == '3960').any():
            # Step 1: Concatenate the 'carrier_scac' and 'accessorial_charge' values and check in the cross reference table
            concat_value = df_new['carrier_scac'] + "*" + df_new[col]
            df_new[col] = np.where(
                concat_value.isin(accessorial_cross_ref.keys()), 
                concat_value.map(accessorial_cross_ref),
                df_new[col]
            )

            # Step 2: Replace 'FFS' with 'FUE'
            df_new[col] = np.where(df_new[col] == "FSM", "FUE", df_new[col])
        elif (df_new['customer_id'] == '5918').any():  # Fixed the extra quote here
            # Concatenate the 'carrier_scac' and 'accessorial_charge' values and check in the cross reference table
            value = df_new[col]
            df_new[col] = np.where(
                value.isin(nxp_taiwan_flat_file_accessorial_type_crossref.keys()), 
                value.map(nxp_taiwan_flat_file_accessorial_type_crossref),
                df_new[col]
            )

        # Default: Map only the first 4 characters
        df_new[col] = df_new[col].str.slice(0, 4)

    return df_new
#endregion

#region Process Equipments
def process_equipments_Taiwan(df_new, df_input):
    equipment_type_result = np.where(
        df_new['carrier_scac'].isin(['UP19', 'UP49']), 
        'equipmenttype', 
        'equipment type'  # Default to 'equipment type' for all other cases
    )


    equipment_type_result = equipment_type_result[0]

    # Iterate through all columns of df_input
    for col in df_input.columns:
        if col.startswith(equipment_type_result):
            # Determine corresponding charge column by replacing 'type' with 'charge'
            number_col = col.replace('type', 'number')
            
            if number_col in df_input.columns:
                # Determine suffix for new columns based on index
                if col == equipment_type_result:
                    suffix = ''
                else:
                    # Extract the number from the column name if it exists
                    suffix = f"_{col.split('.')[-1]}" 
                
                # Assign new columns in df_new
                df_new[f'equipment_type{suffix}'] = df_input[col].str[:4]
                df_new[f'equipment_number{suffix}'] = df_input[number_col]

    return df_new

def process_equipments_Teradyne(df_new, df_input):
    # Assign new columns in df_new
    df_new['equipment_type'] = df_input['equipment type'].map(tera_nxp_taiwan_equipment_cross_ref).fillna('14').str[:4]
    df_new[f'equipment_number'] = df_input['equipment number']
    df_new['equipment_name'] = df_input['equipment type'].str[:4]
    
    return df_new
#end region

 #endregion