import pandas as pd
import numpy as np
from datetime import datetime
from logger import create_logger
from lookup.NXP_Thailand_lookup import *
from web_utilities import *
import configparser
import traceback

logger=create_logger(__name__)

#region NXP Thailand Custom Rule
def NXP_Thailand_Custom_Rule(df_new, df_input):
    try:
        # HDR
        df_new['system'] = 'E'
        df_new['customer_id'] = df_new['customer_id'].str[-4:]
        df_new['pro_no'] = np.where(df_new['pro_no'] != '', df_new['pro_no'], 
                                    df_new['master_bill_number'])            
        df_new['prepaid_collect'] = df_new['prepaid_collect'].str[:1]
        df_new = map_service_level(df_new)
        df_new = map_header_equipment_type(df_new)
        df_new = map_trailer_size(df_new)
        df_new['pricing_basis'] = np.where(df_new['carrier_scac'] == 'PRQE', 'CTP', '')
        df_new['shipment_signed'] = df_new['shipment_signed'].str[:20].str.upper()
        df_new['hdr_accounting_code'] = '0'
        df_new = map_image_name(df_new)  
        df_new['created_by'] = 'EDI'

        # SHP
        df_new = map_shipper_name(df_new)
        df_new['shipper_address'] = np.where(df_new['shipper_address'] != '', df_new['shipper_address'], 'NS')
        df_new = map_shipper_city(df_new)
        df_new = map_shipper_state(df_new)
        df_new = map_shipper_postal(df_new)
        df_new = map_shipper_country(df_new)


        # CON
        df_new = map_consignee_name(df_new)
        df_new['consignee_address'] = np.where(df_new['consignee_address'] != '', df_new['consignee_address'], 'NS')
        df_new = map_consignee_city(df_new)
        df_new = map_consignee_state(df_new)
        df_new = map_consignee_postal(df_new)
        df_new = map_consignee_country(df_new)

        # PUR
        df_new['po_number'] = np.where(df_new['po_number'] != '', df_new['po_number'], 'NS')
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
        df_new['bill_of_lading_number'] = np.where(df_new['bill_of_lading_number'] != '', df_new['bill_of_lading_number'], 'NS')
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
     
        # MSC
        df_new['udf_5'] = df_new['udf_5'].map(crosref_port_code).fillna(df_new['udf_5'])
        df_new['udf_6'] = df_new['udf_6'].map(crosref_port_code).fillna(df_new['udf_6'])

        # ITM
        df_new['line_item_number'] = '1'
        df_new = map_pieces(df_new)
        df_new = map_weight(df_new)
        df_new['item_desc'] = df_new['item_desc'].str[:50].str.upper()
        df_new['bill_qty_uom'] = df_new['bill_qty_uom'].str[:4].str.upper()

        df_new['length'] = np.where(df_new['length'] != '', df_new['length'], '0')
        df_new['length'] = np.abs(pd.to_numeric(df_new['length'], errors='coerce').fillna(0))

        df_new['width'] = np.where(df_new['width'] != '', df_new['width'], '0')
        df_new['width'] = np.abs(pd.to_numeric(df_new['width'], errors='coerce').fillna(0))

        df_new['height'] = np.where(df_new['height'] != '', df_new['height'], '0')
        df_new['height'] = np.abs(pd.to_numeric(df_new['height'], errors='coerce').fillna(0))

        df_new['dim_uom'] = np.where(df_new['dim_uom'].isin(['KG', 'K']), 'C', 'N')
        df_new['weight_uom'] = np.where(df_new['weight_uom'].isin(['KG', 'K']), 'KG', df_new['weight_uom'])

        df_new['dimensional_weight'] = np.where(df_new['dimensional_weight'] != '', df_new['dimensional_weight'], '0')
        df_new['dimensional_weight'] = np.abs(pd.to_numeric(df_new['dimensional_weight'], errors='coerce').fillna(0))

        df_new = map_uom(df_new)
        df_new = map_weight_uom(df_new)
        df_new['actual_weight'] = df_new.apply(map_actual_weight, axis=1)
        

        # Accessorials
        process_accessorials(df_new, df_input)

        # Tax
        process_tax(df_new, df_input)

        # FBM
        df_new['invoice_number'] = np.where(df_new['invoice_number'] != '', df_new['invoice_number'], 
                                    df_new['pro_no'])
        # Calculate total_invoice_amount
        df_new['total_invoice_amount'] = pd.to_numeric(df_new['total_invoice_amount'], errors='coerce')
        df_new['total_invoice_amount'] = df_new.groupby('invoice_number')['total_invoice_amount'].transform('sum').round(2)

        # Calculate total_shipment_count
        df_new['total_shipment_count'] = df_new.groupby('invoice_number')['invoice_number'].transform('count')

        df_new['bill_to_name'] = df_new['bill_to_name'].str.upper()
        df_new['bill_to_addr1'] = df_new['bill_to_addr1'].str.upper()
        df_new['bill_to_addr2'] = df_new['bill_to_addr2'].str.upper()
        df_new['bill_to_city'] = df_new['bill_to_city'].str.upper()
        df_new['bill_to_state'] = df_new['bill_to_state'].str.upper()
        df_new['bill_to_postal'] = df_new['bill_to_postal'].str.upper()
        df_new['bill_to_country'] = np.where(df_new['bill_to_country'] == "Germany", "DE", df_new['bill_to_country'].str[:3].str.upper())

        
        # EQT
        process_equipment(df_new, df_input)

        # FSP
        df_new['shipper_port_country_code'] = df_new['shipper_port_country_code'].str[:2]
        df_new['shipper_port_code'] = df_new['shipper_port_code'].map(crosref_port_code).fillna(df_new['shipper_port_code'].str[:5])

        # FCP
        df_new['consignee_port_country_code'] = df_new['consignee_port_country_code'].str[:2]
        df_new['consignee_port_code'] = df_new['consignee_port_code'].map(crosref_port_code).fillna(df_new['consignee_port_code'].str[:5])

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
        # Capture detailed error and stack trace
        error_message = f"Error in NXP_Thailand_Custom_Rule: {str(e)}\n" + traceback.format_exc()
        raise Exception(error_message)


def process_tax(df_new, df_input):
    # Iterate through all columns of df_input
    customer_id = df_new['customer_id'].iloc[0]
    for col in df_input.columns:
        if col.startswith('tax type'):
            # Determine corresponding charge column by replacing 'type' with 'amount'
            tax_amount_col = col.replace('type', 'amount')
            iva_code_col = col.replace('tax type', 'iva code')
            iva_code_col = col.replace('tax type', 'iva code') if customer_id == '5673' else col.replace('tax type', 'iva_code')
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


#region HDR
def map_service_level(df_new):
    # Conditions
    conditions = [
        (df_new['customer_id'] == '5673') & df_new['service_level'].map(service_level_cross_ref).notna(),
        (df_new['customer_id'] == '5920"') & (df_new['carrier_scac'] + "*" + df_new['service_level']).map(nxp_thailand_flat_service_level_cross_ref1).notna(),
        (df_new['customer_id'] == '5920') & df_new['service_level'].map(nxp_thailand_flat_service_level2).notna()
    ]

    # Choices
    choices = [
        df_new['service_level'].map(service_level_cross_ref),
        df_new['service_level'].map(nxp_thailand_flat_service_level_cross_ref1),
        df_new['service_level'].map(nxp_thailand_flat_service_level2)
    ]

    # Default choice (map Service_Level in uppercase)
    default_choice = df_new['service_level'].str.upper()

    # Apply np.select to map the conditions
    df_new['service_level'] = np.select(conditions, choices, default=default_choice)

    return df_new

def map_header_equipment_type(df_new):
    # Conditions
    conditions = [
        (df_new['customer_id'] == '5673') & (df_new['equipment_type'].map(equipment_cross_ref).notna()),
        (df_new['customer_id'] == '5920') & (df_new['equipment_type'].str.contains('&', na=False)),
        (df_new['equipment_type'].isna()) | (df_new['equipment_type'] == ''),
        df_new['equipment_type'].notna()
    ]

    # Choices
    choices = [
        df_new['equipment_type'].map(equipment_cross_ref),
        '&' + df_new['equipment_type'].str.extract(r'&\s*(.*)', expand=False).fillna(''),
        "00000",
        df_new['equipment_type'].str.zfill(5) 
    ]

    df_new['equipment_type'] = np.select(conditions, choices, default=df_new['equipment_type'].str.upper())

    return df_new


def map_trailer_size(df_new):
    # Conditions
    conditions = [
        (df_new['customer_id'] == '5673') & (df_new['trailer_size'].map(equipment_cross_ref).notna()),
        (df_new['customer_id'] == '5920') & (df_new['trailer_size'].str.contains('&', na=False)),
        (df_new['trailer_size'].isna()) | (df_new['trailer_size'] == ''),
        df_new['trailer_size'].notna()
    ]

    # Choices
    choices = [
        df_new['trailer_size'].map(equipment_cross_ref),
        '&' + df_new['trailer_size'].str.extract(r'&\s*(.*)', expand=False).fillna(''),
        "00000",
        df_new['trailer_size'].str.zfill(5)
    ]

    df_new['trailer_size'] = np.select(conditions, choices, default=df_new['trailer_size'].str.upper())

    return df_new

    
def map_image_name(df_new):
    # Define conditions for each prefix
    conditions = [
        df_new['image_name'].str.startswith("TSI_"),
        df_new['image_name'].str.startswith("NVG_"),
        df_new['image_name'].str.startswith("FPI_"),
        df_new['image_name'].str.startswith("TEK_")
    ]
    
    # Define choices corresponding to each condition
    choices = [
        (df_new['image_name'].str[4:] + ".tif").str[:45],  # Remove prefix and append ".tif"
        (df_new['image_name'].str[4:] + ".tif").str[:45],
        (df_new['image_name'].str[4:] + ".tif").str[:45],
        (df_new['image_name'].str[4:] + ".tif").str[:45]
    ]
    
    # Default choice: first 45 characters of image_name
    default_choice = df_new['image_name'].str[:45]

    # Apply np.select to map the conditions and choices to the image_name column
    df_new['image_name'] = np.select(conditions, choices, default=default_choice)
    
    return df_new
    
#endregion

#region Shipper
def map_shipper_name(df_new):
    # Define conditions
    conditions = [
        df_new['shipper_name'].notna() & (df_new['shipper_name'].str.strip() != ''),  # If shipper_name is present and not empty
        df_new['shipper_misc'].notna() & (df_new['shipper_misc'].str.strip() != '')  # If shipper_name is missing, check shipper_misc
    ]
    
    # Define choices corresponding to each condition
    choices = [
        df_new['shipper_name'].str.upper(),  # Uppercase shipper_name if it's present
        df_new['shipper_misc'].str.upper()  # Uppercase shipper_misc if shipper_name is missing
    ]
    
    # Default value if none of the conditions match
    default_choice = 'NS'
    
    # Apply np.select to map the conditions and choices to the shipper_name column
    df_new['shipper_name'] = np.select(conditions, choices, default=default_choice)
    
    return df_new


def map_shipper_city(df_new):
    # Define conditions
    conditions = [
        df_new['shipper_city'].isin(cross_reference_city_ct),  # If shipper_city is in cross_reference_city_ct
        df_new['shipper_city'] != ''  # If shipper_city is not empty
    ]
    
    # Define choices corresponding to each condition
    choices = [
        df_new['shipper_city'].map(cross_reference_city_ct),  # Map shipper_city based on cross_reference_city_ct
        df_new['shipper_city']  # Return the original shipper_city if not in cross_reference_city_ct
    ]
    
    # Default value if none of the conditions match
    default_choice = 'NS'
    
    # Apply np.select to map the conditions and choices to the shipper_city column
    df_new['shipper_city'] = np.select(conditions, choices, default=default_choice)
    
    return df_new


def map_shipper_state(df_new):
    # Define conditions
    conditions = [
        df_new['shipper_city'] == 'CALEPPIO DI SETTALA',  # Condition if shipper_city is 'CALEPPIO DI SETTALA'
        df_new['shipper_state'] != ''  # Condition if shipper_state is not empty
    ]
    
    # Define choices corresponding to each condition
    choices = [
        'NS',  # Set to 'NS' if shipper_city is 'CALEPPIO DI SETTALA'
        df_new['shipper_state']  # Return the original shipper_state if not empty
    ]
    
    # Default value if none of the conditions match
    default_choice = 'NS'
    
    # Apply np.select to map the conditions and choices to the shipper_state column
    df_new['shipper_state'] = np.select(conditions, choices, default=default_choice)
    
    return df_new

    
def map_shipper_postal(df_new):
    # Extract and strip postal code while handling NaN
    df_new['shipper_zip'] = df_new['shipper_zip'].str.strip().str.upper()
    
    # Create a variable for the postal code to use in conditions
    postal = df_new['shipper_zip'].fillna('')  # Fill NaN with empty string for easier handling
    
    # Define conditions
    conditions = [
        df_new['shipper_city'].str.upper() == 'CALEPPIO DI SETTALA',  # Condition 1: Shipper_City is 'CALEPPIO DI SETTALA'
        df_new['shipper_country'].str.upper() == 'CA',  # Condition 2: Shipper_Country is 'CA'
        (postal.str.len() >= 5) & (postal.str[4] == ''),  # Condition 3: 5th character is empty
        postal != ''  # Condition 4: Postal code is present
    ]
    
    # Define choices corresponding to each condition
    choices = [
        '20090',  # Return hardcoded postal code for Condition 1
        postal.str[:3] + ' ' + postal.str[-3:],  # Return mapped postal code for Condition 2
        '0' + postal,  # Map postal code with a leading zero for Condition 3
        postal  # Return original postal code in uppercase for Condition 4
    ]
    
    # Default value if none of the conditions match
    default_choice = 'NS'
    
    # Apply np.select to map the conditions and choices to the shipper_postal column
    df_new['shipper_zip'] = np.select(conditions, choices, default=default_choice)
    
    return df_new



def map_shipper_country(df_new):
    # Define conditions
    conditions = [
        df_new['shipper_country'].str.strip() != ''  # Condition: shipper_country is not empty
    ]
    
    # Define choices corresponding to each condition
    choices = [
        df_new['shipper_country'].str.upper()  # Return uppercase shipper_country
    ]
    
    # Default value if none of the conditions match
    default_choice = 'NS'
    
    # Apply np.select to map the conditions and choices to the shipper_country column
    df_new['shipper_country'] = np.select(conditions, choices, default=default_choice)
    
    return df_new


#endregion

#region Consignee
def map_consignee_name(df_new):
    # Define conditions
    conditions = [
        df_new['consignee_name'].notna() & (df_new['consignee_name'].str.strip() != ''),  # Condition: consignee_name is present and not empty
        df_new['consignee_misc'].str.strip() != ''  # Condition: consignee_misc is not empty
    ]
    
    # Define choices corresponding to each condition
    choices = [
        df_new['consignee_name'].str.upper(),  # Return uppercase consignee_name
        df_new['consignee_misc'].str.upper()  # Return uppercase consignee_misc
    ]
    
    # Default value if none of the conditions match
    default_choice = 'NS'
    
    # Apply np.select to map the conditions and choices to the consignee_name column
    df_new['consignee_name'] = np.select(conditions, choices, default=default_choice)
    
    return df_new


def map_consignee_city(df_new):
    # Define conditions
    conditions = [
        df_new['consignee_city'].isin(cross_reference_city_ct.keys()),  # Condition: consignee_city is in the cross-reference
        df_new['consignee_city'] != ''  # Condition: consignee_city is not empty
    ]
    
    # Define choices corresponding to each condition
    choices = [
        df_new['consignee_city'].map(cross_reference_city_ct),  # Return mapped value from cross_reference_city_ct
        df_new['consignee_city']  # Return original consignee_city
    ]
    
    # Default value if none of the conditions match
    default_choice = 'NS'
    
    # Apply np.select to map the conditions and choices to the consignee_city column
    df_new['consignee_city'] = np.select(conditions, choices, default=default_choice)
    
    return df_new

def map_consignee_state(df_new):
    # Define conditions
    conditions = [
        df_new['consignee_state'].str.strip() != ''  # Condition: shipper_state is not empty
    ]
    
    # Define choices corresponding to each condition
    choices = [
        df_new['consignee_state'].str[:2].str.upper()  # Return the first 2 characters of shipper_state in uppercase
    ]
    
    # Default value if none of the conditions match
    default_choice = 'NS'
    
    # Apply np.select to map the conditions and choices to the consignee_state column
    df_new['consignee_state'] = np.select(conditions, choices, default=default_choice)
    
    return df_new

    
def map_consignee_postal(df_new):
    # Extract and strip postal code, create a new column for processed postal codes
    df_new['consignee_zip'] = df_new['consignee_zip'].str.strip().str.upper().fillna('')
    
    # Define conditions
    conditions = [
        df_new['consignee_city'].str.upper() == 'CALEPPIO DI SETTALA',  # Condition 1: Check for specific city
        df_new['consignee_country'].str.upper() == 'CA',  # Condition 2: Check for country 'CA'
        (df_new['consignee_zip'].str.len() >= 5) & (df_new['consignee_zip'].str[4] == ''),  # Condition 3: 5th character check
        df_new['consignee_zip'] != ''  # Condition 4: Check if postal code is present
    ]
    
    # Define choices corresponding to each condition
    choices = [
        '20090',  # Hardcoded value for Condition 1
        df_new['consignee_zip'].str[:3] + ' ' + df_new['consignee_zip'].str[-3:],  # Formatted postal code for Condition 2
        '0' + df_new['consignee_zip'],  # Add a leading zero for Condition 3
        df_new['consignee_zip']  # Original postal code for Condition 4
    ]
    
    # Default value if none of the conditions match
    default_choice = 'NS'
    
    # Apply np.select to map the conditions and choices to the consignee_postal column
    df_new['consignee_zip'] = np.select(conditions, choices, default=default_choice)
    
    return df_new


def map_consignee_country(df_new):
    # Define conditions
    conditions = [
        df_new['consignee_country'].str.strip() != ''  # Condition: consignee_country is not empty
    ]
    
    # Define choices corresponding to each condition
    choices = [
        df_new['consignee_country'].str[:3].str.upper()  # Return the first 3 characters of consignee_country in uppercase
    ]
    
    # Default value if none of the conditions match
    default_choice = 'NS'
    
    # Apply np.select to map the conditions and choices to the consignee_country column
    df_new['consignee_country'] = np.select(conditions, choices, default=default_choice)
    
    return df_new


#endregion

#region Item
def map_pieces(df_new):
    # Ensure 'no_pieces' is treated as numeric
    df_new['no_pieces'] = pd.to_numeric(df_new['no_pieces'], errors='coerce').fillna(0)

    # Define conditions
    conditions = [
        df_new['no_pieces'] > 1  # Condition: no_pieces greater than 1
    ]
    
    # Define choices corresponding to each condition
    choices = [
        df_new['no_pieces'].round()  # Round the value of no_pieces
    ]
    
    # Default value if none of the conditions match
    default_choice = 1  # If pieces <= 1, return 1
    
    # Apply np.select to map the conditions and choices to the pieces column
    df_new['no_pieces'] = np.select(conditions, choices, default=default_choice)
    
    return df_new


def clean_weight(weight):
    # Trim spaces from right and replace "," with "."
    weight = weight.strip().replace(',', '.')
    # Replace special characters using regex
    weight = re.sub(r'((?:(\.\d*?[1-9]+)|\.)0*$)', '', weight)
    return weight

def map_weight(df_new):
    # Convert chargeable_weight and actual_weight to strings and replace commas with dots
    df_new['item_weight'] = df_new['item_weight'].astype(str).str.strip().replace(r'^\s*$', '0', regex=True).str.replace(',', '.')
    df_new['actual_weight'] = df_new['actual_weight'].astype(str).str.strip().replace(r'^\s*$', '0', regex=True).str.replace(',', '.')


    # Convert to float for comparison
    df_new['item_weight'] = df_new['item_weight'].astype(float)
    df_new['actual_weight'] = df_new['actual_weight'].astype(float)

    # Define conditions
    conditions = [
        df_new['item_weight'] > 1,  # Condition: chargeable_weight greater than 1
        df_new['actual_weight'] > 1        # Condition: actual_weight greater than 1
    ]
    
    # Define choices corresponding to each condition
    choices = [
        df_new['item_weight'],         # Return chargeable_weight if condition 1 is met
        df_new['actual_weight']        # Return actual_weight if condition 2 is met
    ]
    
    # Default value if none of the conditions match
    default_choice = '1'  # If both weights are <= 1, return '1'
    
    # Apply np.select to map the conditions and choices to the weight column
    df_new['item_weight'] = np.select(conditions, choices, default=default_choice)
    
    
    return df_new


    
    
def map_uom(df_new):
    # Convert weight_uom to uppercase
    df_new['uom'] = df_new['uom'].fillna('').str.upper()

    # Define conditions
    conditions = [
        df_new['uom'] == 'K'  # Condition: weight_uom equals 'K'
    ]
    
    # Define choices corresponding to each condition
    choices = [
        'KG'  # Return 'KG' if condition is met
    ]
    
    # Default value: return the original weight_uom in uppercase
    default_choice = df_new['uom']

    # Apply np.select to map the conditions and choices to the uom column
    df_new['uom'] = np.select(conditions, choices, default=default_choice)
    
    return df_new

def map_weight_uom(df_new):
    # Convert weight_uom to uppercase
    df_new['weight_uom'] = df_new['weight_uom'].fillna('').str.upper()

    # Define conditions
    conditions = [
        df_new['weight_uom'] == 'K'  # Condition: weight_uom equals 'K'
    ]
    
    # Define choices corresponding to each condition
    choices = [
        'KG'  # Return 'KG' if condition is met
    ]
    
    # Default value: return the original weight_uom in uppercase
    default_choice = df_new['weight_uom']

    # Apply np.select to map the conditions and choices to the uom column
    df_new['weight_uom'] = np.select(conditions, choices, default=default_choice)
    
    return df_new

    
def map_actual_weight(row):
    # Convert actual_weight and chargeable_weight to strings and replace commas with dots
    actual_weight = str(row['actual_weight']).strip().replace(',', '.') if row['actual_weight'] is not None else '0'
    chargeable_weight = str(row['item_weight']).strip().replace(',', '.') if row['item_weight'] is not None else '0'

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

def map_accessorial_charge(df_new, df_input, type_col, charge_col, suffix):
    # Define the conditions
    df_input[charge_col] = pd.to_numeric(df_input[charge_col], errors='coerce')
    conditions = [
        df_input[type_col].isin(['400', 'AIR', 'MIN']),
        (df_new['customer_id'] == "5673") & (df_input[type_col] == 'FUE') & (df_input[charge_col] < 0.00),
        (df_new['customer_id'] == "5920") & (df_input[type_col] == 'DOC'),
        (df_new['carrier_scac'] == "5920") & (df_input[type_col] == "OAL")
    ]

    # Define the corresponding choices
    choices = [
        "",
        "FDC",
        "HDC",
        "998"
    ]

    # Apply np.select with the defined conditions and choices
    df_new[f'accessorial_charge{suffix}'] = np.select(conditions, choices, default=df_input[type_col])
    return df_new

def process_accessorials(df_new, df_input):
    # Iterate through all columns of df_input
    # tax_code_counter = 1  # For accessorial_carrier_tax_code
    for col in df_input.columns:
        if col.startswith('accessorial type'):
            # Determine corresponding charge column by replacing 'type' with 'charge'
            charge_col = col.replace('type', 'charge')
            # acc_carr_tax_code_col = col.replace(col, f'tax code.{tax_code_counter}')
            
            if charge_col in df_input.columns:
                # Determine suffix for new columns based on index
                if col == 'accessorial type':
                    suffix = ''
                else:
                    # Extract the number from the column name if it exists
                    suffix = f"_{col.split('.')[-1]}" 
                
                # Assign new columns in df_new
                map_accessorial_charge(df_new, df_input, col, charge_col, suffix)
                df_new[f'accessorial_charge_amount{suffix}'] = df_input[charge_col]    
                # df_new[f'accessorial_carrier_tax_code{suffix}'] = df_input[acc_carr_tax_code_col]

                # Increment the tax code counter for the next accessorial type
                # tax_code_counter += 1

    return df_new

def map_equipment(df_new, df_input, type_col, suffix):
    df_input[type_col] = df_input[type_col].str[:4]
    # Define the conditions
    conditions = [
        df_input[type_col].map(equipment_cross_ref).notna()
    ]

    # Define the corresponding choices
    choices = [
        df_input[type_col].map(equipment_cross_ref)
    ]

    # Apply np.select with the defined conditions and choices
    df_new[f'equipment_type{suffix}'] = np.select(conditions, choices, default=df_input[type_col])
    return df_new

def process_equipment(df_new, df_input):
    # Iterate through all columns of df_input
    for col in df_input.columns:
        if col.startswith('equipment type'):
            # Determine corresponding charge column by replacing 'type' with 'charge'
            number_col = col.replace('type', 'number')           
            if col in df_input.columns:
                # Determine suffix for new columns based on index
                if col == 'equipment type':
                    suffix = ''
                else:
                    # Extract the number from the column name if it exists
                    suffix = f"_{col.split('.')[-1]}" 
                
                # Assign new columns in df_new
                map_equipment(df_new, df_input, col, suffix)
                df_new[f'equipment_number{suffix}'] = df_input[number_col]    
    return df_new

 #endregion