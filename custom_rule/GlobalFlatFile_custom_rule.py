import pandas as pd
import numpy as np
from datetime import datetime
from lookup.GlobalFlatFile_lookup import *
import re
from web_utilities import *

# Global Flat File Custom Rule
def GlobalFlatFile_Custom_Rule(df_new, df_input):
    try:
        # HDR
        df_new['system'] = 'E'
        df_new['customer_id'] = df_new['customer_id'].str[-4:]
        df_new = map_carrier_scac(df_new, df_input)
        df_new['pro_no'] = np.where(df_new['pro_no'] != '', df_new['pro_no'], df_new['master_bill_number'].where(df_new['master_bill_number'] != '', ''))
        df_new['prepaid_collect'] = df_new['prepaid_collect'].str[:1]
        df_new = map_service_level(df_new)
        df_new = map_account_number(df_new)
        df_new['hdr_equipment_type'] = np.where(
            (df_new['carrier_scac'] == 'GE15') & (df_new['service_level'] == 'TL'),
            '14',
            (df_new['carrier_scac'] + "*" + df_new['service_level']).map(equipment_type_cross_ref_1).fillna('')
            )

        df_new['trailer_size'] = np.where((df_new['customer_id'] == '5710') & (df_new['carrier_scac'] == 'PT24'), "14",  # Hardcode value
                                    np.where(
                                        (df_new['carrier_scac'] + '*' + df_new['equipment_type']).map(equipment_type_cross_ref_2).notna(),
                                        (df_new['carrier_scac'] + '*' + df_new['equipment_type']).map(equipment_type_cross_ref_2),
                                        df_new['equipment_type'].map(equipment_type_cross_ref_1).fillna('')
                                    )
                                )
        df_new['pricing_basis'] = np.where(df_new['carrier_scac'] == 'PRQE', 'CTP', 
                                    np.where(df_new['carrier_scac'] == 'SC23', 'FLT', 
                                        np.where(df_new['carrier_scac'] == 'SC36', 'SY', '')
                                    )
                                )
        
        df_new['hdr_accounting_code'] = '0'
        df_new = map_image_name(df_new)
        df_new['created_by'] = 'EDI'

        # SHP
        df_new['shipper_name'] = np.where(df_new['shipper_name'] != '', 
                                    df_new['shipper_name'], 
                                    df_new['shipper_misc'])
        
        df_new['shipper_address'] = np.where(df_new['shipper_address'] != "", df_new['shipper_address'], "NS")

        df_new['shipper_city'] = np.where(
            (df_new['carrier_scac'] + '*' + df_new['shipper_city']).map(shipper_city_cross_ref_1).notna(),
            (df_new['carrier_scac'] + '*' + df_new['shipper_city']).map(shipper_city_cross_ref_1),
            np.where(
                df_new['shipper_city'].map(shipper_city_cross_ref_2).notna(),
                df_new['shipper_city'].map(shipper_city_cross_ref_2),
                df_new['shipper_city']
            )
        )

        df_new['shipper_state'] = np.where(
            df_new['shipper_city'] == "CALEPPIO DI SETTALA", "NS",
            np.where(
                (df_new['carrier_scac'] == "MT36") & (df_new['consignee_country'] == "IT"), "NS",
                np.where(
                    df_new['shipper_state'] == '', "NS",
                    df_new['shipper_state'].str[:2]
                )
            )
        )

        df_new = map_shipper_zip(df_new)

        df_new['shipper_country'] = np.where(df_new['shipper_country'] != "", df_new['shipper_country'].str.upper(), "NS")

        # CON
        df_new['consignee_name'] = np.where(df_new['consignee_name'] != '', 
                                        df_new['consignee_name'], 
                                        df_new['consignee_misc'])   
        
        df_new['consignee_name'] = np.where((df_new['carrier_scac'] == 'GSON') | (df_new['carrier_scac'] == 'GE15'), 
                                    df_new['consignee_misc'], 
                                    df_new['consignee_name'])
        
        df_new['consignee_address'] = np.where(df_new['consignee_address'] != "", df_new['consignee_address'], "NS")

        df_new['consignee_city'] = np.where(
            (df_new['carrier_scac'] + "*" + df_new['consignee_city']).map(receiver_city_cross_ref_2).notnull(),
            (df_new['carrier_scac'] + "*" + df_new['consignee_city']).map(receiver_city_cross_ref_2),
            np.where(
                df_new['consignee_city'].map(receiver_city_cross_ref_1).notnull(),
                df_new['consignee_city'].map(receiver_city_cross_ref_1),
                df_new['consignee_city'].str.upper()
            )
        )

        df_new = map_consignee_state(df_new)
        df_new = map_consignee_zip(df_new)

        df_new['consignee_country'] = np.where(df_new['consignee_country'] != "", df_new['consignee_country'].str.upper(), "NS")

        # PUR
        df_new['po_number'] = np.where(
                    df_new['carrier_scac'].isin(['TDVM', 'GSON']), df_new['bill_of_lading_number'],
                    np.where(
                        df_new['po_number'] == '', 'NS',
                        df_new['po_number']
                    )
                )
        # Split 'po_number' based on both commas and colons
        po_split = df_new['po_number'].str.split(r'[,:;/]', expand=True)
        # Assign each split value to a new column dynamically
        for idx, col in enumerate(po_split.columns):
            if idx == 0:
                df_new['po_number'] = po_split[col]  # First column remains as 'po_number'
                df_new['po_consignee_stop_sequence'] = '0'  # Default stop sequence
            else:
                df_new[f'po_number_{idx}'] = po_split[col]  # Additional columns as 'po_number_1', 'po_number_2', etc.
                df_new[f'po_number_{idx}'] = np.where(
                    df_new['carrier_scac'].isin(['TDVM', 'GSON']), df_new['bill_of_lading_number'],
                    np.where(
                        df_new[f'po_number_{idx}'] == '', 'NS',
                        df_new[f'po_number_{idx}']
                    )
                )
                df_new[f'po_consignee_stop_sequence_{idx}'] = '0'  # Default stop sequence for additional columns

        

        # BOL
        df_new = map_bol_num(df_new)

        # ITM
        df_new['line_item_number'] = '1'
        df_new['no_pieces'] = pd.to_numeric(df_new['no_pieces'], errors='coerce').fillna(0).astype(int)
        df_new['no_pieces'] = np.where(df_new['no_pieces'] > 1, df_new['no_pieces'], '1')

        df_new['item_weight'] = pd.to_numeric(df_new['item_weight'], errors='coerce')
        df_new['actual_weight'] = pd.to_numeric(df_new['actual_weight'], errors='coerce')

        df_new['item_weight'] = np.where(
                df_new['carrier_scac'] == 'DS28',
                df_new['item_weight'].astype(str).str.strip().str.replace(',', ''),
                np.where(
                    df_new['item_weight'] > 1,
                    df_new['item_weight'].astype(str).str.strip().str.replace(',', ''),
                    np.where(
                        df_new['actual_weight'] > 1,
                        df_new['actual_weight'].astype(str).str.strip().str.replace(',', ''),
                        '1'
                    )
                )
            )


        df_new['bill_qty'] = pd.to_numeric(df_new['bill_qty'], errors='coerce').fillna(0).astype(float)
        df_new = map_bill_qty(df_new)
        df_new = map_bill_uom(df_new)

        df_new['dim_uom'] =  np.where(
            df_new['carrier_scac'] == 'EN02', 'C', 
            np.where(
                df_new['carrier_scac'].isin(['CS17', 'AA13']), 'LB',
                np.where(df_new['weight_uom'].isin(['KG', 'K']), 'C', 'N')
            )
        )

        df_new['uom'] = np.where(
            df_new['carrier_scac'].isin(['CS17', 'MKED']), 'LB', 
            np.where(
                df_new['weight_uom'] == 'K', 'KG',
                df_new['weight_uom'].str.upper()
            )
        )

        df_new['actual_weight'] = pd.to_numeric(df_new['actual_weight'], errors='coerce')
        df_new['item_weight'] = pd.to_numeric(df_new['item_weight'], errors='coerce')

        df_new['actual_weight'] = np.where(
                    df_new['actual_weight'] > 1,
                    df_new['actual_weight'].astype(str).str.strip().str.replace(',', ''),
                    np.where(
                        df_new['item_weight'] > 1,
                        df_new['item_weight'].astype(str).str.strip().str.replace(',', ''),
                        '1'
                    )
                )
        
        # Accessorials
        # check
        process_accessorials(df_new, df_input)

        # MSC
        df_new['udf_3'] = np.where(df_new['carrier_scac'] == "SC36", df_input['pod time'], df_new['udf_3'])
        df_new['udf_4'] = np.where(
            (df_new['carrier_scac'] == "EN02") & (df_new['bill_of_lading_number'] == "Retoure"), 
            "Y", 
            df_new['udf_4']
        )
        df_new = map_udf_5(df_new)
        df_new = map_udf_6(df_new)
        df_new['udf_8'] = np.where(
            (df_new['carrier_scac'] == "DS28") & (df_new['service_level'].isin(["WPLT", "PRD"])),
            df_new['bill_qty'],  # If Carrier_Code is "DS28" and Service_Level is "WPLT" or "PRD"
            np.where(
                df_new['customer_id'] == "5861",
                '',  # If Receiver_ID is "TSI5861" (map NULL, represented by np.nan here)
                df_new['udf_8']  # Default to UDF8 if none of the above conditions are met
            )
        )

        # TAX
        df_new = process_tax(df_new, df_input)

        # FBM
        df_new['invoice_number'] = np.where(
            df_new['invoice_number'] != '',
            df_new['invoice_number'],
            df_new['pro_no']
        )
        df_new['customs_document_number'] = np.where(df_new['carrier_scac'] == 'FT11', df_new['customs_document_number'], '')
        # Calculate total_invoice_amount
        df_new['billed_amount'] = pd.to_numeric(df_new['billed_amount'], errors='coerce')
        df_new['total_invoice_amount'] = df_new.groupby('invoice_number')['billed_amount'].transform('sum').round(2)

        # Calculate total_shipment_count
        df_new['total_shipment_count'] = df_new.groupby('invoice_number')['invoice_number'].transform('count')

        df_new['bill_to_addr1'] = np.where(df_new['bill_to_addr1']=='', 'NS', df_new['bill_to_addr1'])
        df_new['bill_to_country'] = np.where(df_new['bill_to_country']=='Germany', 'DE', df_new['bill_to_country'])

        # FSP
        df_new = map_shipper_port_code(df_new)

        # FCP
        df_new = map_consignee_port_code(df_new)

        # EQT
        df_new = process_equipment(df_new, df_input)

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

def replace_special_chars(value):
    return re.sub(r'[^A-Za-z0-9]', ' ', value) if pd.notnull(value) else ""

def map_carrier_scac(df_new, df_input):
    # Define conditions
    conditions = [
        (df_input['senderid'] == 'PC15'),  # First condition
        (df_input['senderid'] == 'OTDI') & (df_input['receiverid'] == 'TSI5694'),  # Second condition
        (df_input['senderid'] == 'FC10') & (df_input['receiverid'] == 'TSI5694')  # Third condition
    ]

    # Define corresponding choices for each condition
    choices = [
        'PDQC',  # For PC15
        'OTDN',  # For OTDI and TSI5694
        'USPL'   # For FC10 and TSI5694
    ]

    # Use np.select to apply the conditions and corresponding values
    df_new['carrier_scac'] = np.select(conditions, choices, default=df_new['carrier_scac'])

    return df_new



def map_service_level(df_new):
    # Conditions
    df_new['service_level'] = df_new['service_level'].str.upper()
    conditions = [
        (df_new['carrier_scac'] + "*" + df_new['service_level']).map(service_level_cross_ref_1).notna(),
        df_new['service_level'].map(service_level_cross_ref_2).notna(),  # If Service_Level in table 2
        (df_new['carrier_scac'] == "EN02") & (df_new['customer_id'] == "5822") & 
        (df_new['shipper_country'] == "DE") & (df_new['consignee_country'] == "DE"),  # Hardcode 'LTL'
        (df_new['carrier_scac'] == "EN02") & (df_new['customer_id'] == "5822") & 
        (df_new['shipper_country'] == "DE") & 
        ((df_new['shipper_city'] == "BOPPARD") | (df_new['shipper_city'] == "Boppard")) & (df_new['consignee_country'] != "DE"),  # Hardcode 'EXDL'
        (df_new['carrier_scac'] == "EN02") & (df_new['customer_id'] == "5822") & 
        (df_new['shipper_country'] == "DE") & (df_new['consignee_country'].isin(["CZ", "HU", "SK", "AU"])),  # Hardcode 'EXDL'
        (df_new['carrier_scac'] == "TC13") & (df_new['customer_id'] == "5694") & (df_new['service_level'] == "RU"),  # Map 'RUS'
        (df_new['carrier_scac'] == "TC13") & (df_new['customer_id'] == "5694") & (df_new['service_level'] == "DED"),  # Map 'VAN'
        ((df_new['carrier_scac'] == "DS54") & (df_new['customer_id'] == "5822") & (df_new['service_level'] == "SL1"))  # Map 'STD'
    ]

    # Choices
    choices = [
        (df_new['carrier_scac'] + "*" + df_new['service_level']).map(service_level_cross_ref_1),  # Get from dictionary 1
        df_new['service_level'].map(service_level_cross_ref_2),  # Lookup from table 2
        'LTL',  # Hardcoded value for LTL
        'EXDL',  # Hardcoded value for EXDL (shipper_city == Boppard)
        'EXDL',  # Hardcoded value for EXDL (consignee_country in CZ, HU, SK, AU)
        'RUS',  # Hardcoded value for RUS
        'VAN',  # Hardcoded value for VAN
        'AA5'   # Hardcoded value for STD
    ]

    # Default choice (map Service_Level in uppercase)
    default_choice = df_new['service_level'].str.upper()

    # Apply np.select to map the conditions
    df_new['service_level'] = np.select(conditions, choices, default=default_choice)

    return df_new

def map_account_number(df_new):
    conditions = [
        (df_new['carrier_scac'] == "GE15") & (df_new['receiverid'] == "5786") & (df_new['bill_to_postal'] == "64-920"),
        (df_new['carrier_scac'] == "GE15") & (df_new['receiverid'] == "5786") & (df_new['bill_to_postal'] == "95-200")
    ]

    # Choices (hardcoded values)
    choices = [
        "1845243166",  # When Carrier_Code = GE15, Receiver_ID = TSI5786, Bill_To_Postal = 64-920
        "1845243383"   # When Carrier_Code = GE15, Receiver_ID = TSI5786, Bill_To_Postal = 95-200
    ]

    default_choice = df_new['account_number']

    df_new['account_number'] = np.select(conditions, choices, default=default_choice)
    return df_new

def map_image_name(df_new):
    # Define conditions
    conditions = [
        (df_new['carrier_scac'] == 'EN02') & (df_new['customer_id'] == 'TSI5882'),
        df_new['image_name'].str.startswith('TSI_'),
        df_new['image_name'].str.startswith('NVG_'),
        df_new['image_name'].str.startswith('FPI_'),
        df_new['image_name'].str.startswith('TEK_'),
        df_new['carrier_scac'].isin(['FT11', 'FT12'])
    ]

    # Define corresponding choices
    choices = [
        df_new['carrier_scac'] + '_TSI5882_' + df_new['fb_interline_pro_no'] + '.PDF',
        df_new['image_name'].str.removeprefix('TSI_') + '.tif',
        df_new['image_name'].str.removeprefix('NVG_') + '.tif',
        df_new['image_name'].str.removeprefix('FPI_') + '.tif',
        df_new['image_name'].str.removeprefix('TEK_') + '.tif',
        df_new['image_name']
    ]

    # Default choice (when none of the conditions are met)
    default_choice = df_new['image_name']

    # Apply np.select to create the new column
    df_new['image_name'] = np.select(conditions, choices, default=default_choice)
    return df_new

def map_shipper_zip(df_new):
    conditions = [
        df_new['shipper_city'] == "CALEPPIO DI SETTALA",
        (df_new['carrier_scac'] == "JL06") & (df_new['shipper_city'].isin(["CARLOW", "DUBLIN"])),
        (df_new['carrier_scac'] == "JL06") & (df_new['shipper_zip'] != ''),
        df_new['carrier_scac'] == "JL06",
        df_new['shipper_country'] == "CA",
        (df_new['shipper_zip'] != '') & (df_new['shipper_zip'].str.len() == 4),
        df_new['shipper_zip'] != ''
    ]

    choices = [
        "20090",  # For Shipper_City == "CALEPPIO DI SETTALA"
        "01",     # For carrier_scac == "JL06" and Shipper_City is CARLOW or DUBLIN
        df_new['shipper_zip'].str.upper(),  # For carrier_scac == "JL06" and Shipper_Postal is Present
        "NS",     # For carrier_scac == "JL06" with no postal code present
        df_new['shipper_zip'].str[:3] + " " + df_new['shipper_zip'].str[-3:],  # For Shipper_Country == "CA"
        "0" + df_new['shipper_zip'].str.upper(),  # For Shipper_Postal's 5th character being null
        df_new['shipper_zip'].str.upper()  # For Shipper_Postal present
    ]

    df_new['shipper_zip'] = np.select(conditions, choices, default="NS")
    return df_new

def map_consignee_zip(df_new):
    conditions = [
        df_new['carrier_scac'].isin(["EN02", "DS28"]),
        df_new['consignee_city'] == "CALEPPIO DI SETTALA",
        (df_new['carrier_scac'] == "TS16") & (df_new['consignee_country'].isin(["SI", "BG"])),
        (df_new['carrier_scac'] + "*" + df_new['consignee_city']).map(postal_cross_ref).notna(),
        (df_new['carrier_scac'] == "JL06") & (df_new['consignee_zip'] != ''),
        (df_new['carrier_scac'] == "JL06") & (df_new['consignee_zip'] == ''),
        df_new['consignee_country'] == "CA",
        (df_new['consignee_zip'] != '') & (df_new['consignee_zip'].str.len() == 4),
        df_new['consignee_zip'] != ''
    ]

    choices = [
        df_new['consignee_zip'], # FOR carrier code in "EN02", "DS28"
        "20090",  # FOR consignee city = "CALEPPIO DI SETTALA",
        df_new['consignee_zip'],  # FOR carrier code = TS16 and consignee country in "SI", "BG"
        df_new['consignee_zip'].map(postal_cross_ref), # FOR carrier code concat consignee city map from postal_cross_ref
        df_new['consignee_zip'],  # For carrier_scac == "JL06" and consignee_zip is Present
        "NS",     # For carrier_scac == "JL06" with no consignee_zip present
        df_new['consignee_zip'].str[:3] + " " + df_new['consignee_zip'].str[-3:],  # For consignee_country == "CA"
        "0" + df_new['consignee_zip'],  # For consignee_zip's 5th character being null
        df_new['consignee_zip']  # For consignee_zip present
    ]

    df_new['consignee_zip'] = np.select(conditions, choices, default="NS")
    return df_new

def map_consignee_state(df_new):
    conditions = [
        (df_new['carrier_scac'] == 'MT36') & (df_new['consignee_country'] == 'IT'),
        df_new['carrier_scac'] == 'JL06',
        df_new['consignee_state'] == ''
    ]

    choices = ['NS', 'NS', 'NS']

    df_new['consignee_state'] = np.select(conditions, choices, default=df_new['consignee_state'].str[:2].str.upper())
    return df_new

def map_bol_num(df_new):
    # Define the conditions
    conditions = [
        (df_new['carrier_scac'] == 'GSON') & df_new['pro_no'].notna(),
        (df_new['carrier_scac'] == 'GSON') & df_new['fb_interline_vessel'].notna(),
        df_new['bill_of_lading_number'] == '',
        df_new['bill_of_lading_number'].notna() & (df_new['carrier_scac'] == 'DS28')
    ]

    # Define the corresponding choices for each condition
    choices = [
        df_new['pro_no'],
        df_new['fb_interline_vessel'],
        'NS',
        df_new['bill_of_lading_number'].str.lstrip('0')
    ]

    # Use np.select to apply the conditions and choices
    df_new['bill_of_lading_number'] = np.select(conditions, choices, default=df_new['bill_of_lading_number'])
    return df_new

def map_item_weight(df_new):
    # Define the conditions
    conditions = [
        df_new['carrier_scac'] == 'DS28',
        df_new['accounting_weight'].astype(float) > 1,
        df_new['item_weight'] > 1
    ]

    # Define the corresponding choices for each condition
    choices = [
        df_new['accounting_weight'],
        df_new['accounting_weight'],
        df_new['item_weight']
    ]

    # Use np.select to apply the conditions and choices
    df_new['item_weight'] = np.select(conditions, choices, default=df_new['item_weight'])
    return df_new

def map_bill_qty(df_new):
    # Define the conditions
    conditions = [
        df_new['carrier_scac'] == 'CF08',
        (df_new['carrier_scac'] == 'JL06') & (df_new['customer_id'] == '5861'),
        df_new['carrier_scac'] == 'EN02',
        df_new['carrier_scac'] == 'GE15',
        df_new['carrier_scac'].isin(['QUSI', 'TDVM']),
        df_new['carrier_scac'].isin(['CS17', 'AA13']),
        df_new['carrier_scac'].isin(['GSON', 'MA05', 'PC15', 'JR01', 'WE04', 'OTDI', 'NE14', 'BTSA', 'FC10']),
        (df_new['carrier_scac'] == 'MKED') & df_new['accounting_weight'].notnull(),
        (df_new['carrier_scac'] == 'MKED') & df_new['item_weight'].notnull()
    ]

    # Define the corresponding choices for each condition
    choices = [
        df_new['bill_qty'],
        df_new['bill_qty'],
        df_new['bill_qty'],
        df_new['item_weight'],
        df_new['no_pieces'],
        '1',  # Hardcoded value "1"
        df_new['item_weight'],
        df_new['actual_weight'],
        df_new['item_weight']
    ]

    # Use np.select to apply the conditions and choices
    df_new['bill_qty'] = np.select(conditions, choices, default=df_new['bill_qty'])
    return df_new

def map_bill_uom(df_new):
    # Define the conditions
    conditions = [
        df_new['carrier_scac'] == 'CF08',
        (df_new['carrier_scac'] == 'JL06') & (df_new['customer_id'] == '5861'),
        df_new['carrier_scac'] == 'EN02',
        df_new['carrier_scac'] == 'GE15',
        (df_new['carrier_scac'] == 'BS18') & (df_new['bill_qty_uom'] == 'CTNS'),
        df_new['carrier_scac'] == 'NCRI',
        df_new['carrier_scac'].isin(['GSON', 'CS17', 'AA13', 'WE04', 'OTDN', 'NE14', 'BTSA', 'FC10', 'MKED']),
        df_new['carrier_scac'].isin(['JR01', 'MA05', 'PC15'])
    ]

    # Define the corresponding choices
    choices = [
        'LDM',       # Hardcode "LDM" for 'CF08'
        'CTN',       # Hardcode "CTN" for 'JL06' and 'TSI5861'
        '',     # Hardcode null for 'EN02'
        'KG',        # Hardcode "KG" for 'GE15'
        'CA',        # Hardcode "CA" for 'BS18' and 'CTNS'
        'MIL',       # Hardcode "MIL" for 'NCRI'
        'LB',        # Hardcode "LB" for various carriers
        df_new['bill_qty_uom']  # Map "Weight_Uom" for certain carriers
    ]

    # Use np.select to apply the conditions and choices
    df_new['bill_qty_uom'] = np.select(conditions, choices, default=df_new['bill_qty_uom'])
    df_new['bill_qty_uom'] = df_new['bill_qty_uom'].str[:4]
    return df_new

def map_udf_5(df_new):
    conditions = [
        df_new['udf_5'] == "TAIPEI",
        df_new['udf_5'] == "SINGAPORE",
        df_new['udf_5'] == "INCHEON",
        df_new['udf_5'] == "HONG KONG PORT",
        df_new['udf_5'] == "HONG KONG",
        df_new['udf_5'] == "PUSAN"
    ]

    choices = ["TPE", "SIN", "ICH", "HKG", "HKG", "PUS"]

    df_new['udf_5'] = np.select(conditions, choices, default=df_new['udf_5'])
    return df_new

def map_udf_6(df_new):
    conditions = [
        df_new['customer_id'] == "5861",
        df_new['udf_6'] == "TAIPEI",
        df_new['udf_6'] == "SINGAPORE",
        df_new['udf_6'] == "INCHEON",
        df_new['udf_6'] == "HONG KONG PORT",
        df_new['udf_6'] == "HONG KONG",
        df_new['udf_6'] == "PUSAN"
    ]

    choices = [df_new['udf_6'], "TPE", "SIN", "ICH", "HKG", "HKG", "PUS"]

    df_new['udf_6'] = np.select(conditions, choices, default=df_new['udf_6'])
    return df_new

def map_accessorial_charge(df_new, df_input, type_col, charge_col, suffix):
    try:
        # Ensure charge_col is numeric
        df_input[charge_col] = df_input[charge_col].astype(str).str.replace(r'[$"]', '', regex=True)
        df_input[charge_col] = pd.to_numeric(df_input[charge_col], errors='coerce')
        
        # Define the conditions
        conditions = [
            df_input[type_col].isin(['400', 'AIR', 'MIN']),
            (df_new['carrier_scac'] == "GSON") & (df_input[type_col] == "20"),
            (df_input[type_col] == "FUE") & (df_input[charge_col] < 0.00),
            (df_new['carrier_scac'] == "PT24") & (df_input[type_col] == "FUE") & (df_input[charge_col] < 0.00),
            df_input[type_col] == "AWB"
        ]

        # Define the corresponding choices
        choices = [
            "",
            "ADC",
            "FDC",
            "FUE",
            "MWB"
        ]

        # Apply np.select with the defined conditions and choices
        df_new[f'accessorial_charge{suffix}'] = np.select(conditions, choices, default=df_input[type_col])
    
    except Exception as e:
        # Handle errors gracefully
        print(f"An error occurred while mapping accessorial charges: {e}")
    
    return df_new


def map_equipment(df_new, df_input, type_col, suffix):
    # Define the conditions
    conditions = [
        df_new['carrier_scac'] == "PT24",
        (df_new['carrier_scac'] == "GE15") & (df_new['service_level'] == "TL"),
        (df_new['carrier_scac'] + "*" + df_input[type_col]).map(equipment_type_cross_ref_2).notna(),
        df_input[type_col].map(equipment_type_cross_ref_1).notna()
    ]

    # Define the corresponding choices
    choices = [
        "14",
        "14",
        df_input[type_col].map(equipment_type_cross_ref_2),
        df_input[type_col].map(equipment_type_cross_ref_1)
    ]

    # Apply np.select with the defined conditions and choices
    df_new[f'equipment_type{suffix}'] = np.select(conditions, choices, default=df_input[type_col])
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

def map_shipper_port_code(df_new):
    conditions = [
        df_new['shipper_city'] == 'INCHEON',
        df_new['shipper_port_code'] == 'SHANGHAI',
        df_new['shipper_port_code'] == 'ISTANBUL',
        df_new['shipper_port_code'] == 'IZMIR',
        df_new['shipper_port_code'] == 'TAIPEI',
        df_new['shipper_port_code'] == 'SINGAPORE',
        df_new['shipper_port_code'] == 'INCHEON',
        df_new['shipper_port_code'] == 'HONG KONG PORT',
        df_new['shipper_port_code'] == 'HONG KONG',
        df_new['shipper_port_code'] == 'PUSAN'
    ]

    choices = ['ICH', 'SHA', 'IST', 'IZM', 'TPE', 'SIN', 'ICH', 'HKG', 'HKG', 'PUS']

    df_new['shipper_port_code'] = np.select(conditions, choices, default=df_new['shipper_port_code'])
    return df_new

def map_consignee_port_code(df_new):
    conditions = [
        df_new["consignee_port_code"] == "SHANGHAI",
        df_new["consignee_port_code"] == "ISTANBUL",
        df_new["consignee_port_code"] == "IZMIR",
        df_new["consignee_port_code"] == "TAIPEI",
        df_new["consignee_port_code"] == "SINGAPORE",
        df_new["consignee_port_code"] == "INCHEON",
        df_new["consignee_port_code"] == "HONG KONG PORT",
        df_new["consignee_port_code"] == "HONG KONG",
        df_new["consignee_port_code"] == "PUSAN",
        df_new["consignee_country"] == "HK"
    ]

    choices = ['SHA', 'IST', 'IZM', 'TPE', 'SIN', 'ICH', 'HKG', 'HKG', 'PUS', 'HKG']

    df_new['consignee_port_code'] = np.select(conditions, choices, default=df_new["consignee_port_code"])
    return df_new

