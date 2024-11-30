import pandas as pd
import numpy as np
from datetime import datetime
from lookup.BomiGroup_lookup import *
from web_utilities import *

#region Bomi Group Custom Rule
def BomiGroup_Custom_Rule(df_new, df_input):
    try:
        # HDR
        df_new['system'] = 'E'
        df_new['customer_id'] = '5748'
        df_new['carrier_scac'] = 'BG07'
        df_new['discount_amount'] = '000000000000'
        df_new['discount_percent'] = '000'
        df_new['prepaid_collect'] = 'P'
        df_new['service_level'] = df_new['service_level'].map(BomiGroup_service_lookup).fillna('STD')
        df_new['account_number'] = '100151'
        df_new['currency_code'] = 'EUR'
        df_new['hdr_accounting_code'] = '0'
        df_new['image_name'] = df_new['invoice_number'] + '.pdf'
        df_new['created_by'] = 'EDI'

        # SHP
        df_new['shipper_name'] = df_new['shipper_name'].str.upper()
        df_new['shipper_address'] = df_new['shipper_address'].str.upper()
        df_new['shipper_address2'] = 'NS'
        df_new['shipper_state'] = 'NS' 
        df_new['shipper_zip'] = np.where(df_new['shipper_zip'] != '', df_new['shipper_zip'], 'NS')

        # CON
        df_new['consignee_name'] = df_new['consignee_name'].str.upper()
        df_new['consignee_address'] = df_new['consignee_address'].str.upper()
        df_new['consignee_address2'] = 'NS'
        df_new['consignee_state'] = 'NS'
        df_new['consignee_zip'] = np.where(df_new['consignee_zip'] != '', df_new['consignee_zip'], 'NS')

        # ITM
        df_new['line_item_number'] = '1'
        df_new['no_pieces'] = pd.to_numeric(df_new['no_pieces'], errors='coerce').fillna(0).astype(int)
        df_new['no_pieces'] = np.where(df_new['no_pieces'] > 1, df_new['no_pieces'], '1')

        df_new['item_weight'] = pd.to_numeric(df_new['item_weight'], errors='coerce').fillna(0).astype(float)
        df_new['item_weight'] = np.where(df_new['item_weight'] > 1, df_new['item_weight'], '1')

        df_new['bill_qty'] = pd.to_numeric(df_new['bill_qty'], errors='coerce').fillna(0).astype(float)
        df_new['bill_qty'] = np.where(df_new['bill_qty'] > 1, df_new['bill_qty'], '1')

        df_new['bill_qty_uom'] = 'KG'
        df_new['liquid_volume']  ='00000000'
        df_new['length'] = '00000000' 
        df_new['width'] = '00000000' 
        df_new['height'] = '00000000'
        df_new['dimensional_weight'] = '0000000000'
        df_new['uom'] = 'KG'
        df_new['weight_uom'] = 'KG'
        df_new['actual_weight'] = pd.to_numeric(df_new['actual_weight'], errors='coerce').fillna(0).astype(float)
        df_new['actual_weight'] = np.where(df_new['actual_weight'] > 1, df_new['actual_weight'], '1')

        # TAX
        df_new['tax_type_code'] = 'VAT'
        df_new['base_tax_amount'] = '0'

        # FBM
        df_new['total_shipment_count'] = '1'
        df_new['bill_to_name'] = 'ILLUMINA NETHERLANDS BV'
        df_new['bill_to_addr1'] = 'FREDDY VAN RIEMSDIJKWEG 15'
        df_new['bill_to_addr2'] = 'NS'
        df_new['bill_to_city'] = 'EINDHOVEN'
        df_new['bill_to_state'] = 'NS'
        df_new['bill_to_postal'] = '5657 EE'
        df_new['bill_to_country'] = 'NL'

        # Format Dates
        df_new['ship_date'] = df_new['ship_date'].apply(lambda x: convert_to_yyyymmdd(x, '%d/%m/%Y'))
        df_new['delivery_date'] = df_new['delivery_date'].apply(lambda x: convert_to_yyyymmdd(x, '%d/%m/%Y'))
        df_new['pro_date'] = df_new['pro_date'].apply(lambda x: convert_to_yyyymmdd(x, '%d/%m/%Y'))
        df_new['interline_date'] = df_new['interline_date'].apply(lambda x: convert_to_yyyymmdd(x, '%d/%m/%Y'))
        df_new['guaranteed_date'] = df_new['guaranteed_date'].apply(lambda x: convert_to_yyyymmdd(x, '%d/%m/%Y'))
        df_new['invoice_date'] = df_new['invoice_date'].apply(lambda x: convert_to_yyyymmdd(x, '%d/%m/%Y'))
        df_new['master_bill_date'] = df_new['master_bill_date'].apply(lambda x: convert_to_yyyymmdd(x, '%d/%m/%Y'))

        df_new['billed_amount'] = pd.to_numeric(df_new['billed_amount'], errors='coerce')

        # Filter out rows where 'billed_amount' is 0
        df_new = df_new[df_new['billed_amount'] != 0]
        
        return df_new
    except Exception as e:
        # Re-throw the exception to propagate it to the calling function
        raise
#endregion