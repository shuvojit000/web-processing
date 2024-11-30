import pandas as pd
import os
import time
import math
# from process_data import *
from logger import create_logger
import traceback
from pathlib import Path
import numpy as np
from web_utilities import *

base_dir = Path(__file__).parent.resolve()

def process_web(row, index, formatted_rows):
    try:
        #region HDR
        sHDR = "HDR" + clean_string(str(row['system']))[:1] + '~'    # system
        sHDR +=  clean_string(str(row['customer_id']))[:4] + '~'    # customer_id
        sHDR += clean_string(str(row['carrier_scac']))[:50] + '~'        # carrier_scac
        sHDR += clean_string(str(row['pro_no']), True)[:25] + '~'              # pro_no
        sHDR += str(row['ship_date'])[:8] + '~'             # ship_date
        sHDR += to_float_100(row['billed_amount'])[:12] + '~'        # billed_amount
        sHDR += str(row['discount_amount'] or 0)[:12] + '~'         # discount_amount
        sHDR += str(row['discount_percent'] or 0)[:3] + '~'          # discount_percent
        sHDR += clean_string(str(row['prepaid_collect']))[:1] + '~'          # prepaid_collect
        sHDR += clean_string(str(row['interline_pro_no']))[:20] + '~'         # interline_pro_no
        sHDR += str(row['interline_date'])[:8] + '~'          # interline_date
        sHDR += clean_string(str(row['interline_scac']))[:4] + '~'          # interline_scac
        sHDR += str(row['delivery_date'])[:8] + '~'          # delivery_date
        sHDR += str(row['cod_amount'] or 0)[:12] + '~'         # cod_amount
        sHDR += clean_string(str(row['dept_number']))[:6] + '~'          # dept_number
        sHDR += clean_string(str(row['service_level']))[:30] + '~'         # service_level
        sHDR += str(row['pro_date'])[:8] + '~'          # pro_date
        sHDR += str(row['guaranteed_date'])[:8] + '~'          # guaranteed_date
        sHDR += clean_string(str(row['account_number']), True)[:30] + '~'         # account_number
        sHDR += clean_string(str(row['receipt_number']))[:30] + '~'         # receipt_number
        sHDR += clean_string(str(row['hdr_equipment_type']))[:2] + '~'         # equipment_type
        sHDR += str(row['trailer_size'] or 0)[:5] + '~'          # trailer_size
        sHDR += clean_string(str(row['currency_code']))[:3] + '~'             # currency_code
        sHDR += str(row['exchange_rate'] or 0)[:10] + '~'             # exchange_rate
        sHDR += clean_string(str(row['pricing_basis']))[:4] + '~'             # pricing_basis
        sHDR += clean_string(str(row['shipment_signed']).upper())[:30] + '~'         # shipment_signed
        sHDR += clean_string(str(row['transportation_mode_type']))[:3] + '~'         # transportation_mode_type
        sHDR += clean_string(str(row['cargo_type']))[:3] + '~'         # cargo_type
        sHDR += clean_string(str(row['hdr_accounting_code']))[:30] + '~'         # accounting_code
        sHDR += clean_string(str(row['image_name']), True, '._-')[:44] + '~'         # image_name
        sHDR += clean_string(str(row['vat_reg_number']),True)[:35] + '~'         # vat_reg_number
        sHDR += clean_string(str(row['client_ref_number']))[:20] + '~'         # client_ref_number
        sHDR += clean_string(str(row['created_by']))[:10]         # created_by
        formatted_rows.append(sHDR)
        #endregion HDR

        #region SHP
        sSHP = "SHP" + clean_string(str(row['shipper_name']).upper())[:30] + '~'  # shipper_name
        sSHP += clean_string(str(row['shipper_address']).upper())[:35]  + '~'         # shipper_address
        sSHP += clean_string(str(row['shipper_address2']).upper())[:35] + '~'         # shipper_address2
        sSHP += clean_string(str(row['shipper_city']).upper())[:60] + '~'         # shipper_city
        sSHP += clean_string(str(row['shipper_state']).upper())[:2] + '~'          # shipper_state
        sSHP += clean_string(str(row['shipper_zip']).upper(), True, '-')[:10] + '~'         # shipper_zip
        sSHP += clean_string(str(row['shipper_stop_sequence']).upper())[:2] + '~'          # shipper_stop_sequence
        sSHP += clean_string(str(row['shipper_country']).upper())[:2]         # shipper_country
        formatted_rows.append(sSHP)
        #endregion

        #region CON
        sCON = "CON" + clean_string(str(row['consignee_name']).upper())[:30] + '~'  # consignee_name
        sCON += clean_string(str(row['consignee_address']).upper())[:35]  + '~'         # consignee_address
        sCON += clean_string(str(row['consignee_address2']).upper())[:35]  + '~'         # consignee_address2
        sCON += clean_string(str(row['consignee_city']).upper())[:60] + '~'         # consignee_city
        sCON += clean_string(str(row['consignee_state']).upper())[:2] + '~'          # consignee_state
        sCON += clean_string(str(row['consignee_zip']).upper(), True, '-')[:10]  + '~'         # consignee_zip
        sCON += clean_string(str(row['consignee_stop_sequence']).upper())[:2]  + '~'          # consignee_stop_sequence
        sCON += clean_string(str(row['consignee_country']).upper())[:2]          # consignee_country
        formatted_rows.append(sCON)
        #endregion

        #region PO
        po_number_count = sum(1 for key in row.keys() if key.startswith('po_number'))

        for i in range(po_number_count):
            if i == 0:
                po_number = 'po_number'
                po_consignee_stop_sequence = 'po_consignee_stop_sequence'
            else:
                po_number = f'po_number_{i}'
                po_consignee_stop_sequence = f'po_consignee_stop_sequence_{i}'

            if row.get(po_number, '') not in [None, '']:
                sPUR = "PUR" + row.get(po_number, '').strip() + '~'  # po_number
                sPUR += row.get(po_consignee_stop_sequence, '').strip() + '~'         # consignee_stop_sequence
                formatted_rows.append(sPUR)

        #endregion

        #region BOL
        # sBOL = "BOL" + row['bill_of_lading_number'] + '~'  # bill_of_lading_number
        # sBOL += clean_string(row['shipper_stop_sequence'])        # shipper_stop_sequence
        # formatted_rows.append(sBOL)
        # Count BOL number
        bill_of_lading_number_count = sum(1 for key in row.keys() if key.startswith('bill_of_lading_number'))

        for i in range(bill_of_lading_number_count):
            if i == 0:
                bill_of_lading_number = 'bill_of_lading_number'
                bol_shipper_stop_sequence = 'bol_shipper_stop_sequence'
            else:
                bill_of_lading_number = f'bill_of_lading_number_{i}'
                bol_shipper_stop_sequence = f'bol_shipper_stop_sequence_{i}'

            if row.get(bill_of_lading_number, '') not in [None, '']:
                sBOL = "BOL" + row.get(bill_of_lading_number, '').strip() + '~'  # bill_of_lading_number
                sBOL += row.get(bol_shipper_stop_sequence, '').strip() + '~'         # bol_shipper_stop_sequence
                formatted_rows.append(sBOL)
        #endregion

        #region ITM
        sITM = "ITM" + str(row['line_item_number']) + '~'   # line_item_number
        sITM += clean_string(row['actual_class'])[:4] + '~'   # actual_class
        sITM += str(row['no_pieces'] or 0)[:8] + '~'            # no_pieces
        sITM += str(row['item_weight'] or 0)[:10] + '~'               # weight
        sITM += clean_string(str(row['item_desc']))[:50] + '~'         # item_desc
        sITM += clean_string(str(row['nmfc_item']))[:12] + '~'         # nmfc_item
        sITM += clean_string(str(row['product_code']))[:30] + '~'         # product_code
        sITM += clean_string(str(row['itm_accounting_code']))[:30] + '~'         # accounting_code_desc
        sITM += str(row['bill_qty'] or 0)[:18] + '~'         # bill_qty
        sITM += clean_string(str(row['bill_qty_uom']).upper())[:4] + '~'          # bill_qty_uom
        sITM += str(row['liquid_volume'] or 0)[:8] + '~'          # liquid_volume
        sITM += clean_string(str(row['liquid_volume_uom']).upper())[:2] + '~'          # liquid_volume_uom
        sITM += str(row['length'] or 0)[:8] + '~'         # length
        sITM += str(row['width'] or 0)[:8] + '~'         # width
        sITM += str(row['height'] or 0)[:8] + '~'         # height
        sITM += clean_string(str(row['dim_uom']).upper())[:2] + '~'          # dim_uom
        sITM += clean_string(str(row['dim_divisor']))[:30] + '~'         # dim_divisor
        sITM += str(row['dimensional_weight'] or 0)[:10] + '~'         # dimensional_weight
        sITM += str(row['item_shipper_stop_sequence'])[:2] + '~'         # shipper_stop_sequence
        sITM += str(row['item_consignee_stop_sequence'])[:2] + '~'         # consignee_stop_sequence
        sITM += clean_string(str(row['uom']).upper())[:3] + '~'         # uom
        sITM += clean_string(str(row['weight_uom']).upper())[:3] + '~'          # weight_uom
        sITM += str(row['container_size'])[:5] + '~'          # container_size
        sITM += clean_string(str(row['container_number']))[:20] + '~'         # container_number
        sITM += str(row['actual_weight'] or 0)[:10]       # actual_weight
        formatted_rows.append(sITM)
        #endregion

        #region Accessotial
        # Count accessorial charges
        accessorial_charge_count = sum(1 for key in row.keys() if key.startswith('accessorial_charge_amount'))

        for i in range(accessorial_charge_count):
            if i == 0:
                acc_charge_charge = 'accessorial_charge'
                acc_charge_amount = 'accessorial_charge_amount'
                acc_carrier_tax_code = 'accessorial_carrier_tax_code'
            else:
                acc_charge_charge = f'accessorial_charge_{i}'
                acc_charge_amount = f'accessorial_charge_amount_{i}'
                acc_carrier_tax_code = f'accessorial_carrier_tax_code_{i}'

            if row.get(acc_charge_charge, '') != '' and float(to_float_100(row.get(acc_charge_amount))) != 0:
                sACC = "ACC" + str(row.get(acc_charge_charge, ''))[:15] + '~'  # accessorial_charge
                sACC += to_float_100(row.get(acc_charge_amount)) + '~'         # accessorial_charge_amount
                sACC += str(row.get(acc_carrier_tax_code, ''))[:10].ljust(10)     # carrier_tax_code
                formatted_rows.append(sACC)
        #endregion

        #region PKG
        sPKG = "PKG" + str(row['package_number'])  # package_number
        formatted_rows.append(sPKG)
        #endregion

        #region NTE
        sNTE = "NTE" + str(row['note1']) + '~'  # note1
        sNTE += str(row['note2']) # note2
        formatted_rows.append(sNTE)
        #endregion

        #region ACT
        sACT = "ACT" + str(row['company_code'])[:12] + '~'  # company_code
        sACT += str(row['business_area'])[:8] + '~'          # business_area
        sACT += str(row['cost_center'])[:25] + '~'         # cost_center
        sACT += str(row['account_code'])[:20] + '~'         # account_code
        sACT += str(row['profit_center'])[:20] + '~'         # profit_center
        sACT += str(row['internal_order'])[:30] + '~'         # internal_order
        sACT += str(row['department'])[:30] + '~'         # department
        sACT += str(row['project_number'])[:15] + '~'         # project_number
        sACT += str(row['sub_account_code'])[:5] + '~'         # sub_account_code
        sACT += str(row['object_code'])[:4] + '~'         # object_code
        sACT += str(row['order_number_so'])[:9] + '~'         # order_number_so
        sACT += str(row['accounting_product_code'])[:6] + '~'         # accounting_product_code
        sACT += str(row['accounting_code_desc'])[:105] + '~'         # accounting_code_desc
        sACT += str(row['amount'] or 0)[:12] + '~'         # amount
        sACT += str(row['accounting_weight'])[:10]      # weight
        #endregion

        #region MSC
        sMSC = "MSC" + str(row['udf_1'])[:50] + '~'  # udf_1
        sMSC += str(row['udf_2'])[:50] + '~'         # udf_2
        sMSC += str(row['udf_3'])[:50] + '~'         # udf_3
        sMSC += str(row['udf_4'])[:50] + '~'         # udf_4
        sMSC += str(row['udf_5'])[:50] + '~'         # udf_5
        sMSC += str(row['udf_6'])[:50] + '~'         # udf_6
        sMSC += str(row['udf_7'])[:50] + '~'         # udf_7
        sMSC += str(row['udf_8'])[:50] + '~'         # udf_8
        sMSC += str(row['udf_9'])[:50] + '~'         # udf_9
        sMSC += str(row['udf_10'])[:50] + '~'         # udf_10
        sMSC += str(row['udf_11'])[:50] + '~'         # udf_11
        sMSC += str(row['udf_12'])[:50] + '~'         # udf_12
        sMSC += str(row['udf_13'])[:50] + '~'         # udf_13
        sMSC += str(row['udf_14'])[:50] + '~'         # udf_14
        sMSC += str(row['udf_15'])[:50] + '~'         # udf_15
        sMSC += str(row['udf_16'])[:50] + '~'         # udf_16
        sMSC += str(row['udf_17'])[:50] + '~'         # udf_17
        sMSC += str(row['udf_18'])[:50] + '~'         # udf_18
        sMSC += str(row['udf_19'])[:50] + '~'         # udf_19
        sMSC += str(row['udf_20'])[:50]        # udf_20
        formatted_rows.append(sMSC)
        #endregion

        #region TAX
        # Count tax charges

        tax_charge_count = sum(1 for key in row.keys() if 'tax_type_code' in key)
        for i in range(tax_charge_count):
            if i == 0:
                tax_type_code = 'tax_type_code'
                billed_tax_amount = 'billed_tax_amount'
                base_tax_code = 'base_tax_code'
                base_tax_amount = 'base_tax_amount'
            else:
                tax_type_code = f'tax_type_code_{i}'
                billed_tax_amount = f'billed_tax_amount_{i}'
                base_tax_code = f'base_tax_code_{i}'
                base_tax_amount = f'base_tax_amount_{i}'

            if (
                row.get(tax_type_code, '') != '' and
                (float(to_float_100(row.get(billed_tax_amount, 0))) != 0 or float(to_float_100(row.get(base_tax_amount, 0))) != 0)
            ):
                sTAX = "TAX" + str(row[tax_type_code]) + '~'   # tax_type_code
                sTAX += to_float_100(row[billed_tax_amount]) + '~'  # billed_tax_amount
                sTAX += str(row[base_tax_code]) + '~'          # carrier_tax_code
                sTAX += to_float_100(row[base_tax_amount])     # base_tax_amount
                formatted_rows.append(sTAX)

        #endregion

        #region ITL
        sITL = "ITL" + str(row['fb_interline_scac']) + '~'  # fb_interline_scac
        sITL += str(row['fb_interline_pro_no']) + '~'         # fb_interline_pro_no
        sITL += str(row['fb_interline_date']) + '~'          # fb_interline_date
        sITL += str(row['fb_interline_vessel']) + '~'         # fb_interline_vessel
        sITL += str(row['fb_interline_delivery_date']) + '~'          # fb_interline_delivery_date
        sITL += str(row['fb_interline_voyage_no'])         # fb_interline_voyage_no
        formatted_rows.append(sITL)
        #endregion

        #region FBM
        sFBM = "FBM" + clean_string(str(row['invoice_number']),True)[:25] + '~'  # invoice_number
        sFBM += str(row['invoice_date'])[:8] + '~'          # invoice_date
        sFBM += clean_string(str(row['master_bill_number']), True)[:25] + '~'         # master_bill_number
        sFBM += str(row['master_bill_date'])[:8] + '~'          # master_bill_date
        sFBM += str(row['service_type'])[:6] + '~'          # service_type
        sFBM += str(row['fbm_equipment_number'])[:20] + '~'         # equipment_number
        sFBM += str(row['customs_document_number'])[:25] + '~'         # customs_document_number
        sFBM += str(row['carrier_tax_code'])[:10] + '~'         # carrier_tax_code
        sFBM += to_float_100(row['total_invoice_amount'])[:12] + '~'         # total_invoice_amount
        sFBM += str(row['total_shipment_count'] or 0)[:6] + '~'          # total_shipment_count
        sFBM += clean_string(str(row['bill_to_name']).upper())[:30] + '~'         # bill_to_name
        sFBM += clean_string(str(row['bill_to_addr1']).upper())[:35] + '~'         # bill_to_addr1
        sFBM += clean_string(str(row['bill_to_addr2']).upper())[:35] + '~'         # bill_to_addr2
        sFBM += clean_string(str(row['bill_to_city']).upper())[:60] + '~'         # bill_to_city
        sFBM += clean_string(str(row['bill_to_state']).upper())[:2] + '~'          # bill_to_state
        sFBM += clean_string(str(row['bill_to_postal']).upper())[:10] + '~'         # bill_to_postal
        sFBM += clean_string(str(row['bill_to_country']).upper())[:2] + '~'          # bill_to_country
        sFBM += str(row['spot_quote_number'])        # spot_quote_number
        formatted_rows.append(sFBM)
        #endregion

        #region FSP
        sFSP = "FSP" + str(row['shipper_port_country_code'])[:2] + '~'   # shipper port_country_code
        sFSP += str(row['shipper_port_code'])[:5] + '~'          # shipper port_code
        sFSP += str(row['shipper_port_type'])[:3] + '~'          # shipper port_type
        sFSP += str(row['shipper_port_key'])[:12]         # shipper port_key
        formatted_rows.append(sFSP)
        #endregion

        #region FCP
        sFCP = "FCP" + str(row['consignee_port_country_code'])[:2] + '~'   # consignee port_country_code
        sFCP += str(row['consignee_port_code'])[:5] + '~'          # consignee port_code
        sFCP += str(row['consignee_port_type'])[:3] + '~'          # consignee port_type
        sFCP += str(row['consignee_port_key'])[:12]        # consignee port_key
        formatted_rows.append(sFCP)
        #endregion

        #region EQT
        equipment_type_count = sum(1 for key in row.keys() if 'equipment_type' in key)
        for i in range(equipment_type_count):
            if i == 0:
                equipment_type = 'equipment_type'
                equipment_number = 'equipment_number'
                equipment_name = 'equipment_name'
            else:
                equipment_type = f'equipment_type_{i}'
                equipment_number = f'equipment_number_{i}'
                equipment_name = f'equipment_name_{i}'

            if pd.notna(row.get(equipment_type)) and row.get(equipment_type) != '':
                sEQT = "EQT" + str(row[equipment_type]) + '~'   # equipment_type
                sEQT += str(row[equipment_number]) + '~'         # equipment_number
                sEQT += str(row[equipment_name]) + '~'         # equipment_name
                formatted_rows.append(sEQT)
        #endregion
        
        #region IVK
        sIVK = "IVK" + str(row['reload'])   # reload
        formatted_rows.append(sIVK)
        #endregion

        return formatted_rows
    except Exception as e:
        # Re-throw the exception to propagate it to the calling function
        raise

def process_partner_columns(mapping_file, partner_name):
    """
    Process the mapping file to create a dictionary of standard fields to partner columns.

    Parameters:
    mapping_file (str): Path to the Excel mapping file.
    partner_name (str): The partner name to get the corresponding columns.

    Returns:
    dict: A dictionary with standard fields as keys and partner columns as values.
    """
    # Read the Excel file into a DataFrame
    cols_df = pd.read_excel(mapping_file)

    # Extract the partner columns based on the partner name
    partner_columns = cols_df[partner_name].str.strip().str.lower()

    # Extract the list of fields from the DataFrame
    fields_list = cols_df['Fields'].tolist()

    # Initialize an empty dictionary to hold the column mappings
    column_mapping = {}

    # Iterate over the pairs of fields and partner columns
    for dcol, fcol in zip(fields_list, partner_columns):
        # Check if the partner column is NaN or empty
        if pd.isna(fcol) or fcol.strip() == '':
            column_mapping[dcol] = ''  # Assign an empty string for NaN or empty values
        else:
            column_mapping[dcol] = fcol.strip()  # Assign the actual value otherwise

    return column_mapping

