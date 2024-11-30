import logging
from web_utilities import *
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_web_carrier(inputdf):
    inputdf = inputdf.replace({'None': ''})
    inputdf['line_item_number'] = '0'
    inputdf['shipper_stop_sequence'] = inputdf['shipper_stop_sequence'].fillna("00")
    inputdf['consignee_stop_sequence'] = inputdf['consignee_stop_sequence'].fillna("00")

    inputdf['actual_class'] = pd.to_numeric(inputdf['actual_class'], errors='coerce').fillna(0).astype(int).astype(str)
    inputdf['equipment_type'] = pd.to_numeric(inputdf['equipment_type'], errors='coerce').fillna(0).astype(int).astype(str)

    inputdf['no_pieces'] = pd.to_numeric(inputdf['no_pieces'], errors='coerce').fillna(0).astype(int)
    inputdf['item_weight'] = pd.to_numeric(inputdf['item_weight'], errors='coerce').fillna(0).astype(int)
    inputdf['bill_qty'] = pd.to_numeric(inputdf['bill_qty'], errors='coerce').fillna(0).astype(int)

    inputdf['ship_date'] = inputdf['ship_date'].apply(lambda x: convert_to_yyyymmdd(x, '%m/%d/%Y'))
    inputdf['delivery_date'] = inputdf['delivery_date'].apply(lambda x: convert_to_yyyymmdd(x, '%m/%d/%Y'))
    inputdf['pro_date'] = inputdf['pro_date'].apply(lambda x: convert_to_yyyymmdd(x, '%m/%d/%Y'))
    inputdf['interline_date'] = inputdf['interline_date'].apply(lambda x: convert_to_yyyymmdd(x, '%m/%d/%Y'))
    inputdf['guaranteed_date'] = inputdf['guaranteed_date'].apply(lambda x: convert_to_yyyymmdd(x, '%m/%d/%Y'))
    inputdf['invoice_date'] = inputdf['invoice_date'].apply(lambda x: convert_to_yyyymmdd(x, '%m/%d/%Y'))
    inputdf['master_bill_date'] = inputdf['master_bill_date'].apply(lambda x: convert_to_yyyymmdd(x, '%m/%d/%Y'))

    # Process accessorial charges, taxes, equipment details, and PO details for each customer_freight_bill_key
    processed_rows = []
    for _, group in inputdf.groupby('carrier_freight_bill_key'):
        processed_row = add_accessorial_columns(group)
        processed_row = add_tax_columns(group)  # Process tax columns similarly
        processed_row = add_equipment_columns(group)  # Process equipment columns
        processed_row = add_po_columns(group)  # Process PO number and stop sequence
        processed_row = add_bol_columns(group)  # Process Bill of Lading number and stop sequence
        processed_rows.append(processed_row)
    return processed_rows

# Function to process accessorial charges dynamically
def add_accessorial_columns(group_df):
    charge_cols = ['accessorial_charge', 'accessorial_charge_amount']
    distinct_charges = group_df[charge_cols].drop_duplicates()
    result = group_df.iloc[0].copy()
    existing_charges = set(result.filter(regex='^accessorial_charge').dropna().values)
    
    for idx, (charge, amount) in enumerate(distinct_charges.itertuples(index=False), start=1):
        if charge not in existing_charges:
            result[f'accessorial_charge_{idx}'] = charge
            result[f'accessorial_charge_amount_{idx}'] = amount
            existing_charges.add(charge)
    
    return result

# Function to process tax type codes and billed amounts dynamically
def add_tax_columns(group_df):
    tax_cols = ['tax_type_code', 'billed_tax_amount']
    distinct_taxes = group_df[tax_cols].drop_duplicates()
    result = group_df.iloc[0].copy()
    existing_taxes = set(result.filter(regex='^tax_type_code').dropna().values)
    
    for idx, (tax_type, amount) in enumerate(distinct_taxes.itertuples(index=False), start=1):
        if tax_type not in existing_taxes:
            result[f'tax_type_code_{idx}'] = tax_type
            result[f'billed_tax_amount_{idx}'] = amount
            existing_taxes.add(tax_type)
    
    return result

# Function to process equipment details dynamically
def add_equipment_columns(group_df):
    equipment_cols = ['equipment_type', 'equipment_number', 'equipment_name']
    distinct_equipment = group_df[equipment_cols].drop_duplicates()
    result = group_df.iloc[0].copy()
    existing_equipment = set(result.filter(regex='^equipment').dropna().values)
    
    for idx, (equipment_type, equipment_number, equipment_name) in enumerate(distinct_equipment.itertuples(index=False), start=1):
        if equipment_type not in existing_equipment:
            result[f'equipment_type_{idx}'] = equipment_type
            result[f'equipment_number_{idx}'] = equipment_number
            result[f'equipment_name_{idx}'] = equipment_name
            existing_equipment.add(equipment_type)
    
    return result

# Function to process PO number and PO consignee stop sequence dynamically
def add_po_columns(group_df):
    # Define columns to use for PO number and PO consignee stop sequence
    po_cols = ['po_number', 'po_consignee_stop_sequence']
    
    # Drop duplicates within PO columns, if any
    distinct_po = group_df[po_cols].drop_duplicates()
    
    # Initialize the result row using the first row in the group
    result = group_df.iloc[0].copy()
    
    # Track existing PO numbers and consignee stop sequences already present in the row
    existing_po = set(result.filter(regex='^po_number').dropna().values)
    existing_stop_sequence = set(result.filter(regex='^po_consignee_stop_sequence').dropna().values)
    
    # Iterate over each distinct PO number and consignee stop sequence
    for idx, (po_number, stop_sequence) in enumerate(distinct_po.itertuples(index=False), start=1):
        # Add only if the PO number and stop sequence are not already present
        if po_number not in existing_po and stop_sequence not in existing_stop_sequence:
            result[f'po_number_{idx}'] = po_number
            result[f'po_consignee_stop_sequence_{idx}'] = stop_sequence
            existing_po.add(po_number)  # Update set to prevent duplicates
            existing_stop_sequence.add(stop_sequence)  # Update set to prevent duplicates
    
    return result

# Function to process Bill of Lading number and BOL shipper stop sequence dynamically
def add_bol_columns(group_df):
    # Define columns to use for Bill of Lading number and BOL shipper stop sequence
    bol_cols = ['bill_of_lading_number', 'bol_shipper_stop_sequence']
    
    # Drop duplicates within BOL columns, if any
    distinct_bol = group_df[bol_cols].drop_duplicates()
    
    # Initialize the result row using the first row in the group
    result = group_df.iloc[0].copy()
    
    # Track existing Bill of Lading numbers and shipper stop sequences already present in the row
    existing_bol = set(result.filter(regex='^bill_of_lading_number').dropna().values)
    existing_bol_stop_sequence = set(result.filter(regex='^bol_shipper_stop_sequence').dropna().values)
    
    # Iterate over each distinct Bill of Lading number and shipper stop sequence
    for idx, (bol_number, bol_stop_sequence) in enumerate(distinct_bol.itertuples(index=False), start=1):
        # Add only if the Bill of Lading number and stop sequence are not already present
        if bol_number not in existing_bol and bol_stop_sequence not in existing_bol_stop_sequence:
            result[f'bill_of_lading_number_{idx}'] = bol_number
            result[f'bol_shipper_stop_sequence_{idx}'] = bol_stop_sequence
            existing_bol.add(bol_number)  # Update set to prevent duplicates
            existing_bol_stop_sequence.add(bol_stop_sequence)  # Update set to prevent duplicates
    
    return result

