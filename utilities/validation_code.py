import xml.etree.ElementTree as ET
import pandas as pd


def get_validation_dict(file_path, filter="web"):
    """
    Parse an EDIFLATReader XML file to return a validation dictionary.
    file_path : str
        The path to the EDIFLATReader XML file to be parsed.  
    filter : str, optional
        The mask filter to apply when searching for records. Default is "web".
        Only records with the specified mask will be included in the validation dictionary.
    """    
    with open(file_path, 'r') as file:
        xml_data = file.read()

    root = ET.fromstring(xml_data)

    validation_dict = {}
    for format_elem in root.findall('format'):
        masks = format_elem.findall(".//mask")
        for mask in masks:
                if mask.text ==filter:
                    for record in format_elem.find('records').findall('record'):
                                    for field in record:                
                                        field_name = field.tag
                                        validation_dict[field_name] = field.attrib

    return validation_dict


def validate_dataframe(df, validation_dictionary):
    df_col = df.dropna(axis=1, how='all')
    cols = df_col.columns

    del df_col

    # Initialize validation messages column
    df['validation_message'] = [{} for _ in range(len(df))]  # Initialize empty dicts for messages

    # Loop through validation rules
    for col, rules in validation_dictionary.items():
        if col in cols:
            length = int(rules.get('length', 0))
            required = rules.get('required', 'N')
            trim = rules.get('trim', 'false').lower() == 'true'

            # If trim is true, trim the column values to the specified length
            if trim:
                df[col] = df[col].apply(lambda x: str(x)[:length] if pd.notna(x) else x)

            # Check for 'required' rule (vectorized)
            if required == 'Y':
                is_na = df[col].isna()
                df.loc[is_na, 'validation_message'] = df.loc[is_na, 'validation_message'].apply(
                    lambda msg: {**msg, 'R': msg.get('R', []) + [col]}
                )

            # Check for 'length' rule (only if trim is false)
            if length > 0 and not trim:
                long_entries = df[col].apply(lambda x: len(str(x)) > length if pd.notna(x) else False)
                df.loc[long_entries, 'validation_message'] = df.loc[long_entries, 'validation_message'].apply(
                    lambda msg: {**msg, 'L': msg.get('L', []) + [col]}
                )

    # Add validation status
    df['validation_status'] = df['validation_message'].apply(lambda msg: True if msg == {} else False)

    return df


if __name__=="__main__":
    # Example usage:
    data = {
        'customer_id': ['123423456789', None, '123', None],
        'carrier_scac': ['ABCDEFGHIJKLMN0', 'ABCDEFGHIJKLMNO4356789', None, 'ABCDEFGHIJKLMN'],
        'pro_no': ['PRO12345678901234567', None, None, 'PRO12345678901234567890']
    }

    df = pd.DataFrame(data)

    # Sample validation dictionary
    validation_dictionary_ = {
        'customer_id': {'length': '4', 'required': 'Y', 'trim': 'false'},
        'carrier_scac': {'length': '15', 'required': 'Y', 'trim': 'true'},
        'pro_no': {'length': '20', 'required': 'Y', 'trim': 'true'},
    }

    # Validate the DataFrame
    validated_df = validate_dataframe(df, validation_dictionary_)

    # Display the validated DataFrame
    print(validated_df)

