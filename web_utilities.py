from datetime import datetime
import re
import pandas as pd

def clean_string(s, removeSpace=False, exclude_chars=""):
    # Create a regex pattern to include characters that should be excluded
    exclude_pattern = re.escape(exclude_chars)  # Escape to handle special characters
    if not removeSpace:
        s = re.sub(fr'[^0-9a-zA-Z{exclude_pattern}]', ' ', s)
    else:
        s = re.sub(fr'[^0-9a-zA-Z{exclude_pattern}]', '', s)

    # Trim leading and trailing spaces
    s = s.strip()
    # Replace multiple spaces with a single space
    s = re.sub(r'\s+', ' ', s)
    
    return s

def clean_column_header_name(col):
    return re.sub(r'\s+', ' ', col.replace('\n', '').strip().lower())

# Generalized function to format decimal places
def format_decimal_two_places(value, decimal_places=2):
    return f"{value:.{decimal_places}f}"

def remove_decimal_points(value):
    return str(value).replace('.', '') 

def clean_decimal(s):
    return re.sub(r'[^0-9\s-]', '', s)

def format_date(date_str):
    """
    Convert a date string to the YYYYMMDD format.

    :param date_str: The input date string.
    :return: The formatted date string in YYYYMMDD format.
    """
    if not date_str:  # Check if the date string is blank
        return ''  # Return an empty string if the date string is blank

    date_formats = ['%m/%d/%Y', '%d%b%y']  # List of possible date formats

    for date_format in date_formats:
        try:
            # Try to convert the input date string to a datetime object
            dt = datetime.strptime(date_str, date_format)
            break  # Exit the loop if parsing is successful
        except ValueError:
            continue  # Try the next date format if parsing fails
    else:
        # Handle the case where the date string cannot be parsed by any format
        return ''  # Return an empty string if the date string cannot be parsed

    # Extract day, month, and year
    day = dt.day
    month = dt.month
    year = dt.year

    # Format day and month with leading zeros if necessary
    sDay = f"{day:02}"
    sMon = f"{month:02}"

    # Combine year, month, and day into the final string
    sDate = f"{year}{sMon}{sDay}"

    return sDate

def convert_to_yyyymmdd(date_str, input_format):
    try:
        # Check if the date string is blank or None
        if not date_str:
            return ""
        # Parse the input date string with the given format
        date_obj = datetime.strptime(date_str, input_format)
        # Convert the date to the desired yyyymmdd format
        return date_obj.strftime('%Y%m%d')
    except ValueError:
        return ""

def to_float_100(value):
    try:
        # If value is not None or empty, convert it to float, multiply by 100, and round to remove floating-point errors
        return str(round(float(value) * 100)) if value else '0'
    except (ValueError, TypeError):
        # In case of an invalid value, return '0'
        return '0'
    
 # Apply the function to the specified date columns
date_columns = ['ship_date', 'delivery_date', 'pro_date', 'interline_date', 'guaranteed_date', 'invoice_date', 'master_bill_date']
# Define a dictionary mapping customer_id to their respective date format
date_format_mapping = {
    '3960': '%d%b%y',
    '5918': '%m/%d/%Y',
    # Add more customer_id and formats here as needed
}

# function to handle both customer_id and carrier-specific date formats
def apply_date_format(row, column):
    customer_id = row['customer_id']
    carrier_scac = row['carrier_scac']
    date_value = row[column]

    # Special case: customer '3960' and carrier 'KL12'
    if customer_id == '3960' and carrier_scac == 'KL12':
        return convert_to_yyyymmdd(date_value, '%m/%d/%Y')
    
    # Check if the customer_id exists in the mapping and apply the correct format
    elif customer_id in date_format_mapping:
        return convert_to_yyyymmdd(date_value, date_format_mapping[customer_id])
    
    # Return the original value if no format is matched
    return date_value

def get_col_no(colname):
    num = 0
    for c in colname:
        # Convert the letter to its corresponding number (A=1, B=2, ..., Z=26)
        num = num * 26 + (ord(c.upper()) - ord('A')) + 1
    return num

def rearrange_col(start_col,end_col,names, df):

    start_col_n = get_col_no(start_col)-1
    end_col_n = get_col_no(end_col)

    print(start_col_n,end_col_n)
    df_1 = df.iloc[:,start_col_n:end_col_n]
    df_2 = df.iloc[:,:start_col_n]
    df_3 = df.iloc[:,end_col_n:]
    num_cols = len(df_1.columns)  # Number of columns in the DataFrame
    iter = num_cols//len(names)
    col_ = []
    for i in range(0,iter,1):
        if i >0:
            col_.extend([x+f".{i}" for x in names])
        else:
            col_.extend(names)
    df_1.columns = col_
    df = pd.concat([df_2,df_1,df_3],axis = 1)

    return df