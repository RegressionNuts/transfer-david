import requests
import zipfile
import urllib.parse
import os
import pandas as pd
from datetime import datetime,timedelta
import tempfile
import hashlib
from typing import Callable, Optional, Any
import logging
from typing import List
import win32com.client as win32

SGX_BASE_URL = (
        "https://api2.sgx.com/sites/default/files/reports/final-settlement-prices/"
        "{year}/{month}/wcm%40sgx_en%40Aggregate%20Exposure%20Report%40"
        "{date}%40futcurrent.zip"
        )
SGX_TOTAL_URL = (
        "https://api2.sgx.com/sites/default/files/reports/final-settlement-prices/"
        "{year}/{month}/wcm%40sgx_en%40Aggregate%20Exposure%20Report%40"
        "{date}%40fnocurrent.zip"
)

SGX_CMD = {
    'C5TC': 'CWF',
    'P4TC': 'PVF'
}

SGX_SYMBOL_MAP = {
    'CWF': 'C5TC',
    'PVF': 'P4TC'
}

EEX_BASE_URL = (
'https://public.eex-group.com/eex/mifid2/'
)



#---------------------------
# Set up logging
#---------------------------
logging.basicConfig(
    filename='freight_cot/logs/sgx_scraper.log',
    filemode="w",
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)



#---------------------------
# main functions
#---------------------------

def get_nearest_previous(date: datetime = None, offset: int = 4) -> datetime:
    '''
    Get the nearest previous Friday
    '''
    if date is None:
        date = datetime.now()
    
    # Monday = 0, Sunday = 6, so Friday = 4
    days_since_friday = (date.weekday() - 4) % 7
    if days_since_friday == 0:
        # Today is Friday
        return date.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        # Go back to previous Friday
        previous_friday = date - timedelta(days=days_since_friday)
        return previous_friday.replace(hour=0, minute=0, second=0, microsecond=0)

def get_days_between_dates(start_date: datetime, end_date: datetime, offset: int = 4) -> List[datetime]:
    
    if start_date > end_date:
        raise ValueError("start_date cannot be after end_date")
    
    # Normalize dates to midnight
    start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0)
    
    days_wanted = []
    
    # Find the first Friday on or after start_date
    current_date = start_date
    while current_date.weekday() != offset:  # 4 = Friday
        current_date += timedelta(days=1)
    
    # Collect all Fridays until end_date
    while current_date <= end_date:
        days_wanted.append(current_date)
        current_date += timedelta(days=7)
    
    return days_wanted

def download_and_process_file(
    url: str,
    process_function: Callable[[str], pd.DataFrame],
    local_file_path: str,
    identifier_column: str = 'id',
    temp_dir: Optional[str] = None,
    force_download: bool = False
) -> bool:
    """
    Download, unzip, process a file and handle incremental updates.
    
    Args:
        url: URL of the zip file to download
        process_function: Function that takes a file path and returns a DataFrame
        local_file_path: Path to the local file to update
        identifier_column: Column name to use for identifying duplicates
        temp_dir: Temporary directory for downloading/extracting (optional)
        force_download: If True, download and process even if file exists
    """
    
    # Create temp directory if not provided
    if temp_dir is None:
        temp_dir = tempfile.mkdtemp()
    
    # Check if local file exists
    local_file_exists = os.path.exists(local_file_path)
    
    if local_file_exists and not force_download:
        logger.info(f"Local file exists at {local_file_path}")
        
        # Read existing data
        try:
            existing_data = pd.read_csv(local_file_path)
            logger.info(f"Existing data has {len(existing_data)} rows")
        except Exception as e:
            logger.error(f"Error reading existing file: {e}")
            existing_data = pd.DataFrame()
    
    else:
        existing_data = pd.DataFrame()
        logger.info("No existing file found or force download requested")
    
    # Download the file
    zip_path = os.path.join(temp_dir, 'downloaded_file.zip')
    logger.info(f"Downloading file from {url} to {zip_path}")
    
    try:
        response = requests.get(url, verify=False)
        response.raise_for_status()
        
        with open(zip_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        logger.info("Download completed successfully")
        
    except requests.RequestException as e:
        logger.error(f"Download failed: {e}")
        return False
    
    # Unzip the file
    extracted_files = []
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            extracted_files = zip_ref.namelist()
            zip_ref.extractall(temp_dir)
            logger.info(f"Extracted {len(extracted_files)} files: {extracted_files}")
    
    except zipfile.BadZipFile as e:
        logger.error(f"Invalid zip file: {e}")
        return False
    
    # Process each extracted file
    new_data_list = []
    for file_name in extracted_files:
        if file_name.startswith('CMD_EXT_Futures_Only_OI') or file_name.startswith('CMD_EXT_Futures_Options_OI'):
            file_path = os.path.join(temp_dir, file_name)
            
            if os.path.isfile(file_path):
                try:
                    logger.info(f"Processing file: {file_name}")
                    processed_data = process_function(file_path)
                    new_data_list.append(processed_data)
                    
                except Exception as e:
                    logger.error(f"Error processing {file_name}: {e}")
    
    if not new_data_list:
        logger.warning("No data was processed from the extracted files")
        return False
    
    # Combine all processed data
    new_data = pd.concat(new_data_list, ignore_index=True)
    logger.info(f"Processed {len(new_data)} rows of new data")
    
    if existing_data.empty:
        # No existing data, save all new data
        final_data = new_data
        logger.info("No existing data found, saving all new data")
    
    else:
        # Find new rows that don't exist in current data
        if identifier_column in new_data.columns and identifier_column in existing_data.columns:
            # Use identifier column to find new data
            existing_ids = set(existing_data[identifier_column].astype(str))
            new_data['is_new'] = ~new_data[identifier_column].astype(str).isin(existing_ids)
            
            new_rows = new_data[new_data['is_new']].copy()
            new_rows.drop('is_new', axis=1, inplace=True)
            
            logger.info(f"Found {len(new_rows)} new rows based on {identifier_column}")
            
            if len(new_rows) > 0:
                final_data = pd.concat([existing_data, new_rows], ignore_index=True)
                logger.info(f"Combined data has {len(final_data)} rows")
            else:
                logger.info("No new data found. File is up to date.")
                return False
        
        else:
            # If no identifier column, compare entire datasets
            # Create hash for each row to compare
            def create_row_hash(row):
                return hashlib.md5(str(tuple(row)).encode()).hexdigest()
            
            existing_data['hash'] = existing_data.apply(create_row_hash, axis=1)
            new_data['hash'] = new_data.apply(create_row_hash, axis=1)
            
            existing_hashes = set(existing_data['hash'])
            new_data['is_new'] = ~new_data['hash'].isin(existing_hashes)
            
            new_rows = new_data[new_data['is_new']].copy()
            new_rows.drop(['hash', 'is_new'], axis=1, inplace=True)
            existing_data.drop('hash', axis=1, inplace=True)
            
            logger.info(f"Found {len(new_rows)} new rows based on content hashing")
            
            if len(new_rows) > 0:
                final_data = pd.concat([existing_data, new_rows], ignore_index=True)
                logger.info(f"Combined data has {len(final_data)} rows")
            else:
                logger.info("No new data found. File is up to date.")
                return False
    
    # Save the final data
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
        
        final_data.to_csv(local_file_path, index=False)
        logger.info(f"Data successfully saved to {local_file_path}")
        
        # Add timestamp metadata
        timestamp_file = local_file_path + '.timestamp'
        with open(timestamp_file, 'w') as f:
            f.write(datetime.now().isoformat())
        
    except Exception as e:
        logger.error(f"Error saving file: {e}")
        return False
    
    # Clean up temporary files
    try:
        os.remove(zip_path)
        for file_name in extracted_files:
            file_path = os.path.join(temp_dir, file_name)
            if os.path.exists(file_path):
                os.remove(file_path)
        logger.info("Temporary files cleaned up")
    except Exception as e:
        logger.warning(f"Could not clean up temporary files: {e}")
    return True

def process_function_SGX(file_path: str) -> pd.DataFrame:
    """
    Example process function - reads a CSV file and performs some processing.
    Replace this with your actual processing logic.
    """
   
    if file_path.endswith('.csv'):
        df = pd.read_csv(file_path)
    elif file_path.endswith(('.xlsx', '.xls')):
        df = pd.read_excel(file_path)
    else:
        raise ValueError(f"Unsupported file format: {file_path}")
    df = df[df['Product Code'].isin(['CWF','PVF'])]
    # df.rename(columns={'Clear Date':'date'}, inplace=True)
    # df['date'] = pd.to_datetime(df['date'])
    # df.set_index('date', inplace=True)
    
    df['Symbol'] = df['Product Code'].map(SGX_SYMBOL_MAP)
    return df



def generate_sgx_url(_date, future_only = True):
    '''
    Generate SGX URL
    '''
    year = _date.strftime("%Y")
    month = _date.strftime("%m")
    formatted_date = _date.strftime("%d-%b-%Y")
    encoded_date = urllib.parse.quote(formatted_date)
    if future_only:
        return SGX_BASE_URL.format(year=year, month=month, date=encoded_date)
    else:
        return SGX_TOTAL_URL.format(year=year, month=month, date=encoded_date)
    
#---------------------------
# Send Emails
#---------------------------

def send_email(subject, body, is_success=True, file_attachment=None):
    '''
    Function to send email
    '''
    try:
        # Create Outlook application instance
        outlook = win32.Dispatch('Outlook.Application')
        mail = outlook.CreateItem(0)  # 0 = olMailItem
        
        # Configure email
        mail.Subject = subject
        mail.Body = body
        mail.To = "yuhang.hou@olam-agri.com" # Replace with your email
        
        if file_attachment is not None:
            for file in file_attachment:
                file_path = os.path.abspath(file)
                mail.Attachments.Add(file_path)
                logger.info(f"Attached file: {file}")
        mail.Send() 
        print(f"Email sent successfully: {subject}")
        
    except Exception as e:
        logger.error(f"Failed to send email: {str(e)}")
        mail.subject = "Failed Title"
        mail.body = "The job failed. Please check the details from the log."
        mail.To = "yuhang.hou@olam-agri.com"  # Replace with your email
        file_path = os.path.abspath('path of your log')
        mail.Attachments.Add(file_path)
        mail.Send()



def job_wrapper():
    '''
    Main execution example
    '''
    success = False
    body = ""
    try:
        date = datetime.now()
        last_friday = get_nearest_previous(date,4)
        last_wednesday = get_nearest_previous(date,2)
        if last_friday > last_wednesday:
            logger.info("File is up to date, no download needed")
        else:
            logger.info("File is not up to date, downloading data")
            # last_friday -= timedelta(days=7)
            file_url_future = generate_sgx_url(last_friday)
            file_url_total = generate_sgx_url(last_friday, future_only = False)
            download_and_process_file(
                url=file_url_future,
                process_function= process_function_SGX,
                local_file_path='freight_cot/data/SGX_COT.csv',
                identifier_column='id',  # Replace with your identifier column
                force_download=False
            )
            download_and_process_file(
                url=file_url_total,
                process_function= process_function_SGX,
                local_file_path='freight_cot/data/SGX_COT_All.csv',
                identifier_column='id',  # Replace with your identifier column
                force_download=False
            )
            success = True
            body = "The SGX COT job completed successfully."
        
    except Exception as e:
        success = False
        body = f"The SGX COT job failed:\n {e}"
    
    return success, body

# Main execution example
if __name__ == "__main__":

    success, body = job_wrapper()
    logger.info(f"Job success: {success}")


    if success:
        send_email(
            subject="SGX COT Job Success",
            body=body,
            is_success=True,
            file_attachment=['freight_cot/data/SGX_COT.csv','freight_cot/data/SGX_COT_All.csv']
        )
    else:
        send_email(
            subject="SGX COT Job Failure",
            body=body,
            is_success=False,
            file_attachment=['freight_cot/logs/SGX_COT.log']  
        )
