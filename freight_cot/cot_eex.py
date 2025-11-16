import io
import requests
from bs4 import BeautifulSoup
import os
import re
from urllib.parse import urljoin
import time
from typing import List, Set, Dict
import logging
from pathlib import Path
import pandas as pd
import hashlib
from datetime import datetime,timedelta
import win32com.client as win32

#---------------------------
# Constants 
#---------------------------

EEX_CMD = {
    'CPTM': 'C5TC',
    'PTCM': 'P4TC',
    'SPTM': 'S10TC'
}

#---------------------------
# Set up logging
#---------------------------

logging.basicConfig(
    filename='freight_cot/logs/eex_scraper.log',
    filemode="w",
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


#---------------------------
# EEX Scraper
#---------------------------

class EEXScraper:
    def __init__(self):
        self.base_url = "https://public.eex-group.com/eex/mifid2/rts-21/archive/"
        self.current_url = 'https://public.eex-group.com/eex/mifid2/rts-21/index.html'
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.download_dir = "freight_cot/eex_downloads"
        self.create_download_directory()
        
    def create_download_directory(self):
        os.makedirs(self.download_dir, exist_ok=True)
        logger.info(f"Download directory: {os.path.abspath(self.download_dir)}")

    
    def get_page_content(self, url: str) -> BeautifulSoup:
        try:
            response = self.session.get(url, timeout=30,verify=False)
            response.raise_for_status()
            return BeautifulSoup(response.content, 'html.parser')
        except requests.RequestException as e:
            logger.error(f"Failed to fetch {url}: {e}")
            raise
    
    def extract_quarter_folders(self, soup: BeautifulSoup) -> List[str]:
        quarter_folders = []
        quarter_pattern = re.compile(r'^\d{4}Q[1-4]/index.html$')  # Matches 2023Q1/, 2024Q2/, etc.
        
        for link in soup.find_all('a', href=True):
            href = link['href']
            # print(href)
            if quarter_pattern.match(href):
                print(href)
                full_url = urljoin(self.base_url, href)
                quarter_folders.append(full_url)
        
        return sorted(quarter_folders)
    
    def extract_files_from_quarter(self, soup: BeautifulSoup, quarter_url: str) -> List[str]:
        """Extract files from a quarter folder that match our pattern"""
        files = []
        pattern = re.compile(r'.*(CPTM|SPTM|PTCM).*', re.IGNORECASE)
        
        for link in soup.find_all('a', href=True):
            href = link['href']
            filename = href.lower()
            
            # Check if link matches our pattern and is a file (not a directory)
            if pattern.search(filename) and not href.endswith('/'):
                full_url = urljoin(quarter_url, href)
                files.append(full_url)
        
        return files
    
    def download_file(self, url: str, quarter_folder: str) -> bool:
        """Download a single file and organize by quarter folder"""
        try:
            filename = os.path.basename(url)
            # Create quarter subfolder in download directory
            quarter_dir = os.path.join(self.download_dir, quarter_folder)
            os.makedirs(quarter_dir, exist_ok=True)
            
            filepath = os.path.join(quarter_dir, filename)
            
            # Skip if file already exists
            if os.path.exists(filepath):
                logger.info(f"File already exists: {quarter_folder}/{filename}")
                return True
            
            logger.info(f"Downloading: {quarter_folder}/{filename}")
            response = self.session.get(url, verify=False, timeout=30)
            response.raise_for_status()
            
            # Save file
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            file_size = os.path.getsize(filepath)
            logger.info(f"Downloaded: {quarter_folder}/{filename} ({file_size} bytes)")
            return True
            
        except requests.RequestException as e:
            logger.error(f"Failed to download {url}: {e}")
            return False
        except Exception as e:
            logger.error(f"Error saving file {url}: {e}")
            return False
    
    def get_all_quarter_folders(self) -> List[str]:
        """Get all quarter folders from the main archive page"""
        logger.info("Fetching quarter folders...")
        soup = self.get_page_content(self.base_url)
        quarter_folders = self.extract_quarter_folders(soup)
        logger.info(f"Found {len(quarter_folders)} quarter folders")
        return quarter_folders
    
    def get_files_from_quarter(self, quarter_url: str) -> List[str]:
        """Get all matching files from a specific quarter folder"""
        quarter_name = quarter_url.strip('/').split('/')[-1]  # Extract quarter name from URL
        logger.info(f"Scanning quarter: {quarter_name}")
        
        try:
            soup = self.get_page_content(quarter_url)
            files = self.extract_files_from_quarter(soup, quarter_url)
            logger.info(f"Found {len(files)} matching files in {quarter_name}")
            return files
        except Exception as e:
            logger.error(f"Error scanning quarter {quarter_name}: {e}")
            return []
    
    def download_all_files(self, specific_quarters: List[str] = None, max_files: int = None, download_dir: str = None):
        """Main method to download files from all quarters"""
        logger.info("Starting EEX quarterly scraper...")
        
        # Get all quarter folders
        quarter_folders = self.get_all_quarter_folders()
        if download_dir:
            quarter_folders =[download_dir]
        elif specific_quarters:
            # Filter to specific quarters if provided
            quarter_folders = [q for q in quarter_folders if any(sq in q for sq in specific_quarters)]
            logger.info(f"Filtered to {len(quarter_folders)} specific quarters")
        
        total_files_downloaded = 0
        total_files_found = 0
        
        for quarter_url in quarter_folders:
            quarter_name = quarter_url.strip('/').split('/')[-1]
            
            # Get files from this quarter
            files = self.get_files_from_quarter(quarter_url)
            total_files_found += len(files)
            
            if not files:
                continue
            
            # Download files from this quarter
            quarter_files_downloaded = 0
            for i, file_url in enumerate(files, 1):
                if max_files and total_files_downloaded >= max_files:
                    logger.info(f"Reached maximum download limit ({max_files})")
                    break
                
                if self.download_file(file_url, quarter_name):
                    quarter_files_downloaded += 1
                    total_files_downloaded += 1
                
                # Add delay between downloads to be polite
                time.sleep(0.5)
            
            logger.info(f"Quarter {quarter_name}: {quarter_files_downloaded}/{len(files)} files downloaded")
            
            if max_files and total_files_downloaded >= max_files:
                break
        
        logger.info(f"Download completed: {total_files_downloaded}/{total_files_found} files downloaded across {len(quarter_folders)} quarters")
    
    def get_download_stats(self) -> Dict:
        """Get statistics about downloaded files organized by quarter"""
        stats = {
            'total_files': 0,
            'total_size': 0,
            'quarters': {}
        }
        
        download_path = Path(self.download_dir)
        
        for quarter_dir in download_path.iterdir():
            if quarter_dir.is_dir():
                quarter_files = []
                for file_path in quarter_dir.iterdir():
                    if file_path.is_file():
                        file_info = {
                            'name': file_path.name,
                            'size': file_path.stat().st_size,
                            'modified': file_path.stat().st_mtime
                        }
                        quarter_files.append(file_info)
                        
                        stats['total_files'] += 1
                        stats['total_size'] += file_info['size']
                
                stats['quarters'][quarter_dir.name] = {
                    'file_count': len(quarter_files),
                    'total_size': sum(f['size'] for f in quarter_files),
                    'files': sorted(quarter_files, key=lambda x: x['name'])
                }
        logger.info(f"Download stats: {stats['total_files']} files, {stats['total_size'] / (1024*1024):.2f} MB")
        return stats

    def generate_report(self):
        """Generate a CSV report of downloaded files"""
        stats = self.get_download_stats()
        
        report_data = []
        for quarter, quarter_data in stats['quarters'].items():
            for file_info in quarter_data['files']:
                report_data.append({
                    'quarter': quarter,
                    'filename': file_info['name'],
                    'size_bytes': file_info['size'],
                    'size_mb': file_info['size'] / (1024 * 1024),
                    'modified': pd.to_datetime(file_info['modified'], unit='s')
                })
        
        if report_data:
            df = pd.DataFrame(report_data)
            report_path = os.path.join(self.download_dir, 'download_report.csv')
            df.to_csv(report_path, index=False)
            logger.info(f"Report generated: {report_path}")
            return df
        return None

# Alternative: Simple version for specific quarters
def download_specific_quarters(quarters: List[str], max_files: int = None):
    """Download files from specific quarters only"""
    scraper = EEXScraper()
    scraper.download_all_files(specific_quarters=quarters, max_files=max_files)
    return scraper.get_download_stats()

def download_current(max_files: int = None):
    """Download files from current quarter only"""
    scraper = EEXScraper()
    scraper.download_all_files(None, max_files, scraper.current_url)
    return scraper.get_download_stats()


def check_eex_df(df: pd.DataFrame):
    '''
    Check if the dataframe matches the template that we used for parsing
    '''
    report_date = pd.to_datetime(df.loc[1, 'EUROPEAN ENERGY EXCHANGE'])
    default_row = 7 # is used to be 7 until eex changed the format in 2025 0930
    errors = 0
    if report_date > pd.to_datetime('2025-09-25'):
        default_row = 8
    errors += df.loc[default_row, 'Unnamed: 3'] != 'Investment Firms or credit institutions'
    # print(errors,df.loc[default_row, 'Unnamed: 3'])
    errors += df.loc[default_row, 'Unnamed: 5'] != 'Investment Funds'
    # print(errors,df.loc[default_row, 'Unnamed: 5'])
    errors += df.loc[default_row, 'Unnamed: 7'] != 'Other Financial Institutions'
    errors += df.loc[default_row, 'Unnamed: 9'] != 'Commercial Undertakings'
    errors += df.loc[default_row, 'Unnamed: 11'] != 'Operators with compliance obligations under Directive 2003/87/EC'
    for i in range(3,13):
        colname = f'Unnamed: {i}'
        if i%2 == 1:
            errors += df.loc[default_row+1, colname] != 'Long'
        else:
            errors += df.loc[default_row+1, colname] != 'Short'
    errors += df.loc[default_row+2, 'Unnamed: 2'] != 'Risk reducing directly related to commercial activities'
    errors += df.loc[default_row+4, 'Unnamed: 2'] != 'Total'
    errors += df.loc[default_row+10, 'Unnamed: 2'] != 'Total'
    if errors:
        raise ValueError(f"Errors: The excel doesn't match the template {errors}")
    

def parse_eex_df(df: pd.DataFrame, _date: str, product_code: str):

    report_date = pd.to_datetime(df.loc[1, 'EUROPEAN ENERGY EXCHANGE'])
    default_row = 7 # is used to be 7 until eex changed the format in 2025 0930
    # errors = 0
    if report_date > pd.to_datetime('2025-09-25'):
        default_row = 8

    parsed_df = pd.DataFrame()
    parsed_df.loc[0,'Clear Date'] = _date #should get this from file name
    parsed_df.loc[0,'Product Code'] = product_code #should get this from file name also
    parsed_df.loc[0,'Symbol'] = EEX_CMD[product_code] #' #should get this from product code

    parsed_df.loc[0,'Financial Institutions Long'] = df.loc[default_row+4, 'Unnamed: 3']
    parsed_df.loc[0,'Financial Institutions Short'] = df.loc[default_row+4, 'Unnamed: 4']
    parsed_df.loc[0,'Managed Money Long'] = df.loc[default_row+4, 'Unnamed: 5']
    parsed_df.loc[0,'Managed Money Short'] = df.loc[default_row+4, 'Unnamed: 6']
    parsed_df.loc[0,'Others Long'] = df.loc[default_row+4, 'Unnamed: 7']
    parsed_df.loc[0,'Others Short'] = df.loc[default_row+4, 'Unnamed: 8']
    parsed_df.loc[0,'Physicals Long'] = df.loc[default_row+4, 'Unnamed: 9']
    parsed_df.loc[0,'Physicals Short'] = df.loc[default_row+4, 'Unnamed: 10']
    parsed_df.loc[0,'Operators Long'] = df.loc[default_row+4, 'Unnamed: 11']
    parsed_df.loc[0,'Operators Short'] = df.loc[default_row+4, 'Unnamed: 12']
    parsed_df.loc[0,'Financial Institutions Long Risk'] = df.loc[default_row+2, 'Unnamed: 3']
    parsed_df.loc[0,'Financial Institutions Short Risk'] = df.loc[default_row+2, 'Unnamed: 4']
    parsed_df.loc[0,'Managed Money Long Risk'] = df.loc[default_row+2, 'Unnamed: 5']
    parsed_df.loc[0,'Managed Money Short Risk'] = df.loc[default_row+2, 'Unnamed: 6']
    parsed_df.loc[0,'Others Long Risk'] = df.loc[default_row+2, 'Unnamed: 7']
    parsed_df.loc[0,'Others Short Risk'] = df.loc[default_row+2, 'Unnamed: 8']
    parsed_df.loc[0,'Physicals Long Risk'] = df.loc[default_row+2, 'Unnamed: 9']
    parsed_df.loc[0,'Physicals Short Risk'] = df.loc[default_row+2, 'Unnamed: 10']
    parsed_df.loc[0,'Operators Long Risk'] = df.loc[default_row+2, 'Unnamed: 11']
    parsed_df.loc[0,'Operators Short Risk'] = df.loc[default_row+2, 'Unnamed: 12']
    parsed_df.loc[0,'Open Interest'] =df.loc[default_row+4, 'Unnamed: 9']/ df.loc[default_row+10, 'Unnamed: 9']*100
    for col in parsed_df.columns:
        if col != 'Clear Date' and col != 'Product Code' and col != 'Symbol':
            if parsed_df.loc[0,col] == '.':
                parsed_df.loc[0,col] = 0.0
            # parsed_df[col] = pd.to_numeric(parsed_df[col],errors='coerce')
    parsed_df['Open Interest'] = parsed_df['Open Interest'].round(2)
    return parsed_df

def process_all_file(from_folder: str, to_file: str ):
    local_file_exists = os.path.exists(to_file)
    if local_file_exists:
        try:
            existing_data = pd.read_csv(to_file)
            logger.info(f"Existing data has {len(existing_data)} rows")
            last_date = existing_data['Clear Date'].max()
        except Exception as e:
            logger.error(f"Error reading existing file: {e}")
            existing_data = pd.DataFrame()
            last_date = '2001-01-01'
    else:
        existing_data = pd.DataFrame()
        last_date = '2001-01-01'
    
    parsed_dfs = []
    for filename in os.listdir(from_folder):
        data_list = filename.split('_')
        _date = data_list[1]
        #load 30 days more than last date
        if pd.to_datetime(_date) <= pd.to_datetime(last_date)-pd.Timedelta(days=30):
            continue
        prroduct_code = data_list[2]
        df = pd.read_excel(from_folder + filename)
        check_eex_df(df)
        parsed_df = parse_eex_df(df,_date,prroduct_code)
        parsed_dfs.append(parsed_df)
    new_data = pd.concat(parsed_dfs,ignore_index=True)
    logger.info(f"Processed {len(new_data)} rows of new data")

    if existing_data.empty:
        # No existing data, save all new data
        final_data = new_data
        logger.info("No existing data found, saving all new data")
    
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
                
     # Save the final data
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(to_file), exist_ok=True)
        
        final_data.to_csv(to_file, index=False)
        logger.info(f"Data successfully saved to {to_file}")
        
        # Add timestamp metadata
        timestamp_file = to_file + '.timestamp'
        with open(timestamp_file, 'w') as f:
            f.write(datetime.now().isoformat())
        
    except Exception as e:
        logger.error(f"Error saving file: {e}")


def send_email(subject, body, is_success=True, file_attachment=None):
    try:
        # Create Outlook application instance
        outlook = win32.Dispatch('Outlook.Application')
        mail = outlook.CreateItem(0)  # 0 = olMailItem
        
        # Configure email
        mail.Subject = subject
        mail.Body = body
        mail.To = "yuhang.hou@olam-agri.com"  # Replace with your email
        
        if file_attachment is not None:
            for file in file_attachment:
                file_path = os.path.abspath(file)
                mail.Attachments.Add(file_path)
                logger.info(f"Attached file: {file}")
        mail.Send() 
        print(f"Email sent successfully: {subject}")
        
    except Exception as e:
        logger.error(f"Failed to send email: {str(e)}")
        mail.subject = "EEX COT Job Failure - Email Error"
        mail.body = "The EEX COT job failed. Please check the details from the log."
        mail.To = "yuhang.hou@olam-agri.com"  # Replace with your email
        file_path = os.path.abspath('freight_cot\\logs\\eex_scraper.log')
        mail.Attachments.Add(file_path)
        mail.Send()

def job_wrapper():
    try:
        logger.info("Starting EEX COT job wrapper")
        scraper = EEXScraper()
        # Download all files from all quarters, only run when first time. After that, use download_current() to update
        # scraper.download_all_files()

        # Download current file only
        download_current(max_files=10)
        os.makedirs('freight_cot/data', exist_ok=True)
        logger.info("Processing all downloaded files into consolidated CSV")
        process_all_file('freight_cot/eex_downloads/index.html/', 'freight_cot/data/EEX_COT.csv')



        report_df = scraper.generate_report()
        body = ""
        if report_df is not None:
            body += f"\nDownload Report:\n"
            body += f"Total quarters: {report_df['quarter'].nunique()}\n"
            body += f"Total files: {len(report_df)}\n"
            body += f"Total size: {report_df['size_mb'].sum():.2f} MB\n"
            
            # Show files by quarter
            for quarter, group in report_df.groupby('quarter'):
                body += f"  {quarter}: {len(group)} files ({group['size_mb'].sum():.2f} MB)\n"
        
        # Get detailed statistics
        stats = scraper.get_download_stats()
        body += f"Detailed Statistics:\n"
        body += f"Total files downloaded: {stats['total_files']}\n"
        body += f"Total size: {stats['total_size'] / (1024*1024):.2f} MB\n"
        
        for quarter, data in stats['quarters'].items():
            body += f"  {quarter}: {data['file_count']} files ({data['total_size'] / (1024*1024):.2f} MB)\n"

        return True, body
        
    except Exception as e:
        logger.error(f"Job failed: {e}")
        return False,"The EEX COT job failed. Please check the details from the log."


if __name__ == "__main__":
    
    success, body = job_wrapper()
    logger.info(f"Job success: {success}")


    if success:
        send_email(
            subject="EEX COT Job Success",
            body=body,
            is_success=True,
            file_attachment=['freight_cot/eex_downloads/download_report.csv','freight_cot/data/EEX_COT.csv']
        )
    else:
        send_email(
            subject="EEX COT Job Failure",
            body=body,
            is_success=False,
            file_attachment=['freight_cot\\logs\\eex_scraper.log']  
        )
