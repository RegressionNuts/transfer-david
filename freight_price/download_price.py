import os
import sys
import numpy as np
import pandas as pd   
import json
import win32com.client as win32
from sqlalchemy import create_engine
import urllib.parse
from freight import ROUTE_INFO
from datetime import datetime, timedelta
from pandas.tseries.offsets import BQuarterEnd, DateOffset


SERVER_INFO ={
    'server': "SGTCX-SQLPRD01" ,#'INCHN-DRSQL01',#
    'instance': "SMARPRD2019" ,#'SMAR2019',#"
    'port': 10506,
    'username':'SMAR_REPORT',
    'password':'BeFU0aRo8?uKi$Is'
}


index_map = {
    'panamax': 'P4TC',
    'supermax': 'S10TC',
    'capesize': 'C5TC'
}

CONTRACT_FACTOR = {
    'monthly': 1,
    'quarterly': 3,
    'yearly': 12
}

NUM_TO_MONTH = {
        1: 'F',  # January
        2: 'G',  # February
        3: 'H',  # March
        4: 'J',  # April
        5: 'K',  # May
        6: 'M',  # June
        7: 'N',  # July
        8: 'Q',  # August
        9: 'U',  # September
        10: 'V',  # October
        11: 'X',  # November
        12: 'Z'   # December
    }

def generate_sql_query(data_type, symbol, start_date):
    if data_type == '':
        query_str =  f"""
            SELECT 
                "TradeRate_Rate",
                "TradeRate_PublishedDate",
                "TradeRate_PeriodType",
                "TradeRate_Source",
                "Timebucket"
            FROM 
                "dbo"."ARC_MARKET_RATES"
            WHERE 
                "Trade_Code" = '{symbol}'
                AND "TradeRate_PublishedDate" > '{start_date}'
            ORDER BY 
                "TradeRate_PublishedDate" DESC
            """

    elif data_type == 'route':
        routes = ROUTE_INFO[symbol].keys()
        routes_str = ','.join(f"'{route}'" for route in routes)
        query_str =f"""
            SELECT 
                "Trade_Code",
                "TradeRate_Rate",
                "TradeRate_PublishedDate"
            FROM 
                "dbo"."ARC_MARKET_RATES"
            WHERE 
                "Trade_Code" IN ({routes_str})
                AND "TradeRate_PublishedDate" > '{start_date}'
                AND "TradeRate_Source" = 'Spot'
            ORDER BY 
                "TradeRate_PublishedDate" DESC
            """
    else:
        raise ValueError(f"Invalid data_type: {data_type}")
    
    return query_str

def load_db_data(symbol,start_date,data_type = ''):
    server = SERVER_INFO['server']
    instance = SERVER_INFO['instance']
    port= SERVER_INFO['port']
    username= SERVER_INFO['username']
    password= SERVER_INFO['password']

    # Encode connection string
    params = urllib.parse.quote_plus(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server}\\{instance},{port};"  # Correct format
        f"DATABASE=VesselOpsProd;"
        f"UID={username};"
        f"PWD={password};"
        "TrustServerCertificate=yes;"  
        "Connection Timeout=30;"       # Increase if needed
    )
    print(params)
    connection_string = f"mssql+pyodbc:///?odbc_connect={params}"
    engine = create_engine(
        connection_string,
        pool_recycle=3600,  
        fast_executemany=True  
    )
    query = generate_sql_query(data_type, symbol, start_date)
    try:
        df = pd.read_sql(query, engine)
        print(f"Success! Retrieved {len(df)} rows.")
    except Exception as e:
        print(f"Error: {e}")
        import pyodbc
        try:
            conn = pyodbc.connect(f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server}\\{instance},{port};DATABASE=VesselOpsProd;UID={username};PWD={password}")
            print("Direct pyodbc connection successful!")
            conn.close()
        except pyodbc.Error as e:
            print(f"PyODBC error: {e}")
    finally:
        engine.dispose()
    col_names ={'Timebucket': 'month_code','TradeRate_PublishedDate':'date', 'TradeRate_Rate':'close', 'Trade_Code':'symbol'}
    col_names_to_use = {k:v for k,v in col_names.items() if k in df.columns} 
    df = df.rename(columns=col_names_to_use)
    df = df.sort_values('date')
    return df

def month_mapping(month):
    """
    transfer month of format Nov-25 to X25
    params month: str with format Nov-25
    return: str, month code for future contract
    """
    month_dict = {
        "Jan": "F", "Feb": "G", "Mar": "H", "Apr": "J",
        "May": "K", "Jun": "M", "Jul": "N", "Aug": "Q",
        "Sep": "U", "Oct": "V", "Nov": "X", "Dec": "Z"
    }
    return month_dict[month[:3]] + month[-2:]

def quarter_mapping(month):
    """
    transfer month of format Mar-25 to H2525
    params month: str with format MAr-25
    return: str, month code for future contract
    """
    month_dict = {
        "Mar": "QH",
        "Jun": "QM", 
        "Sep": "QU",
        "Dec": "QZ"
    }
    return month_dict[month[:3]] + month[-2:]

def year_mapping(month):
    return 'YZ'+ month[-2:]

def save_contracts_to_csv(df, file_path):

    def process_and_save_contracts(contract_df, mapping_func):

        contract_df['month_code'] = contract_df['month_code'].apply(mapping_func)
        contract_df = contract_df.drop_duplicates(subset=['date', 'month_code'])
        pivot_df = contract_df.pivot(index='date', columns='month_code', values='close')
        
        for col in pivot_df.columns:
            current_data = pivot_df[[col]].rename(columns={col: 'close'})
            current_data['return'] = current_data['close'] / current_data['close'].shift(1) - 1
            current_data['log_return'] = np.log(current_data['close'] / current_data['close'].shift(1))
            file_name = f"{file_path}/{col}.csv"
            if os.path.exists(file_name):
                existing_data = pd.read_csv(file_name, index_col='date', parse_dates=True)
                currents_data_to_use = current_data.dropna(subset=['close'])
                common_dates = currents_data_to_use.index.intersection(existing_data.index)
            
                if not common_dates.empty:
                    # Compare overlapping data
                    last_common_date = common_dates[-1]
                    existing_value = existing_data.loc[last_common_date, 'close']
                    current_value = current_data.loc[last_common_date, 'close']
                    
                    if not np.isclose(existing_value, current_value, rtol=1e-10):
                        print(f"Data mismatch for {col} on {last_common_date}. Please Check.")
                        continue
                
                # Append only new data
                new_data = current_data[~current_data.index.isin(existing_data.index)]
                if not new_data.empty:
                    combined_data = pd.concat([existing_data, new_data])
                    combined_data.to_csv(file_name)
                    print(f"Appended {len(new_data)} new rows to {col}.csv")
                else:
                    print(f"No new data to append for {col}")
            else:
                # Save new file if it doesn't exist
                if not os.path.exists(os.path.dirname(file_name)):
                    os.makedirs(os.path.dirname(file_name))
                    print(os.path.dirname(file_name))
                current_data.to_csv(file_name)
                print(file_name)
                print(f"Created new file for {col}")

    monthly_contracts = df[(df['TradeRate_Source'] == 'baltic') & 
                           (df['TradeRate_PeriodType'] == 'Monthly')].copy()
    quarterly_contracts = df[(df['TradeRate_Source'] == 'baltic') & 
                             (df['TradeRate_PeriodType'] == 'Quarterly')].copy()
    yearly_contracts = df[(df['TradeRate_Source'] == 'baltic') & 
                          (df['TradeRate_PeriodType'] == 'Yearly')].copy()
    
    process_and_save_contracts(monthly_contracts, month_mapping)
    process_and_save_contracts(quarterly_contracts, quarter_mapping)
    process_and_save_contracts(yearly_contracts, year_mapping)

def save_spot_to_csv(df, file_path):
    """
    save dataframe to csv
    """
    new_data= df[(df['TradeRate_Source'] == 'Spot' )]
    new_data = new_data[['date','close']].set_index('date')
    new_data['return'] = new_data['close'].pct_change()
    new_data['log_return'] = np.log(new_data['close'] / new_data['close'].shift(1))

    file_full_path = os.path.join(file_path, 'spot.csv')


    if os.path.exists(file_full_path):
        existing_data = pd.read_csv(file_full_path, index_col='date', parse_dates=True)
        
        common_dates = new_data.index.intersection(existing_data.index)
        
        if not common_dates.empty:
            close_mismatch = ~np.isclose(
                existing_data.loc[common_dates, 'close'],
                new_data.loc[common_dates, 'close'],
                rtol=1e-10  # 0.001% tolerance
            )
            
            if close_mismatch.any():
                mismatch_dates = common_dates[close_mismatch]
                raise ValueError(
                    f"Close price mismatch on dates: {mismatch_dates.values}\n"
                    f"Existing: {existing_data.loc[mismatch_dates, 'close'].values}\n"
                    f"New: {new_data.loc[mismatch_dates, 'close'].values}"
                )
        
        combined_data = pd.concat([
            existing_data,
            new_data[~new_data.index.isin(existing_data.index)]  # Only new dates
        ]).sort_index()
        
        combined_data['return'] = combined_data['close'].pct_change()
        combined_data['log_return'] = np.log(combined_data['close'] / combined_data['close'].shift(1))
        combined_data['var'] = combined_data['return'].rolling(252).quantile(0.05)
        combined_data = combined_data[['close', 'return', 'log_return', 'var']]
        combined_data.to_csv(file_full_path)
        print(f"Successfully appended {len(new_data) - len(common_dates)} new records")
    
    else:
        new_data['var'] = new_data['return'].rolling(252).quantile(0.05)
        if not os.path.exists(os.path.dirname(file_full_path)):
            os.makedirs(os.path.dirname(file_full_path))
            print(os.path.dirname(file_full_path))
        new_data.to_csv(file_full_path)
        print("Created new spot data file")

def _check_mismatch(df1, df2, dates, _column):
    mismatch = ~np.isclose(
        df1.loc[dates, _column],
        df2[_column],
        rtol=1e-8, atol=1e-8
    )
    
    if mismatch.any():
        error_dates = dates[mismatch]
        raise ValueError(
            f"Data mismatch on dates: {error_dates}\n"
            f"Existing: {df1.loc[error_dates, _column].values}\n"
            f"New calc: {df2.loc[error_dates, _column].values}"
        )

def load_business_days():
    with open('freight_price/business_days.json', 'r') as f:
        business_days = json.load(f)
    return [pd.to_datetime(i) for i in business_days]

def last_day_of_quarter(_date):
    new_date = pd.to_datetime(_date)
    next_q_end = new_date + BQuarterEnd(0)
    return next_q_end


def last_day_of_year(_date):
    _date = pd.to_datetime(_date)
    return pd.to_datetime(f'{_date.year}-12-31' )

def date_to_month(_date, contract_type = 'monthly'):
    _date = pd.to_datetime(_date)
    month_code = NUM_TO_MONTH[_date.month]
    year_code = str(_date.year)[-2:]  # Last 2 digits of year
    code =  f"{month_code}{year_code}"
    if contract_type == 'monthly':
        return code
    elif contract_type == 'quarterly':
        return 'Q'+code
    elif contract_type == 'yearly':
        return 'Y'+code
    else:
        raise ValueError(f"Unsupported contract type: {contract_type}")

def nth_nearby(_date, nth, preroll = 7, cmd = 'C5TC', contract_type = 'monthly'):
    buz_days = load_business_days()
    future_dates  =[ i  for i in buz_days if i > _date]  
    new_date = future_dates[preroll]
    # print(new_date,cmd)
    if contract_type == 'monthly':
        contract_date = new_date
    elif contract_type == 'quarterly':
        contract_date = last_day_of_quarter(new_date)
    else:
        contract_date = last_day_of_year(new_date + pd.DateOffset(years=1))


    ltd = pd.to_datetime(get_last_trading_day(date_to_month(contract_date,contract_type)))
    month = contract_date.month + nth*CONTRACT_FACTOR[contract_type] - 1
    year = contract_date.year
    if new_date>=ltd: 
        month+= CONTRACT_FACTOR[contract_type]
    year = year + month//12
    month = month % 12
    code = f'{NUM_TO_MONTH[month+1]}{str(year)[-2:]}'
    # print(date, ltd, new_date, contract_date, code)
    if contract_type == 'monthly':
        return code
    elif contract_type == 'quarterly':
        return 'Q' + code
    else:
        return 'Y' + code

def _calculate_nearby_data(df, k_nearby, roll_schedule, contract_type):
    k_nearby_spot = [nth_nearby(date, k_nearby, roll_schedule, contract_type=contract_type) 
                    for date in df.index]
    k_nearby_return = [nth_nearby(date, k_nearby, roll_schedule-1, contract_type=contract_type) 
                      for date in df.index]
    
    nearby_spot = []
    nearby_return = []
    nearby_log_return = []
    
    for i, date in enumerate(df.index):
        spot_contract = k_nearby_spot[i]
        return_contract = k_nearby_return[i]
        
        spot_val = df.loc[date, ('close', spot_contract)] if spot_contract in df['close'].columns else np.nan
        ret_val = df.loc[date, ('return', return_contract)] if return_contract in df['return'].columns else np.nan
        log_ret_val = df.loc[date, ('log_return', return_contract)] if return_contract in df['log_return'].columns else np.nan
        
        nearby_spot.append(spot_val)
        nearby_return.append(ret_val)
        nearby_log_return.append(log_ret_val)
    
    return pd.DataFrame({
        'close': nearby_spot,
        'return': nearby_return,
        'log_return': nearby_log_return
    }, index=df.index)

def save_nth_nearby_new(df, symbol, k_nearby, roll_schedule, file_path, contract_type='monthly'):

    tail = {'quarterly': 'Q', 'yearly': 'Y'}.get(contract_type, '')
    file_name = f'{file_path}/{symbol+tail}_{k_nearby}_{roll_schedule}.csv'
    
    curve = pd.DataFrame(columns=['close', 'return', 'log_return', 'var'])
    curve.index.name = 'date'
    
    if os.path.exists(file_name):
        existing_curve = pd.read_csv(file_name, index_col='date', parse_dates=True)
        
        last_date = existing_curve.index[-1]
        start_date = last_date - timedelta(days=60)  # 2 month buffer for validation
        
        new_dates = df.index[df.index > last_date]
        
        if len(new_dates) == 0:
            print("No new dates to process")
            return
        
        validation_dates = df.index[(df.index >= start_date) & (df.index <= last_date)]
        recalculated = _calculate_nearby_data(df.loc[validation_dates], k_nearby, roll_schedule, contract_type)
        
        _check_mismatch(existing_curve, recalculated, validation_dates, 'close')
        _check_mismatch(existing_curve, recalculated, validation_dates, 'return')
        _check_mismatch(existing_curve, recalculated, validation_dates, 'log_return')
        curve = existing_curve
        process_dates = new_dates
    else:
        process_dates = df.index[df.index >= '2017-01-04']
    
    new_data = _calculate_nearby_data(df.loc[process_dates], k_nearby, roll_schedule, contract_type)
    
    curve = pd.concat([curve, new_data])
    
    curve['var'] = curve['return'].rolling(252, min_periods=20).quantile(0.05)
    
    curve.to_csv(file_name)
    print(f"Saved data to {file_name}")

def load_future_data(data_path = './data/C5TC',values = 'close'):

    
    files = [f for f in os.listdir(data_path) if f.endswith('.csv')]
    dfs = (pd.read_csv(os.path.join(data_path, file)).assign(
        contract=file.replace('.csv', ''),
        date=lambda df: pd.to_datetime(df['date'])
    ) for file in files)

    big_df = pd.concat(dfs, ignore_index=True)
    pivot_df = big_df.pivot(index='date', columns='contract', values= values)

    return pivot_df

def get_last_trading_day(month_code):
    with open('freight_price/last_trading_day.json', 'r') as f:
        last_trading_days = json.load(f)
    return last_trading_days[month_code]



# only used for freight to calculate the final fixing given its not published on the exchange
def calculate_fixing(symbol,contract_folder, spot_path):
    spot_data = pd.read_csv(spot_path, index_col='date', parse_dates=True)
    start_date = pd.to_datetime('2017-01-04')
    end_time = list(spot_data.index)[-1]
    def loop_files_recursive(folder_path):
        for root, _, files in os.walk(folder_path):
            
            res = {}
            for filename in files:
                file_path = os.path.join(root, filename)
                month_code = filename.split('.')[0]
                if len(month_code)==3:
                    res[month_code] = file_path
        return res
    contract_folder = loop_files_recursive(contract_folder)
    for month_code,contract_path in contract_folder.items():
        existing_data = pd.read_csv(contract_path, index_col='date', parse_dates=True)
        ltd = get_last_trading_day(month_code)
        ltd = pd.to_datetime(ltd)
        if ltd<=end_time:           
            fixing_data = spot_data.loc[ltd.strftime('%Y-%m')]
            # print(ltd)
            exist_fixing  = existing_data.loc[ltd,'close']
            if not pd.isna(exist_fixing):
                # print(exist_fixing)
                print(f'Fixing for {month_code} already exists {exist_fixing}')
                continue
            fixing = round(np.mean(fixing_data['close']),2)
            existing_data.loc[ltd,'close'] = fixing
            existing_data['return'] = existing_data['close'] / existing_data['close'].shift(1) - 1
            existing_data['log_return'] = np.log(existing_data['close'] / existing_data['close'].shift(1))
            existing_data.to_csv(contract_path)
            print(f"Appended fixing {fixing} for {month_code} to {month_code}.csv on {ltd}")
        else:
            print(f"No new data to append for {month_code}.csv")
        

def update_data():
    size_list = [ 'capesize','panamax','supermax']
    today = pd.Timestamp.now().normalize()
    start_date = (today - pd.Timedelta(weeks=4)).strftime('%Y-%m-%d')
    # start_date = '2017-01-04' # incase of a rerun
    for which_size in size_list:
        print(f'started process {which_size}')
        df = load_db_data(index_map[which_size],start_date)
        save_contracts_to_csv(df, f'./data/{index_map[which_size]}')
        save_spot_to_csv(df, f'./data/series/{index_map[which_size]}')
        contract_df = load_future_data(f'./data/{index_map[which_size]}',['close', 'return', 'log_return'])
        monthly = [(i,j) for i in [0,1,2,3] for j in list(range(1,16))]
        quarterly = [(i,j) for i in [0,1] for j in list(range(1,16))]
        yearly = [(i,j) for i in [0] for j in list(range(1,16))]
        for k_nearby, roll_schedule in monthly:
            save_nth_nearby_new(contract_df,index_map[which_size], k_nearby, roll_schedule, f'./data/series/{index_map[which_size]}')
        for k_nearby,roll_schedule in quarterly:
            save_nth_nearby_new(contract_df,index_map[which_size], k_nearby, roll_schedule, f'./data/series/{index_map[which_size]}', contract_type='quarterly')
        for k_nearby,roll_schedule in yearly:
            save_nth_nearby_new(contract_df, index_map[which_size], k_nearby, roll_schedule, f'./data/series/{index_map[which_size]}',contract_type='yearly')
        calculate_fixing(index_map[which_size],f'./data/{index_map[which_size]}', f'./data/series/{index_map[which_size]}/spot.csv')
        print(f'finished process {which_size}')

if __name__ == '__main__':
    update_data()