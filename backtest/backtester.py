import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Callable
import warnings
import re

PATH = 'C:/Users/yuhang.hou/projects/transfer-david'
MONTH_CODES = {
        'F': 1, 'G': 2, 'H': 3, 'J': 4, 'K': 5, 'M': 6,
        'N': 7, 'Q': 8, 'U': 9, 'V': 10, 'X': 11, 'Z': 12
    }
MONTH_TO_CODE = {v: k for k, v in MONTH_CODES.items()}

def round_to_nearest_5(number):
    return 5 * round(number / 5)

def round_to_nearest_15(number):
    return 15 * round(number / 15)

def parse_roll_schedule_regex(schedule_str, _type = 'monthly') -> list:
    """Parse a roll schedule string into a list of month codes."""
    if _type == 'monthly':
        pattern = r'[A-Z]\*?'
        return re.findall(pattern, schedule_str)
    else:
        pattern = r'[A-Z][A-Z]\*?'
        return re.findall(pattern, schedule_str)


import numpy as np

class KalmanTrendEstimator:

    def __init__(self, process_noise=0.1, observation_noise=1.0, 
                 initial_level=0, initial_trend=0, initial_uncertainty=1.0):
        self.q = np.array([[process_noise, 0], 
                          [0, process_noise]])
        self.r = observation_noise
        self.F = np.array([[1, 1], 
                          [0, 1]])
        self.H = np.array([[1, 0]])
        self.state = np.array([[initial_level], 
                             [initial_trend]])
        self.P = np.eye(2) * initial_uncertainty
        self.history = {
            'levels': [],
            'trends': [],
            'timesteps': []
        }
    
    def update(self, measurement, timestep=None):

        self.state = self.F @ self.state
        self.P = self.F @ self.P @ self.F.T + self.q
        y = measurement - self.H @ self.state
        S = self.H @ self.P @ self.H.T + self.r
        K = self.P @ self.H.T / S
        self.state = self.state + K * y
        self.P = (np.eye(2) - K @ self.H) @ self.P
        self.history['levels'].append(float(self.state[0]))
        self.history['trends'].append(float(self.state[1]))
        self.history['timesteps'].append(timestep if timestep is not None else len(self.history['timesteps']))
        return self.get_estimate()
    
    def get_estimate(self):
        return {
            'level': float(self.state[0]),
            'trend': float(self.state[1]),
            'uncertainty': self.P.tolist()
        }
    
    def predict(self, steps_ahead=1):
        predicted_state = np.linalg.matrix_power(self.F, steps_ahead) @ self.state
        return float(predicted_state[0])

class VOlROllingBacktest:    
    
    def __init__(self, 
                 data = pd.DataFrame(),
                 config = {},
                 trading_days = [],
                 last_trading_day = {},
                vol_series = pd.DataFrame(),    
):
 
        self.data = data.copy()
        self.cash = 0
        self.commission = 70
        self.slippage = 0.007
        self.max_daily_volume = config.get('max_daily_volume', 50)
        self.start_date = pd.to_datetime( config['start_date'])
        self.end_date = pd.to_datetime(config['end_date'])
        self.roll_start = config['roll_start']
        self.roll_schedule = config['roll_schedule']
        self.roll_days = config['roll_dates']
        self.roll_out = config['roll_out']
        self.max_position = config['max_position']
        self.trading_days = trading_days
        self.roll_style = config['roll_style']
        self.last_trading_day = last_trading_day
        self.cost_type = config['cost_type']
        self.longshort = config['longshort']
        self.portfolio = {} 
        self.portfolio[self.start_date] = {}
        self.portfolio[self.start_date]['cash'] = 0
        self.vol_series_path = config['vol_series']

        self.vol_series = self.calculate_vol_series(vol_series)
        self.vol_target = config['vol_target']
        self.round = config['round']
        self.symbol = config.get('symbol','C5TC')


         
        
        self.roll_dates = self._calculate_roll_dates()
        self.roll_start_dates = self._calculate_roll_start_dates()
    
    def get_current_month_contract(self, date):
        schedule = parse_roll_schedule_regex(self.roll_schedule,self.roll_style)            
        current_month = date.month
        current_year = date.year
        
        month_code = schedule[current_month - 1]
        if month_code[-1] == '*':
            current_year+=1
            month_code = month_code[:-1]
        return f'{month_code}{str(current_year)[-2:]}'
        
    def _calculate_roll_dates(self) -> Dict[str, List[datetime]]:

        trading_dates = self.trading_days
        roll_dates = []
        _date = self.start_date.replace(month=1, day=1)
        while _date <= self.end_date:
            dates = [i for i in trading_dates if i.month == _date.month and i.year == _date.year]
            dates = sorted(dates)
            roll_dates += dates[self.roll_start:self.roll_start + self.roll_days]
            _date += pd.DateOffset(months=1)
        roll_dates = list(set(roll_dates))
        roll_dates = sorted(roll_dates)
        return roll_dates
    
    def _calculate_roll_start_dates(self) -> Dict[str, List[datetime]]:

        trading_dates = self.trading_days
        roll_start_dates = []
        _date = self.start_date.replace(month=1, day=1)
        while _date <= self.end_date:
            dates = [i for i in trading_dates if i.month == _date.month and i.year == _date.year]
            roll_start_dates.append( dates[self.roll_start])
            _date += pd.DateOffset(months=1)
        roll_start_dates = list(set(roll_start_dates))
        roll_start_dates = sorted(roll_start_dates)
        return roll_start_dates
    
    def get_prev_date(self,date):
        dates = [i for i in self.trading_days if i<=date]
        dates = sorted(dates)
        return dates[-1]


    def calculate_trading_costs(self, date, contract, quantity, price ):

        if date == self.last_trading_day[contract]:
            return 25*abs(quantity)
        elif self.cost_type == 'fixed':
            return self.commission*abs(quantity)
        else:
            return self.slippage*abs(quantity)*price        
    

    def roll_position(self, date):
        
        if date in self.roll_start_dates:
            target_contract = self.get_current_month_contract(date)
            prev_date =date - pd.DateOffset(months=1)
            current_contracts = self.get_current_month_contract(prev_date)
            if current_contracts == target_contract:
                return True
            position = self.portfolio[date]['positions']
            
            roll_dates = [_date for _date in self.roll_dates if _date.month == date.month and _date.year == date.year]
            if current_contracts not in position:
                size = self.calculate_init_size(date, target_contract)
                if size/len(roll_dates) > self.max_daily_volume:
                    raise Exception(f"Daily volume of {size/len(roll_dates)} exceeds max daily volume of {self.max_daily_volume}")
                for roll_date in roll_dates:
                    self.portfolio[date]['trades'].append(
                        {
                            'contract': target_contract,
                            'quantity': size/len(roll_dates),
                            'date': roll_date
                        }
                    )
            else:
                size = position[current_contracts]
                if size/len(roll_dates) > self.max_daily_volume:
                    raise Exception(f"Daily volume of {size/len(roll_dates)} exceeds max daily volume of {self.max_daily_volume}")
                for roll_date in roll_dates:
                    self.portfolio[date]['trades'].append(
                        {
                            'contract': current_contracts,
                            'quantity': -size/len(roll_dates)*self.roll_out,
                            'date': roll_date
                        }
                    )
                    self.portfolio[date]['trades'].append(
                        {
                            'contract': target_contract,
                            'quantity': size/len(roll_dates),
                            'date': roll_date
                        }
                    )

        return True

    def calculate_vol_series(self,vol_series):
        vol_series = pd.read_csv(self.vol_series_path)
        vol_series['date'] = pd.to_datetime(vol_series['date'])
        vol_series['vol'] = vol_series['return'].rolling(20).apply(lambda x: np.sqrt(np.sum(x**2)/20))*np.sqrt(252)
        vol_series = vol_series[['date','vol']].set_index('date')
        vol_series.dropna(inplace=True)
        return vol_series
    
    def calculate_size_on_date(self,date, price):
        prev_date = self.get_prev_date(date)
        vol = self.vol_series.loc[prev_date,'vol']
        return min(max(-self.max_position, self.vol_target/(vol*price)),self.max_position)
    
    def calculate_init_size(self, date, contract):
        prev_date = self.get_prev_date(date)
        return self.calculate_size_on_date(date, self.get_contract_price(prev_date, contract))

    def ensure_portfolio_data(self,date,positions):
        if date not in self.portfolio:
            self.portfolio[date] = {}

        if 'positions' not in self.portfolio[date]:
            self.portfolio[date]['positions'] = positions
        if 'trades' not in self.portfolio[date]:
            self.portfolio[date]['trades'] = []
                
    def get_contract_price(self, date, contract):
        contract_data = self.data.loc[date,('close',contract)]
        return contract_data
    
    def calculate_portfolio_value(self, date) -> float:
        cash_value = self.portfolio[date]['cash']
        
        position_value = 0
        for contract, quantity in self.portfolio[date]['positions'].items():
            price = self.get_contract_price(date, contract)
            if price is not None:
                position_value += quantity * price
        self.portfolio[date]['level'] = cash_value + position_value
    

    def execute_trades(self, date):
        trades = self.portfolio[date]['trades']
        cash = self.cash
        tc = 0
        unexecuted_trades = []
        future_value = 0
        for trade in trades:
            if trade['date'] == date:
                execution_price = self.get_contract_price(date, trade['contract'])
                tc += self.calculate_trading_costs( date, trade['contract'], trade['quantity'], execution_price )
                future_value -= trade['quantity'] * execution_price
                if  trade['contract'] in self.portfolio[date]['positions']:
                    self.portfolio[date]['positions'][ trade['contract']] += trade['quantity']
                else:
                    self.portfolio[date]['positions'][ trade['contract']] = trade['quantity']
            else:
                unexecuted_trades.append(trade)
        
        positions = self.portfolio[date]['positions']
        new_pos = {}
        for contract, quantity in positions.items():
            price = self.get_contract_price(date, contract)
            if date == self.last_trading_day[contract]:
                tc += self.calculate_trading_costs( date, contract, quantity, price )
                future_value += quantity * price
            else:
                new_pos[contract] = quantity
            
        self.cash = cash - tc + future_value
        self.portfolio[date]['cash'] = self.cash
        self.portfolio[date]['tc'] = tc
        self.portfolio[date]['positions'] = new_pos
                
        return unexecuted_trades
    
    def vol_adjustment(self,date):
        positions = self.portfolio[date]['positions']
        if len(positions) > 1:
            contract = self.get_current_month_contract(date)
        elif len(positions) == 1:
            contract = list(positions.keys())[0]

        else:
            return
        prev_date = self.get_prev_date(date)
        price = self.get_contract_price(prev_date, contract)
        size = self.calculate_size_on_date(prev_date, price)*self.longshort
        trades = self.portfolio[date]['trades']
        trade_size = min(max(size-positions[contract],-self.max_daily_volume),self.max_daily_volume)
        if self.roll_style == 'monthly':
            if self.round:
                trade_size = round_to_nearest_5(trade_size)
            trades.append(
            {
                'contract': contract,
                'quantity': trade_size,
                'date': date
            }
            )
        elif self.roll_style == 'quarterly':
            if self.round:
                trade_size = round_to_nearest_15(trade_size)
            trades.append(
            {
                'contract': contract,
                'quantity': trade_size,
                'date': date
            }
            )
        
        self.portfolio[date]['trades'] = trades.copy()


    def run_backtest(self, 
                    start_date: datetime = None,
                    end_date: datetime = None) -> pd.DataFrame:

        if start_date is None:
            start_date = self.start_date
        if end_date is None:
            end_date = self.end_date

        dates = [date for date in self.trading_days if date>=start_date and date<=end_date]
        unexecuted_trades = []
        positions = {}
        for date in sorted(dates):
            # print(f'Processing {date}')
            self.ensure_portfolio_data(date,positions)
            if len(unexecuted_trades):
                self.portfolio[date]['trades'] = unexecuted_trades.copy()
            #roll
            if date in self.roll_start_dates:
                self.roll_position(date)
            elif date not in self.roll_dates:
                self.vol_adjustment(date)

            unexecuted_trades = self.execute_trades(date)
           
            self.calculate_portfolio_value(date)
            positions = self.portfolio[date]['positions'].copy()

        return self.portfolio


class TrendBacktester(VOlROllingBacktest):    
    
    
    def __init__(self, 
                 data = pd.DataFrame(),
                 config = {},
                 trading_days = [],
                 last_trading_day = {},
                vol_series = pd.DataFrame()):
        self.lookback = config['lookback']
        super().__init__(data, config, trading_days, last_trading_day,vol_series)



    def calculate_vol_series(self,vol_series):
        vol_series = pd.read_csv(self.vol_series_path)
        vol_series['date'] = pd.to_datetime(vol_series['date'])
        vol_series['vol'] = vol_series['return'].rolling(20).apply(lambda x: np.sqrt(np.sum(x**2)/20))*np.sqrt(252)
        vol_series['noise'] = vol_series['return'].rolling(60).apply(lambda x: np.sqrt(np.sum(x**2)/60))*np.sqrt(252)
        vol_series['cumreturn'] = (1 + vol_series['return']).cumprod()
        def calculate_trend(df,lookback):
            data = list(df['cumreturn'])
            obs_noise = list(df['noise'])
            
            noise = obs_noise[-1]
            kf = KalmanTrendEstimator(
                process_noise=0.05*noise,
                observation_noise=noise,
                initial_level= data[0],
                initial_trend=0
            )
            estimates = []
            for measurement in data:
                estimates.append(kf.update(measurement))
            diff = [estimates[i]['level'] - data[i] for i in range(len(estimates))]
            noise_std = np.std(diff)/estimates[-1]['level']

            return  (estimates[-1]['level'] - estimates[-lookback]['level'])/estimates[-lookback]['level'],noise_std
        trends = []
        noises = []
        for i in range(len(vol_series)):
            if i < 59:
                trends.append(0)
                noises.append(0)
            else:
                trend,noise = calculate_trend(vol_series.iloc[i-59:i],self.lookback)
                trends.append(trend)
                noises.append(noise)
        
        vol_series['trend'] = trends
        vol_series['noise'] = noises
        vol_series = vol_series[['date','vol','trend','noise']].set_index('date')
        vol_series.dropna(inplace=True)
        return vol_series

    def calculate_trend_on_date(self,date):
        prev_date = self.get_prev_date(date)
        if prev_date not in self.vol_series.index:
            return 0
        if abs(self.vol_series.loc[prev_date,'trend']) >self.vol_series.loc[prev_date,'noise']:
            return self.vol_series.loc[prev_date,'trend']
        else:
            return 0
            
    def calculate_init_size(self, date, contract):
        prev_date = self.get_prev_date(date)
        price = self.get_contract_price(prev_date, contract)
        vol = self.vol_series.loc[prev_date,'vol']
        size_on_date = min(max(-self.max_position, self.vol_target/(vol*price)),self.max_position)*np.sign(self.calculate_trend_on_date(date))
        return size_on_date


    def calculate_size_on_date(self,date, price, position):
        prev_date = self.get_prev_date(date)
        vol = self.vol_series.loc[prev_date,'vol']
        if position * np.sign(self.calculate_trend_on_date(date)) < 0:
            size_on_date = min(max(-self.max_position, self.vol_target/(vol*price)),self.max_position)*np.sign(self.calculate_trend_on_date(date))
            return size_on_date
        else:
            return position    

    def vol_adjustment(self,date):
        positions = self.portfolio[date]['positions']
        if len(positions) != 1:
            contract = self.get_current_month_contract(date)
        else:
            contract = list(positions.keys())[0]

        prev_date = self.get_prev_date(date)
        price = self.get_contract_price(prev_date, contract)
        if len(positions) == 0:
            size = self.calculate_init_size(date, contract)*self.longshort
            trade_size = min(max(size,-self.max_daily_volume),self.max_daily_volume)
        else:
            size = self.calculate_size_on_date(prev_date, price, positions[contract])*self.longshort
            trade_size = min(max(size-positions[contract],-self.max_daily_volume),self.max_daily_volume)
        trades = self.portfolio[date]['trades']
        if self.roll_style == 'monthly':
            if self.round:
                trade_size = round_to_nearest_5(trade_size)
            trades.append(
            {
                'contract': contract,
                'quantity': trade_size,
                'date': date
            }
            )
        elif self.roll_style == 'quarterly':
            if self.round:
                trade_size = round_to_nearest_15(trade_size)
            trades.append(
            {
                'contract': contract,
                'quantity': trade_size,
                'date': date
            }
            )
        
        self.portfolio[date]['trades'] = trades.copy()

    def roll_position(self, date):
        
        if date in self.roll_start_dates:
            target_contract = self.get_current_month_contract(date)
            prev_date =date - pd.DateOffset(months=1)
            current_contracts = self.get_current_month_contract(prev_date)
            if current_contracts == target_contract:
                return True
            position = self.portfolio[date]['positions']
            
            roll_dates = [_date for _date in self.roll_dates if _date.month == date.month and _date.year == date.year]
            if current_contracts not in position:
                return True
            size = position[current_contracts]
            if size/len(roll_dates) > self.max_daily_volume:
                raise Exception(f"Daily volume of {size/len(roll_dates)} exceeds max daily volume of {self.max_daily_volume}")
            for roll_date in roll_dates:
                self.portfolio[date]['trades'].append(
                    {
                        'contract': current_contracts,
                        'quantity': -size/len(roll_dates)*self.roll_out,
                        'date': roll_date
                    }
                )
                self.portfolio[date]['trades'].append(
                    {
                        'contract': target_contract,
                        'quantity': size/len(roll_dates),
                        'date': roll_date
                    }
                )

        return True


    def run_backtest(self, 
                    start_date: datetime = None,
                    end_date: datetime = None) -> pd.DataFrame:

        if start_date is None:
            start_date = self.start_date
        if end_date is None:
            end_date = self.end_date

        dates = [date for date in self.trading_days if date>=start_date and date<=end_date]
        unexecuted_trades = []
        positions = {}
        for date in sorted(dates):

            self.ensure_portfolio_data(date,positions)
            if len(unexecuted_trades):
                self.portfolio[date]['trades'] = unexecuted_trades.copy()
            if date in self.roll_start_dates:
                self.roll_position(date)
            elif date not in self.roll_dates:
                self.vol_adjustment(date)

            unexecuted_trades = self.execute_trades(date)
           
            self.calculate_portfolio_value(date)
            positions = self.portfolio[date]['positions'].copy()
            positions = {key:value for key, value in positions.items() if abs(value) > 0.00000001}
        return self.portfolio

   
class COTBacktester(VOlROllingBacktest):    
    
    
    def __init__(self, 
                 data = pd.DataFrame(),
                 config = {},
                 trading_days = [],
                 last_trading_day = {},
                vol_series = pd.DataFrame()):
        super().__init__(data, config, trading_days, last_trading_day,vol_series)



    def get_cot_data(self):
        cot_data_sgx = pd.read_csv(f'{PATH}/data/series/SGX/SGX_COT.csv') 
        cot_data_sgx['date'] = pd.to_datetime(cot_data_sgx['Clear Date'])
        cot_data_sgx = cot_data_sgx[cot_data_sgx['Symbol'] == self.symbol]
        cot_data_sgx = cot_data_sgx.set_index('date')
        cot_data_sgx.fillna(0, inplace=True)
        cot_data_eex = pd.read_csv(f'{PATH}/data/series/EEX/EEX_COT.csv')
        cot_data_eex['date'] = pd.to_datetime(cot_data_eex['Clear Date'])
        cot_data_eex = cot_data_eex.set_index('date')
        cot_data_eex.fillna(0, inplace=True)
        cot_data_eex = cot_data_eex[cot_data_eex['Symbol'] == self.symbol]
        cot_data_eex = cot_data_eex.loc[cot_data_eex.index.isin(cot_data_sgx.index)] 
        col_list = [  'Open Interest', 'Physicals Long', 'Physicals Short', 'Managed Money Long', 'Managed Money Short', 'Financial Institutions Long', 'Financial Institutions Short']
        cot_data = cot_data_sgx[col_list].copy()
        cot_data['Managed Money Long'] = cot_data_sgx['Managed Money Long'].add(cot_data_eex['Managed Money Long'],fill_value=0)
        cot_data['Managed Money Short'] = cot_data_sgx['Managed Money Short'].add(cot_data_eex['Managed Money Short'],fill_value=0)
        cot_data.fillna(0, inplace=True)
        cot_data.reset_index(inplace=True)
        cot_data['MM Net'] = cot_data['Managed Money Long'] - cot_data['Managed Money Short']
        cot_data['MM Ratio'] = cot_data['MM Net'] / cot_data['Open Interest']
        cot_data['MM ZScore'] = (cot_data['MM Ratio'] - cot_data['MM Ratio'].rolling(26).mean()) / cot_data['MM Ratio'].rolling(20).std(ddof=0)
        cot_data['Diff'] =  cot_data['MM ZScore'].diff()
        cot_data.set_index('date',inplace= True)
        cot_data.dropna(inplace= True)
        return  cot_data[['MM ZScore','Diff']]

    def calculate_init_size(self, date, contract):
        prev_date = self.get_prev_date(date)
        price = self.get_contract_price(prev_date, contract)
        size_on_date = self.calculate_size_on_date(date, price, 0)
        return size_on_date
    
    def calculate_size_on_date(self,date, price, position):
        prev_date = self.get_prev_date(date)
        vol = self.vol_series.loc[prev_date,'vol']
        cot = self.get_cot_data()
        if date-pd.Timedelta(days=5) in list(cot.index):
            zscore = cot.loc[date-pd.Timedelta(days=5) ,'MM ZScore']
            diff = cot.loc[date-pd.Timedelta(days=5) ,'Diff']
            if abs(zscore)>1.5:
                if self.symbol == 'C5TC':
                    size_on_date = -min(max(-self.max_position, self.vol_target/(vol*price)),self.max_position)*np.sign(zscore)
                else:
                    size_on_date = min(max(-self.max_position, self.vol_target/(vol*price)),self.max_position)*np.sign(zscore)
            elif abs(diff) >0.6:
                size_on_date = min(max(-self.max_position, self.vol_target/(vol*price)),self.max_position)*np.sign(diff)*0.5
            else:
                return 0
            return size_on_date
        else:
            return position
                

    def vol_adjustment(self,date):
        positions = self.portfolio[date]['positions']
        if len(positions) != 1:
            contract = self.get_current_month_contract(date)
        else:
            contract = list(positions.keys())[0]
        prev_date = self.get_prev_date(date)
        price = self.get_contract_price(prev_date, contract)
        if len(positions) == 0:
            size = self.calculate_init_size(date, contract)*self.longshort
            trade_size = min(max(size,-self.max_daily_volume),self.max_daily_volume)
        else:
            size = self.calculate_size_on_date(prev_date, price, positions[contract])*self.longshort
            trade_size = min(max(size-positions[contract],-self.max_daily_volume),self.max_daily_volume)
        trades = self.portfolio[date]['trades']
        if self.roll_style == 'monthly':
            if self.round:
                trade_size = round_to_nearest_5(trade_size)
            trades.append(
            {
                'contract': contract,
                'quantity': trade_size,
                'date': date
            }
            )
        elif self.roll_style == 'quarterly':
            if self.round:
                trade_size = round_to_nearest_15(trade_size)
            trades.append(
            {
                'contract': contract,
                'quantity': trade_size,
                'date': date
            }
            )
        
        self.portfolio[date]['trades'] = trades.copy()

