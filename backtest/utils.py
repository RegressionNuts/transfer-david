#-------------------------
# function used for backtest and analysis the backtest history
#-------------------------


import pickle
import pandas as pd
import json
from backtester import *
import os
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib import dates as mdates
from matplotlib.colors import LinearSegmentedColormap
from dateutil.relativedelta import relativedelta

PATH = 'C:/Users/yuhang.hou/projects/transfer-david'

PREROLL = {
    'Q':[0,1],
}

CONTRACT_TYPE ={
    "Q": "quarterly",
}

MONTH_TO_NUM  = {  'F':1,'G':2,'H':3,'J':4,'K':5,'M':6,'N':7,'Q':8,'U':9,'V':10,'X':11,'Z':12 }
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

CONTRACT_FACTOR = {
    'monthly': 1,
    'quarterly': 3,
}

def load_future_data(data_path = './data/S10TC',values = 'close'):
    files = [f for f in os.listdir(data_path) if f.endswith('.csv')]
    dfs = (pd.read_csv(os.path.join(data_path, file)).assign(
        contract=file.replace('.csv', ''),
        date=lambda df: pd.to_datetime(df['date'])
    ) for file in files)

    big_df = pd.concat(dfs, ignore_index=True)
    pivot_df = big_df.pivot(index='date', columns='contract', values= values)

    return pivot_df

def load_and_process(index):
    with open(f'{PATH}/backtest/strategy/backtest/{index}.pkl', 'rb') as f:  # Note 'rb' mode
        history = pickle.load(f)

    curves = []
    for date,data in history.items():
        temp = {}
        temp['date'] = date
        temp['tc'] = data['tc']
        temp['level'] = data['level']
        curves.append(temp)
    final = pd.DataFrame(curves)
    final['tc_cumsum'] = final['tc'].cumsum()
    final.set_index('date',inplace=True)
    return final



def load_and_run_bt(index, strategy_type):
    if 'c5tc' in index: 
        contract_df = load_future_data(f'{PATH}/data/C5TC',['close'])
    elif 'p4tc' in index: 
        contract_df = load_future_data(f'{PATH}/data/P4TC',['close'])
    elif 's10tc' in index: 
        contract_df = load_future_data(f'{PATH}/data/S10TC',['close'])
    contract_df = contract_df.ffill()
    with open('last_trading_day.json', 'r') as f:
        last_trading_days = json.load(f)
        ltds = {k:pd.to_datetime(v) for k,v in last_trading_days.items()}
    with open('business_days.json', 'r') as f:
        business_days = json.load(f)
        business_days = [pd.to_datetime(i) for i in business_days]
        business_days = list(set(business_days))
        business_days = sorted(business_days)

    with open(f'{PATH}/backtest/strategy/config/{index}.json', 'r') as fi:
        config = json.load(fi)

    if strategy_type == 'KF':
        backtest = TrendBacktester(
            data = contract_df,
            config = config,
            trading_days=business_days,
            last_trading_day=ltds
        )
    elif strategy_type == 'LS':
        backtest = VOlROllingBacktest(
            data = contract_df,
            config = config,
            trading_days=business_days,
            last_trading_day=ltds
        )
    
    else:
        raise NotImplementedError
    results = backtest.run_backtest()
    with open(f'{PATH}/backtest/strategy/backtest/{index}.pkl', 'wb') as f:
        pickle.dump(results, f)


def monthly_pnl_attribution(price_series, title="Monthly Price Differences"):
    """
    Plot heatmap with monthly means (bottom) and yearly means (right).
    
    Parameters:
        price_series (pd.Series): Price time series with datetime index
        title (str): Plot title
        
    Returns:
        tuple: (monthly_mean, yearly_mean, pivot_table)
    """
    # Calculate differences
    diffs = price_series.diff().dropna()
    
    # Prepare DataFrame
    df = pd.DataFrame({
        'Diff': diffs,
        'Year': diffs.index.year,
        'Month': diffs.index.month_name(),
        'MonthNum': diffs.index.month
    })
    
    # Create pivot table
    month_order = ['January', 'February', 'March', 'April', 'May', 'June',
                   'July', 'August', 'September', 'October', 'November', 'December']
    pivot = df.pivot_table(index='Year', columns='Month', values='Diff', aggfunc='mean')[month_order]
    
    # Calculate means
    monthly_mean = pivot.mean()
    yearly_mean = pivot.mean(axis=1).sort_index(ascending=False)
    
    # Create figure with gridspec
    fig = plt.figure(figsize=(14, 8))
    gs = fig.add_gridspec(2, 2, width_ratios=[10, 1], height_ratios=[10, 1],
                         hspace=0.3, wspace=0.1)
    
    # Heatmap (main)
    ax1 = fig.add_subplot(gs[0, 0])
    sns.heatmap(pivot, annot=True, fmt=".1f", cmap="RdYlGn", center=0,
                linewidths=0.5, cbar=False, ax=ax1)
    ax1.set_title(title, pad=20)
    ax1.set_xlabel('')
    ax1.set_xticklabels(ax1.get_xticklabels(), rotation=45)
    
    # Yearly means (right)
    ax2 = fig.add_subplot(gs[0, 1])
    yearly_mean.plot(kind='barh', ax=ax2, color='firebrick')
    ax2.set_title('Avg by Year', pad=10)
    ax2.yaxis.tick_right()
    ax2.grid(axis='x', alpha=0.3)
    
    # Monthly means (bottom)
    ax3 = fig.add_subplot(gs[1, 0])
    monthly_mean.plot(kind='bar', ax=ax3, color='steelblue')
    ax3.set_title('Avg by Month', pad=10)
    ax3.grid(axis='y', alpha=0.3)
    plt.xticks(rotation=45)
    
    plt.tight_layout()
    plt.show()


def calculate_drawdown(price_series,plot=True):
    
    """
    Calculate drawdown statistics and plot both percentage and absolute drawdown information.

    Parameters:
        price_series (pd.Series): Price time series with datetime index
        plot (bool): Whether to plot the drawdown information

    Returns:
        tuple: (pct_table, abs_table)
    """

    if not isinstance(price_series, pd.Series):
        price_series = pd.Series(price_series)
    
    # 1. Calculate peaks
    previous_peaks = price_series.cummax()
    
    # 2. Calculate both types of drawdown series
    pct_drawdown = (price_series / previous_peaks) - 1  # Percentage drawdown
    abs_drawdown = price_series - previous_peaks       # Absolute drawdown
    
    # 3. Calculate max drawdown series (running max drawdown)
    max_pct_drawdown = pct_drawdown.cummin()
    max_abs_drawdown = abs_drawdown.cummin()
    
    # 4. Calculate drawdown statistics tables
    pct_table = calculate_drawdown_stats(pct_drawdown, price_series, is_pct=True)
    abs_table = calculate_drawdown_stats(abs_drawdown, price_series, is_pct=False)
    
    # 5. Plotting
    if plot:
        plot_drawdowns(price_series, pct_drawdown, abs_drawdown, 
                      max_pct_drawdown, max_abs_drawdown, pct_table)
    
    return pct_table, abs_table

def plot_drawdowns(price_series, pct_drawdown, abs_drawdown, 
                  max_pct_drawdown, max_abs_drawdown, pct_table):
    """
    Plot price series with both percentage and absolute drawdown information.
    """
    fig = plt.figure(figsize=(14, 10))
    gs = fig.add_gridspec(3, 1, height_ratios=[2, 1, 1])
    
    # Create axes
    ax1 = fig.add_subplot(gs[0])  # Price series
    ax2 = fig.add_subplot(gs[1])  # Percentage drawdown
    ax3 = fig.add_subplot(gs[2])  # Absolute drawdown
    
    # Custom colormap for drawdown periods
    cmap = LinearSegmentedColormap.from_list('dd_cmap', ['lightcoral', 'darkred'])
    
    # Plot price series
    ax1.plot(price_series.index, price_series, label='Price', color='black', linewidth=1.5)
    
    # Highlight max drawdown period if any
    if not pct_table.empty:
        max_dd = pct_table.iloc[0]  # Most severe drawdown (already sorted)
        max_dd_mask = (price_series.index >= max_dd['Start']) & (price_series.index <= max_dd['End'])
        ax1.plot(price_series.index[max_dd_mask], price_series[max_dd_mask], 
                color='red', linewidth=2.5, label='Max Drawdown Period')
        
        # Add annotation for max drawdown
        ax1.annotate(f"Max DD: {-max_dd['Depth']:.1f}%",
                    xy=(max_dd['Trough'], price_series[max_dd['Trough']]),
                    xytext=(10, 10), textcoords='offset points',
                    bbox=dict(boxstyle='round,pad=0.5', fc='red', alpha=0.3),
                    arrowprops=dict(arrowstyle='->'))
    
    ax1.set_ylabel('Price')
    ax1.legend(loc='upper left')
    ax1.grid(True, linestyle=':', alpha=0.7)
    
    # Plot percentage drawdown
    ax2.fill_between(pct_drawdown.index, pct_drawdown*100, 0, 
                    where=pct_drawdown<0, color='red', alpha=0.2)
    ax2.plot(pct_drawdown.index, pct_drawdown*100, color='darkred', label='Drawdown (%)', linewidth=1)
    ax2.plot(max_pct_drawdown.index, max_pct_drawdown*100, color='black', linestyle='--', label='Max Drawdown')
    
    ax2.set_ylabel('Drawdown (%)')
    ax2.axhline(0, color='black', linestyle='-', linewidth=0.5)
    ax2.legend(loc='lower left')
    ax2.grid(True, linestyle=':', alpha=0.7)
    
    # Plot absolute drawdown
    ax3.fill_between(abs_drawdown.index, abs_drawdown, 0, 
                    where=abs_drawdown<0, color='blue', alpha=0.2)
    ax3.plot(abs_drawdown.index, abs_drawdown, color='darkblue', label='Drawdown (Abs)', linewidth=1)
    ax3.plot(max_abs_drawdown.index, max_abs_drawdown, color='black', linestyle='--', label='Max Drawdown')
    
    ax3.set_ylabel('Drawdown (Absolute)')
    ax3.axhline(0, color='black', linestyle='-', linewidth=0.5)
    ax3.legend(loc='lower left')
    ax3.grid(True, linestyle=':', alpha=0.7)
    
    # Format x-axis for dates
    for ax in [ax1, ax2, ax3]:
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    
    fig.autofmt_xdate()
    plt.tight_layout()
    plt.show()
    
def calculate_drawdown_stats(drawdown_series, price_series, is_pct=True):
    """
    Calculate drawdown statistics including duration and recovery.
    """
    dates = price_series.index
    in_drawdown = drawdown_series < 0
    drawdown_start = (in_drawdown & ~in_drawdown.shift(1).fillna(False)).infer_objects(copy=False)
    drawdown_end = (in_drawdown & ~in_drawdown.shift(-1).fillna(False)).infer_objects(copy=False)
    
    start_dates = dates[drawdown_start]
    end_dates = dates[drawdown_end]
    
    stats = []
    for start in start_dates:
        # Get the drawdown period
        drawdown_period = (dates >= start) & (dates <= end_dates[end_dates >= start].min())
        current_drawdown = drawdown_series[drawdown_period]
        
        # Find trough (max drawdown point)
        trough_date = current_drawdown.idxmin()
        trough_value = current_drawdown.min()
        trough_price = price_series[trough_date]
        peak_price = price_series[price_series.index <= start].max()
        
        # Find recovery date (when back to previous peak)
        recovery_mask = (dates > trough_date) & (price_series >= peak_price)
        recovery_date = dates[recovery_mask].min() if recovery_mask.any() else pd.NaT
        
        # Calculate durations
        time_to_trough = (trough_date - start).days
        recovery_time = (recovery_date - trough_date).days if pd.notna(recovery_date) else np.nan
        
        stats.append({
            'Start': start,
            'Trough': trough_date,
            'End': recovery_date if pd.notna(recovery_date) else end_dates[end_dates >= start].min(),
            'Peak Value': peak_price,
            'Trough Value': trough_price,
            'Depth': trough_value * 100 if is_pct else trough_value,
            'Time to Trough (days)': time_to_trough,
            'Recovery Time (days)': recovery_time,
            'Total Duration (days)': (recovery_date - start).days if pd.notna(recovery_date) else np.nan
        })
    
    # Sort by depth (most severe first)
    stats_df = pd.DataFrame(stats)
    if not stats_df.empty:
        stats_df = stats_df.sort_values('Depth', ascending=True if is_pct else False)
    
    return stats_df


def plot_var(price_series, indices, max_roll_date=10):
    var_series = calculate_var(indices, max_roll_date)
    fig = plt.figure(figsize=(14, 12))
    gs = fig.add_gridspec(6, 1, height_ratios=[2,1,1,1,1,1])
    
    # Create axes
    ax1 = fig.add_subplot(gs[0])  # Price series
    ax2 = fig.add_subplot(gs[1])  # Percentage 
    ax3 = fig.add_subplot(gs[2])  # Percentage 
    ax4 = fig.add_subplot(gs[3])  # Percentage 
    ax5 = fig.add_subplot(gs[4])  # Percentage 
    ax6 = fig.add_subplot(gs[5])  # Percentage 
    # Custom colormap for drawdown periods
    cmap = LinearSegmentedColormap.from_list('dd_cmap', ['lightcoral', 'darkred'])
    
    # Plot price series
    ax1.plot(price_series.index, price_series, label='Price', color='black', linewidth=1.5)
    
    ax1.set_ylabel('Price')
    ax1.legend(loc='upper left')
    ax1.grid(True, linestyle=':', alpha=0.7)
    
    # Plot var
    ax2.plot(var_series.index, var_series['var'], color='darkred', label='var', linewidth=1)
    ax2.set_ylabel('Var')
    ax2.legend(loc='lower left')
    ax2.grid(True, linestyle=':', alpha=0.7)
    
    ax3.plot(var_series.index, var_series['abs_total'], color='darkred', label='abs_total', linewidth=1)
    ax3.set_ylabel('Abs Total Notional')
    ax3.legend(loc='lower left')
    ax3.grid(True, linestyle=':', alpha=0.7)

    ax4.plot(var_series.index, var_series['total'], color='darkred', label='total', linewidth=1)
    ax4.set_ylabel('Total Notional')
    ax4.legend(loc='lower left')
    ax4.grid(True, linestyle=':', alpha=0.7)

    ax5.plot(var_series.index, var_series['long'], color='darkred', label='long', linewidth=1)
    ax5.set_ylabel('Long Notional')
    ax5.legend(loc='lower left')
    ax5.grid(True, linestyle=':', alpha=0.7)

    ax6.plot(var_series.index, var_series['short'], color='darkred', label='short', linewidth=1)
    ax6.set_ylabel('Short Notional')
    ax6.legend(loc='lower left')
    ax6.grid(True, linestyle=':', alpha=0.7)
    
    # Format x-axis for dates
    for ax in [ax1, ax2,ax3,ax4,ax5,ax6]:
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    
    fig.autofmt_xdate()
    plt.tight_layout()
    plt.show()


def calculate_var(indices, max_roll_date=10):
    holding_curve = {}
    tickers = set()  # Using set to automatically handle duplicates
    
    # Step 1: Aggregate positions across all indices
    for index, size in indices.items():
        if 'c5tc' in index: 
            contract_df = load_future_data(f'{PATH}/data/C5TC',['close'])
        elif 'p4tc' in index: 
            contract_df = load_future_data(f'{PATH}/data/P4TC',['close'])
        elif 's10tc' in index: 
            contract_df = load_future_data(f'{PATH}/data/S10TC',['close'])
        try:
            with open(f'{PATH}/backtest/strategy/backtest/{index}.pkl', 'rb') as f:
                history = pickle.load(f)
            for date, data in history.items():
                positions = data.get('positions', {})  # Safe get with default empty list
                
                if date not in holding_curve:
                    holding_curve[date] = {}
                
                # Handle both (ticker,qty) pairs and more complex position formats
                for ticker, qty in positions.items():
                    if ticker == 'USD':
                        continue

                    base_ticker = (index.split('_')[0] ).upper()
                    if index.split('_')[1] == 'q':
                        ticker_symbol = base_ticker+  index.split('_')[1]
                    else:
                        ticker_symbol = base_ticker
                    ticker_symbol = ticker_symbol.upper()  
                    price = contract_df.loc[date,('close',ticker)]
                    holding_curve[date][base_ticker+ticker] = holding_curve[date].get(base_ticker+ticker, 0) + qty * price * size
                    tickers.add(ticker_symbol)
                # print(holding_curve)
        except FileNotFoundError:
            print(f"Warning: File for index {index} not found")
            continue
        except Exception as e:
            print(f"Error processing index {index}: {str(e)}")
            continue

    # Convert to list after collecting all unique tickers
    tickers = list(tickers)
    
    # Step 2: Load nearby series (assuming this function exists)
    nearby_series = load_nearby_series(tickers, max_roll_date)


    # Step 3: Calculate VaR for each date
    with open(f'{PATH}/backtest/last_trading_day.json', 'r') as f:
        last_trading_days = json.load(f)
    business_days = load_business_days()
    var_results = []
    for date in sorted(holding_curve.keys()):  # Process dates in order
        position = holding_curve[date]
        if not position:
            continue
            
        nearby_contracts = []
        position_values = []
        var_position_values = []
        for ticker, value in position.items():
            ticker_symbol = ticker[:-3]
            contract_month = ticker[-3:]
            try:
                contract_type=CONTRACT_TYPE.get(ticker_symbol[-1], 'monthly')
                nearby = contract_to_nearby(
                    date, 
                    contract_month, 
                    max_roll_date,
                    contract_type=contract_type,
                    trading_days=last_trading_days,
                    buz_days=business_days
                )
                nearby = max(nearby,0)
                nearby_contracts.append(f'{ticker_symbol}_{nearby}_{max_roll_date}')
                factor = 1 
                if contract_type == 'monthly' and nearby == 0:
                    factor = calculate_leftdays(date)
                position_values.append(value)
                var_position_values.append(value*factor)
            except Exception as e:
                print(f"Error getting nearby contract for {ticker} on {date}: {str(e)}")
                continue
        # Calculate VaR if we have data
        if nearby_contracts:
            try:
                returns = nearby_series[nearby_contracts].loc[:date].iloc[-252:]
                if len(returns) > 10:  # Minimum data requirement
                    abs_total =np.sum(np.abs(position_values))
                    total = np.sum(position_values)
                    long =np.sum([x for x in position_values if x>0])
                    short=np.sum([x for x in position_values if x<0])
                    portfolio_returns = returns @ var_position_values  # Matrix multiplication
                    var = np.percentile(portfolio_returns, 5)
                    var_results.append({'date': date, 'var': var,'abs_total':abs_total,'total':total,'long':long, 'short':short})
            except Exception as e:
                print(f"Error calculating VaR for {date}: {str(e)}")
    res =  pd.DataFrame(var_results).sort_values('date')
    res.set_index('date',inplace=True)
    return res
    
def calculate_leftdays(_date):
    if _date:
        _date = pd.to_datetime(_date)
    else:
        _date = pd.Timestamp.now()
    
    first_day = _date.replace(day=1)
    last_day = (first_day + relativedelta(months=1)) - timedelta(days=1)
    all_days = pd.date_range(start=first_day, end=last_day, freq='D')    
    remaining_days = pd.date_range(start=_date, end=last_day, freq='D')

    return len(remaining_days[remaining_days.dayofweek < 5])/len(all_days[all_days.dayofweek < 5])


def load_nearby_series(tickers, max_roll_date = 10):
    ts_list = []
    for ticker in tickers:
        if ticker[-1] in ['Q']:
            temp = ticker[:-1]
        else: 
            temp = ticker
        roll_nearbys = PREROLL.get(ticker[-1],[0,1,2])
        for roll_nearby in roll_nearbys:
            symbol = f'{ticker}_{roll_nearby}_{max_roll_date}'
            df = pd.read_csv(f'{PATH}/data/series/{temp}/{symbol}.csv')
            df['date'] = pd.to_datetime(df['date'])
            df = df.reset_index(drop=True).set_index('date')
            df.rename(columns={'return':symbol},inplace=True)
            ts_list.append(df[symbol])
    merged_df = pd.concat(ts_list,axis=1)
    return merged_df

def load_business_days():
    with open(f'{PATH}/backtest/business_days.json', 'r') as f:
        business_days = json.load(f)
    return [pd.to_datetime(i) for i in business_days]

def contract_to_nearby(_date, contract, preroll = 0, contract_type = 'monthly',trading_days = None,buz_days = None):
    """
    Convert a contract code to the nearest future contract code.
    """
    future_dates  =[ i  for i in buz_days if i > _date] 
    new_date = future_dates[preroll]
    if contract_type == 'quarterly':
        contract = 'Q' + contract
    ltd = pd.to_datetime(trading_days.get(contract))
    month = MONTH_TO_NUM[contract[-3]]
    year = 2000+ int(contract[-2:])

    this_month = new_date.month
    this_year = new_date.year
    k = month - this_month + 12*(year-this_year)
    if new_date>=ltd:
        k-=CONTRACT_FACTOR[contract_type]
    if contract_type == 'quarterly':
        k = k//3
    return k

def get_last_trading_day(month_code):
    with open(f'{PATH}/backtest/last_trading_day.json', 'r') as f:
        last_trading_days = json.load(f)
    return last_trading_days[month_code]
