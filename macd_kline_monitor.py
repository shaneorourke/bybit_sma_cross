import pandas as pd
from pybit.usdt_perpetual import HTTP
import bybit_secrets as sc
import datetime as dt
import sqlite3 as sql
from time import sleep
import ta

from pytz import HOUR

conn = sql.connect('bybit_sma')
cur = conn.cursor()

session = HTTP("https://api.bybit.com",
               api_key= sc.API_KEY, api_secret=sc.API_SECRET,request_timeout=30)
try:
    session.set_leverage(symbol="SOLUSDT",buy_leverage=1,sell_leverage=1)
except Exception as e:
    error = e

now_today = dt.datetime.now()
now_timestamp = dt.datetime.now()
now = now_today + dt.timedelta(days=-3)
today = dt.datetime(now.year, now.month, now.day)

def applytechnicals(df):
    df['FastSMA'] = df.close.rolling(7).mean()
    df['SlowSMA'] = df.close.rolling(25).mean()
    df['%K'] = ta.momentum.stoch(df.high,df.low,df.close,window=14,smooth_window=3)
    df['%D'] = df['%K'].rolling(3).mean()
    df['rsi'] = ta.momentum.rsi(df.close,window=14)
    df['macd'] = ta.trend.macd_diff(df.close)
    df.dropna(inplace=True)
    return df

def get_bybit_bars(trading_symbol, interval, startTime):
    startTime = str(int(startTime.timestamp()))
    response = session.query_kline(symbol=trading_symbol,interval=interval,from_time=startTime)
    df = pd.DataFrame(response['result'])
    df.start_at = pd.to_datetime(df.start_at, unit='s') + pd.DateOffset(hours=1)
    df.open_time = pd.to_datetime(df.open_time, unit='s') + pd.DateOffset(hours=1)
    applytechnicals(df)
    return df

def check_open_position():
    position = pd.DataFrame(session.my_position(symbol=trading_symbol)['result'])
    open_position = position[position.columns[0]].count()
    cur.execute(f'select sum(size) from Position')
    open_position = float(str(cur.fetchone()).replace('(','').replace(')','').replace(',',''))
    return open_position

def get_last_order(trading_symbol):
    cur.execute(f'select order_id from Orders where symbol="{trading_symbol}" order by updated_time desc')
    order_id = str(cur.fetchone()).replace('(','').replace(')','').replace(',','')
    cur.execute(f'select last_exec_price from Orders where order_id={order_id} order by updated_time desc')
    price = float(str(cur.fetchone()).replace('(','').replace(')','').replace(',',''))
    cur.execute(f'select side from Orders where order_id={order_id} order by updated_time desc')
    side = str(cur.fetchone()).replace('(','').replace(')','').replace(',','')
    return order_id, price, side

def stock_macd_entry_strategy(trading_symbol,close_price,macd,kline,dline,previous_kline,previous_previous_kline):
    # Buy when: MACD > 0 and kline crosses UP 20
    # Sell when: MACD < 0 and kline crosses DOWN 80
    macd = round(macd,2)
    kline = round(kline,2)
    previous_kline = round(previous_kline,2)
    previous_previous_kline = round(previous_previous_kline,2)
    print(f'trading_symbol:{trading_symbol}')
    if macd > 0:
        print(f'BUY SIGNAL (MACD > 0 and KLINE crosses OVER 20)')
        print(f'macd:{macd}')
        if kline > 20:
            kline_dif = 20 - kline
            print(f'kline:{kline} needs to drop:{kline_dif}')
        #if kline < 20 and (previous_kline > 20 or previous_previous_kline > 20):
        if kline < 20 and previous_kline > 20:
            if previous_kline > 20:
                previous_kline_dif = 20 - previous_kline
                print(f'previous_kline:{previous_kline} needs to drop:{previous_kline_dif}')
            #if previous_previous_kline > 20:
            #    previous_previous_kline_dif = 20 - previous_previous_kline
            #    print(f'previous_previous_kline:{previous_previous_kline} needs to drop:{previous_previous_kline_dif}')
        #if kline <= 20 and not (previous_kline > 20 or previous_previous_kline > 20):
        if kline <= 20 and not previous_kline > 20:
            kline_dif = 20 - kline
            print(f'kline:{kline} needs to rise:{kline_dif}')
        #if kline > 20 and (previous_kline < 20 or previous_previous_kline < 20):
        if kline > 20 and previous_kline < 20:
            print('BUY SIGNAL ON! GO LONG')
    if macd < 0:
        print(f'SELL SIGNAL (MACD < 0 and KLINE crosses UNDER 80)')
        print(f'macd:{macd}')
        if kline < 80:
            kline_dif = 80 - kline
            print(f'kline:{kline} needs to rise:{kline_dif}')
        #if kline > 80 and (previous_kline < 80 or previous_previous_kline < 80):
        if kline > 80 and previous_kline < 80:
            if previous_kline < 80:
                previous_kline_dif = 80 - previous_kline
                print(f'previous_kline:{previous_kline} needs to rise:{previous_kline_dif}')
            #if previous_previous_kline < 80:
            #    previous_previous_kline_dif = 80 - previous_previous_kline
            #    print(f'previous_previous_kline:{previous_previous_kline} needs to rise:{previous_previous_kline_dif}')
        #if kline >= 80 and not (previous_kline < 80 or previous_previous_kline < 80):
        if kline >= 80 and not previous_kline < 80:
            kline_dif = 80 - kline
            print(f'kline:{kline} needs to drop:{kline_dif}')
        #if kline < 80 and (previous_kline > 80 or previous_previous_kline > 80):
        if kline < 80 and previous_kline > 80:
            print('SELL SIGNAL ON! GO SHORT')
    if kline > previous_kline:
        print('kline rising')
    if kline < previous_kline:
        print('kline falling')

if __name__ == '__main__':
    trading_symbol = "SOLUSDT"
    interval='60'
    trailing_stop_take_profit = True
    candles = get_bybit_bars(trading_symbol,interval,today)
    most_recent = candles.iloc[-1]
    close_price = most_recent.close
    fast_sma = most_recent.FastSMA
    slow_sma = most_recent.SlowSMA
    kline = most_recent['%K']
    dline = most_recent['%D']
    rsi = most_recent['rsi']
    macd = most_recent['macd']
    ## Turning off SMA Cross Strategy
    #open_position = check_open_position()
    #if not open_position > 0.0: #If a position is NOT open, e.g. not open else wait for tp and sl
    #    sma_cross_strategy(fast_sma,slow_sma,trading_symbol,close_price,trailing_stop_take_profit)

    open_position = check_open_position()
    #if not open_position > 0.0: #If a position is NOT open, e.g. not open else wait for tp and sl
    #    sma_bounce_strategy(fast_sma,slow_sma,trading_symbol,close_price,trailing_stop_take_profit)
    #if open_position > 0.0 and trailing_stop_take_profit:
    #    trailing_sl = trailing_stop_loss(trading_symbol,close_price,fast_sma,slow_sma)
    previous_close = candles.iloc[-2]
    previous_kline = previous_close['%K']
    previous_previous_close = candles.iloc[-3]
    previous_previous_kline = previous_previous_close['%K']
    print(f'{now_timestamp}')
    if not open_position > 0.0:
        stock_macd_entry_strategy(trading_symbol,close_price,macd,kline,dline,previous_kline,previous_previous_kline)

    if open_position > 0.0:
        print('OPEN POSITION')
    #    stock_macd_exit_strategy(trading_symbol,close_price,macd,kline,dline,previous_kline,previous_previous_kline)

    cur.close()
    conn.close()