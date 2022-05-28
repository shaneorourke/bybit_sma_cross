import pandas as pd
from pybit.usdt_perpetual import HTTP
import secrets as sc
import datetime as dt
import sqlite3 as sql
from time import sleep

from pytz import HOUR

conn = sql.connect('bybit_sma')
cur = conn.cursor()
cur.execute('CREATE TABLE IF NOT EXISTS Logs (id integer PRIMARY KEY AUTOINCREMENT, symbol text, close decimal, fast_sma decimal, slow_sma decimal, cross text, last_cross text, buy_sell text, buy_price decimal, sell_price decimal, market_date timestamp DEFAULT current_timestamp)')
cur.execute('INSERT OR REPLACE INTO Logs (id,symbol,close,fast_sma,slow_sma,cross) VALUES (1,NULL,0,0,0,"wait")')
cur.execute('CREATE TABLE IF NOT EXISTS take_profit_stop_loss (order_id text, bought_price real, current_take_profit real, current_stop_loss real)')
cur.execute('CREATE TABLE  IF NOT EXISTS Python_Orders ( "index" INTEGER, order_id TEXT, user_id INTEGER, symbol TEXT, side TEXT, order_type TEXT, price REAL, qty REAL, time_in_force TEXT, order_status TEXT, last_exec_price INTEGER, cum_exec_qty INTEGER, cum_exec_value INTEGER, cum_exec_fee INTEGER, reduce_only INTEGER, close_on_trigger INTEGER, order_link_id TEXT, created_time TEXT, updated_time TEXT, take_profit REAL, stop_loss REAL, tp_trigger_by TEXT, sl_trigger_by TEXT, position_idx INTEGER)')
conn.commit()

session = HTTP("https://api.bybit.com",
               api_key= sc.API_KEY, api_secret=sc.API_SECRET)
try:
    session.set_leverage(symbol="SOLUSDT",buy_leverage=1,sell_leverage=1)
except Exception as e:
    error = e

now_today = dt.datetime.now()
now = now_today + dt.timedelta(days=-2)
today = dt.datetime(now.year, now.month, now.day)

def applytechnicals(df):
    df['FastSMA'] = df.close.rolling(7).mean()
    df['SlowSMA'] = df.close.rolling(25).mean()
    return df

def get_bybit_bars(trading_symbol, interval, startTime):
    startTime = str(int(startTime.timestamp()))
    response = session.query_kline(symbol=trading_symbol,interval=interval,from_time=startTime)
    df = pd.DataFrame(response['result'])
    df.start_at = pd.to_datetime(df.start_at, unit='s') + pd.DateOffset(hours=1)
    df.open_time = pd.to_datetime(df.open_time, unit='s') + pd.DateOffset(hours=1)
    applytechnicals(df)
    return df

def insert_log(trading_symbol,close_price,fast_sma,slow_sma,cross,last_cross,buy_sell,buy_price,sell_price):
    if str(buy_sell).upper() not in ('LONG','SHORT'):
        buy_sell == None
    insert_query = f'INSERT INTO Logs (symbol,close,fast_sma,slow_sma,cross,last_cross,buy_sell,buy_price,sell_price) VALUES ("{trading_symbol}",{close_price},{fast_sma},{slow_sma},"{cross}","{last_cross}","{buy_sell}",{buy_price},{sell_price})'
    cur.execute(insert_query)
    conn.commit()

def read_last_log():
    query = 'SELECT id,symbol,close,fast_sma,slow_sma,cross,last_cross,market_date,buy_sell,buy_price,sell_price FROM logs ORDER BY id DESC LIMIT 1 '
    cur.execute(query)
    output = cur.fetchone()
    id = output[0]
    symbol = output[1]
    close = output[2]
    fast_sma = output[3]
    slow_sma = output[4]
    cross = output[5]
    last_cross = output[6]
    market_date = output[7]
    buy_sell = output[8]
    buy_price = output[9]
    sell_price = output[10]
    return id,symbol,close,fast_sma,slow_sma,cross,last_cross,market_date,buy_sell,buy_price,sell_price

def get_quantity(close_price):
    funds = pd.DataFrame(session.get_wallet_balance()['result'])
    funds.to_sql(con=conn,name='Funds',if_exists='replace')
    get_available_bal = 'select USDT from Funds where "index" = "available_balance"'
    cur.execute(get_available_bal)
    available_balance = float(str(cur.fetchone()).replace('(','').replace(')','').replace(',',''))
    close_price = 53.865
    qty = round((available_balance / close_price),1)
    return qty

def get_last_cross():
    last_cross_query = """with First_Dir as (select "index" as id
                            , case when FastSMA > SlowSMA then 'up' when SlowSMA > FastSMA then 'down' else 'wait' end as dir
                            from Candles
                            where "index" != (select max("index") from Candles)
                            and FastSMA != 'NULL'
                            and SlowSMA != 'NULL')
                            , Second_Dir as (select "index" as id
                            , case when FastSMA > SlowSMA then 'up' when SlowSMA > FastSMA then 'down' else 'wait' end as dir
                            from Candles
                            where "index" != (select max("index") from Candles)
                            and FastSMA != 'NULL'
                            and SlowSMA != 'NULL')
                            select fd.id, fd.dir Dir_From, sd.dir Dir_To from First_Dir fd
                            inner join Second_Dir sd
                            on fd.id = sd.id-1
                            where fd.dir != sd.dir
                            order by fd.id DESC
                            limit 1;"""
    cur.execute(last_cross_query)
    result = cur.fetchone()
    if result != None or result != 'None':
        last_cross = str(result[2]).replace('(','').replace(')','').replace(',','')
    else:
        last_cross = 'wait'
    return last_cross

def sma_cross_strategy(fast_sma,slow_sma,trading_symbol,close_price):
    
        if float(fast_sma) > float(slow_sma):
            cross = 'up'
        if float(slow_sma) > float(fast_sma):
            cross = 'down'
        elif not float(fast_sma) < float(slow_sma) and not float(slow_sma) < float(fast_sma):
            cross = 'wait'

        buy_sell = ''
        buy_price = 0
        sell_price = 0
        last_cross = get_last_cross()
        
        if last_cross == 'down' and cross == 'up':
            print(f'{now_today}:LONG')
            buy_sell = 'LONG'
            buy_price = close_price
            take_profit_var = round(buy_price+(buy_price * 0.01),3) #1%
            stop_loss_var = round(buy_price-(buy_price * 0.015),3) #-1.5%
            quantity = get_quantity(close_price)
            session.place_active_order(symbol=trading_symbol,
                                    side="Buy",
                                    order_type="Market",
                                    qty=quantity,
                                    price=buy_price,
                                    time_in_force="ImmediateOrCancel",
                                    reduce_only=False,
                                    close_on_trigger=False,
                                    take_profit=take_profit_var,
                                    stop_loss=stop_loss_var)
        if last_cross == 'up' and cross == 'down':
            print(f'{now_today}:SHORT')
            buy_sell == 'SHORT'
            buy_price = close_price
            take_profit_var = round(buy_price-(buy_price * 0.01),3) #1%
            stop_loss_var = round(buy_price+(buy_price * 0.015),3) #-1.5%
            quantity = get_quantity(close_price)
            session.place_active_order(symbol=trading_symbol,
                                    side="Sell",
                                    order_type="Market",
                                    qty=quantity,
                                    price=buy_price,
                                    time_in_force="ImmediateOrCancel",
                                    reduce_only=False,
                                    close_on_trigger=False,
                                    take_profit=take_profit_var,
                                    stop_loss=stop_loss_var)
        insert_log(trading_symbol,close_price,fast_sma,slow_sma,cross,last_cross,buy_sell,buy_price,sell_price)

def place_order(trading_symbol,order_side,quantity,buy_price,take_profit_var,stop_loss_var,trailing_stop_take_profit):
    if not trailing_stop_take_profit:
        order_df = pd.DataFrame(session.place_active_order(symbol=trading_symbol,
                                    side=f"{order_side}",
                                    order_type="Market",
                                    qty=quantity,
                                    price=buy_price,
                                    time_in_force="ImmediateOrCancel",
                                    reduce_only=False,
                                    close_on_trigger=False,
                                    take_profit=take_profit_var,
                                    stop_loss=stop_loss_var)['result'],index=[0])
    else:
        order_df = pd.DataFrame(session.place_active_order(symbol=trading_symbol,
                                    side=f"{order_side}",
                                    order_type="Market",
                                    qty=quantity,
                                    price=buy_price,
                                    time_in_force="ImmediateOrCancel",
                                    reduce_only=False,
                                    close_on_trigger=False)['result'],index=[0])
        amend_take_profit_stop_loss(order_df.order_id,buy_price,take_profit_var,stop_loss_var)
    order_df.to_sql(con=conn,name='Python_Orders',if_exists='replace')


def sma_bounce_strategy(fast_sma,slow_sma,trading_symbol,close_price,trailing_stop_take_profit):
    if float(fast_sma) > float(slow_sma):
        cross = 'up'
    if float(slow_sma) > float(fast_sma):
        cross = 'down'
    elif not float(fast_sma) < float(slow_sma) and not float(slow_sma) < float(fast_sma):
        cross = 'wait'

    buy_sell = ''
    buy_price = 0
    sell_price = 0
    last_cross = get_last_cross()
     
    if float(fast_sma) > float(slow_sma) and float(close_price) < float(slow_sma):
        print('LONG')
        buy_sell = 'LONG'
        buy_price = close_price
        take_profit_var = round(buy_price+(buy_price * 0.01),3) #1%
        stop_loss_var = round(buy_price-(buy_price * 0.015),3) #-1.5%
        quantity = get_quantity(close_price)
        place_order(trading_symbol,"Buy",quantity,buy_price,take_profit_var,stop_loss_var,trailing_stop_take_profit)


    if float(slow_sma) > float(fast_sma) and float(close_price) > float(slow_sma):
        print('SHORT')
        buy_sell == 'SHORT'
        buy_price = close_price
        take_profit_var = round(buy_price-(buy_price * 0.01),3) #1%
        stop_loss_var = round(buy_price+(buy_price * 0.015),3) #-1.5%
        quantity = get_quantity(close_price)
        place_order(trading_symbol,"Sell",quantity,buy_price,take_profit_var,stop_loss_var,trailing_stop_take_profit)

    insert_log(trading_symbol,close_price,fast_sma,slow_sma,cross,last_cross,buy_sell,buy_price,sell_price)

def get_last_python_order(trading_symbol):
    cur.execute(f'select order_id from Python_Orders where symbol="{trading_symbol}" order by updated_time desc')
    order_id = str(cur.fetchone()).replace('(','').replace(')','').replace(',','')
    return order_id

def get_last_python_order_price(order_id):
    bought_price_query = f'select price from Python_Orders where order_id="{order_id}" order by updated_time desc'
    print(bought_price_query)
    cur.execute(bought_price_query)
    bought_price = float(str(cur.fetchone()).replace('(','').replace(')','').replace(',',''))
    return bought_price

def get_last_python_order_side(order_id):
    side_query = f'select side from Python_Orders where order_id="{order_id}" order by updated_time desc'
    cur.execute(side_query)
    side = str(cur.fetchone()).replace('(','').replace(')','').replace(',','')
    return side

def amend_take_profit_stop_loss(order_id,bought_price,take_profit,stop_loss):
    cur.execute(f'select count(*) from take_profit_stop_loss where order_id = {order_id}')
    row_exists = int(str(cur.fetchone()).replace('(','').replace(')','').replace(',',''))
    if row_exists == 1:
        cur.execute(f'update take_profit_stop_loss set current_take_profit={take_profit}, current_stop_loss={stop_loss} where order_id = {order_id}')
    else:
        cur.execute(f'insert into take_profit_stop_loss (order_id, bought_price, current_take_profit, current_stop_loss) values ("{order_id}",{bought_price},{take_profit},{stop_loss})')
    conn.commit()

def get_current_tp_sl(order_id):
    cur.execute(f'select current_take_profit from take_profit_stop_loss where order_id = "{order_id}"')
    tp = float(str(cur.fetchone()).replace('(','').replace(')','').replace(',',''))
    cur.execute(f'select current_stop_loss from take_profit_stop_loss where order_id = "{order_id}"')
    sl = float(str(cur.fetchone()).replace('(','').replace(')','').replace(',',''))
    conn.commit()
    return tp, sl

def close_position(trading_symbol,order_id):
    session.close_position(symbol=trading_symbol)
    cur.execute(f'delete from take_profit_stop_loss where order_id ="{order_id}"')
    conn.commit()

def check_python_orders():
    cur.execute('select count(*) from Python_Orders')
    row_exists = int(str(cur.fetchone()).replace('(','').replace(')','').replace(',',''))
    if row_exists == 1:
        return True
    else:
        print('No order recorded in this database yet')
        return False

if __name__ == '__main__':
    while True:
        trading_symbol = "SOLUSDT"
        interval='60'
        trailing_stop_take_profit = True
        candles = get_bybit_bars(trading_symbol,interval,today)
        candles.to_sql(con=conn,name='Candles',if_exists='replace')
        most_recent = candles.iloc[-1]
        close_price = most_recent.close
        fast_sma = most_recent.FastSMA
        slow_sma = most_recent.SlowSMA

        orders = pd.DataFrame(session.get_active_order(symbol=trading_symbol)['result']['data'])
        orders.to_sql(con=conn,name='Orders',if_exists='replace')
        user_trade_records = pd.DataFrame(session.user_trade_records(symbol=trading_symbol)['result']['data'])
        user_trade_records.trade_time_ms = pd.to_datetime(user_trade_records.trade_time_ms, unit='ms') + pd.DateOffset(hours=1)
        user_trade_records.to_sql(con=conn,name='User_Trade_Records',if_exists='replace')

        position = pd.DataFrame(session.my_position(symbol=trading_symbol)['result'])
        position.to_sql(con=conn,name='Position',if_exists='replace')
        open_position = position[position.columns[0]].count()
        cur.execute(f'select sum(size) from Position')
        open_position = float(str(cur.fetchone()).replace('(','').replace(')','').replace(',',''))

        if not open_position > 0.0: #If a position is NOT open, e.g. not open else wait for tp and sl
            sma_bounce_strategy(fast_sma,slow_sma,trading_symbol,close_price,trailing_stop_take_profit)

        if open_position > 0.0 and trailing_stop_take_profit and check_python_orders():
            print('Open Position Trailing Stop')
            order_id = get_last_python_order(trading_symbol)
            bought_price = get_last_python_order_price(order_id)
            last_order_side = get_last_python_order_side(order_id)
            current_tp = get_current_tp_sl(order_id)[0]
            current_sl = get_current_tp_sl(order_id)[1]


            if last_order_side == 'Sell':
                if close_price < current_sl:
                    print('close - short - stop loss')
                    close_position(trading_symbol,order_id)

                if close_price > current_tp:
                    print('Upping TP SL - Short')
                    take_profit = round(close_price-(close_price * 0.01),3)
                    stop_loss = round(close_price+(close_price * 0.015),3)
                    amend_take_profit_stop_loss(order_id,bought_price,take_profit,stop_loss)

            if last_order_side == 'Buy':
                if close_price < current_sl:
                    print('close - long - stop loss')
                    close_position(trading_symbol,order_id)
                if close_price > current_tp:
                    print('Upping TP SL - Long')
                    take_profit = round(close_price+(close_price * 0.01),3)
                    stop_loss = round(close_price-(close_price * 0.015),3)
                    amend_take_profit_stop_loss(order_id,bought_price,take_profit,stop_loss)

        cur.close()
        conn.close()
        conn = sql.connect('bybit_sma')
        cur = conn.cursor()
        PandL =  pd.DataFrame(session.closed_profit_and_loss(symbol=trading_symbol)['result']['data'])
        PandL.created_at = pd.to_datetime(PandL.created_at, unit='s') + pd.DateOffset(hours=1)
        PandL.to_sql(con=conn,name='Profit_Loss',if_exists='replace')

        read_last_log()
