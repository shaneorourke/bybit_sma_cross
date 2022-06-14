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
conn.commit()

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

def print_Last_log():
    log = read_last_log()
    print(f'{now_today}:id:{log[0]}')
    print(f'{now_today}:symbol:{log[1]}')
    print(f'{now_today}:close:{log[2]}')
    print(f'{now_today}:fast_sma:{log[3]}')
    print(f'{now_today}:slow_sma:{log[4]}')
    print(f'{now_today}:cross:{log[5]}')
    print(f'{now_today}:last_cross:{log[6]}')
    print()

def get_quantity(close_price):
    funds = pd.DataFrame(session.get_wallet_balance()['result'])
    funds.to_sql(con=conn,name='Funds',if_exists='replace')
    get_available_bal = 'select USDT from Funds where "index" = "available_balance"'
    cur.execute(get_available_bal)
    available_balance = float(str(cur.fetchone()).replace('(','').replace(')','').replace(',',''))
    qty = round((available_balance / close_price),1)
    qty = round(qty - 0.1,2)
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
    if result != None and result != 'None':
        last_cross = str(result[2]).replace('(','').replace(')','').replace(',','')
    else:
        last_cross = 'wait'
    return last_cross

def sma_cross_strategy(fast_sma,slow_sma,trading_symbol,close_price,trailing_stop_take_profit):
    
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
            place_order(trading_symbol,"Buy",quantity,buy_price,take_profit_var,stop_loss_var,trailing_stop_take_profit)

        if last_cross == 'up' and cross == 'down':
            print(f'{now_today}:SHORT')
            buy_sell == 'SHORT'
            buy_price = close_price
            take_profit_var = round(buy_price-(buy_price * 0.01),3) #1%
            stop_loss_var = round(buy_price+(buy_price * 0.015),3) #-1.5%
            quantity = get_quantity(close_price)
            place_order(trading_symbol,"Buy",quantity,buy_price,take_profit_var,stop_loss_var,trailing_stop_take_profit)

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
        order_id = order_df['order_id'].to_string(index=False)
        amend_take_profit_stop_loss(order_id,buy_price,take_profit_var,stop_loss_var)
    conn.commit()


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

    try:
        cur.execute('select buy_sell,fast_sma, slow_sma from last_order order by timestamp DESC limit 1;')
        last_results = cur.fetchone()
        last_buy_sell = last_results[0]
        last_fast_sma = last_results[1]
        last_slow_sma = last_results[2]
    except Exception as E:
        print(f'{now_today}:last_results exception:{last_results}')
        last_buy_sell = ''
        last_fast_sma = 0
        last_slow_sma = 0

    try:
        cur.execute('select status from status order by timestamp DESC limit 1;')
        current_status = cur.fetchone()
        ready_status = current_status[0]
    except Exception as E:
        print(f'{now_today}:ready_status exception change:{ready_status}')
        ready_status = 'ready'

    print(f'{now_today}:ready_status:{ready_status}')
    if float(last_fast_sma) > float(last_slow_sma) and last_buy_sell == 'LONG' and ready_status != 'ready':
        if float(close_price) > float(fast_sma) or float(slow_sma) > float(fast_sma):
            print(f'{now_today}:ready_status change:{ready_status}')
            waiting_dict = {'status':'ready','timestamp':now_today}
            status = pd.DataFrame([waiting_dict])
            status.to_sql(name='status',con=conn,if_exists='replace')

    if float(last_fast_sma) < float(last_slow_sma) and last_buy_sell == 'SHORT' and ready_status != 'ready':
        if float(close_price) < float(fast_sma) or float(slow_sma) < float(fast_sma):
            print(f'{now_today}:ready_status change:{ready_status}')
            waiting_dict = {'status':'ready','timestamp':now_today}
            status = pd.DataFrame([waiting_dict])
            status.to_sql(name='status',con=conn,if_exists='replace')
    
    if ready_status == 'ready':
        if float(fast_sma) > float(slow_sma) and float(close_price) < float(slow_sma):
            print(f'{now_today}:LONG')
            buy_sell = 'LONG'
            buy_price = close_price
            take_profit_var = round(buy_price+(buy_price * 0.015),3) #1.5% 
            stop_loss_var = round(buy_price-(buy_price * 0.02),3) #-2%
            quantity = get_quantity(close_price)

            place_order(trading_symbol,"Buy",quantity,buy_price,take_profit_var,stop_loss_var,trailing_stop_take_profit)

            order_dict = {'buy_sell':buy_sell,'buy_price':buy_price,'fast_sma':fast_sma,'slow_sma':slow_sma,'close_price':close_price,'timestamp':now_today}
            last_order_df = pd.DataFrame([order_dict])
            last_order_df.to_sql(name='last_order',con=conn,if_exists='replace')

            print(f'{now_today}:ready_status change:waiting')
            waiting_dict = {'status':'waiting'}
            status = pd.DataFrame([waiting_dict])
            status.to_sql(name='status',con=conn,if_exists='replace')

        if float(slow_sma) > float(fast_sma) and float(close_price) > float(slow_sma):
            print(f'{now_today}:SHORT')
            buy_sell == 'SHORT'
            buy_price = close_price
            take_profit_var = round(buy_price-(buy_price * 0.015),3) #-1.5%
            stop_loss_var = round(buy_price+(buy_price * 0.02),3) #+2%
            quantity = get_quantity(close_price)
            place_order(trading_symbol,"Sell",quantity,buy_price,take_profit_var,stop_loss_var,trailing_stop_take_profit)

            order_dict = {'buy_sell':buy_sell,'buy_price':buy_price,'fast_sma':fast_sma,'slow_sma':slow_sma,'close_price':close_price,'timestamp':now_today}
            last_order_df = pd.DataFrame([order_dict])
            last_order_df.to_sql(name='last_order',con=conn,if_exists='replace')

            print(f'{now_today}:ready_status change:waiting')
            waiting_dict = {'status':'waiting'}
            status = pd.DataFrame([waiting_dict])
            status.to_sql(name='status',con=conn,if_exists='replace')

    insert_log(trading_symbol,close_price,fast_sma,slow_sma,cross,last_cross,buy_sell,buy_price,sell_price)

def trailing_stop_loss(trading_symbol,close_price,fast_sma,slow_sma):
    print(f'{now_today}:Open Position Trailing Stop')
    order_id = get_last_order(trading_symbol)[0]
    bought_price = get_last_order(trading_symbol)[1]
    last_order_side = get_last_order(trading_symbol)[2]
    current_tp = get_current_tp_sl(order_id)[0]
    current_sl = get_current_tp_sl(order_id)[1]
    if last_order_side == "'Sell'":
        if close_price > current_sl:
            print(f'{now_today}:close - short - stop loss')
            close_position(trading_symbol,order_id)
        if close_price < bought_price:
            print(f'{now_today}:Upping TP SL - Short')
            stop_loss = round(close_price+(close_price * 0.02),3) # Up SL to +0.5% of Close (Rasing more than non-trailing for more gains)
            if stop_loss < current_sl:
                amend_take_profit_stop_loss(order_id,bought_price,current_tp,stop_loss)
                
    if last_order_side == "'Buy'":
        if close_price < current_sl:
            print(f'{now_today}:close - long - stop loss')
            close_position(trading_symbol,order_id)
        if close_price > bought_price:
            print(f'{now_today}:Upping TP SL - Long')
            stop_loss = round(close_price-(close_price * 0.02),3) # Up SL to -0.5% of Close (Rasing more than non-trailing for more gains)
            if stop_loss > current_sl:
                amend_take_profit_stop_loss(order_id,bought_price,current_tp,stop_loss)

    insert_log(trading_symbol,close_price,fast_sma,slow_sma,'na',get_last_cross(),last_order_side,bought_price,0)
    
    return bought_price, last_order_side

def get_last_order(trading_symbol):
    cur.execute(f'select order_id from Orders where symbol="{trading_symbol}" order by updated_time desc')
    order_id = str(cur.fetchone()).replace('(','').replace(')','').replace(',','')
    cur.execute(f'select last_exec_price from Orders where order_id={order_id} order by updated_time desc')
    price = float(str(cur.fetchone()).replace('(','').replace(')','').replace(',',''))
    cur.execute(f'select side from Orders where order_id={order_id} order by updated_time desc')
    side = str(cur.fetchone()).replace('(','').replace(')','').replace(',','')
    return order_id, price, side

def amend_take_profit_stop_loss(order_id,bought_price,take_profit,stop_loss):
    print(f'{now_today}:Amending Stop')
    order_id = str(order_id).replace("'","")
    cur.execute(f'select count(*) from take_profit_stop_loss where order_id = "{order_id}"')
    row_exists = int(str(cur.fetchone()).replace('(','').replace(')','').replace(',',''))
    if row_exists == 1:
        cur.execute(f'update take_profit_stop_loss set current_take_profit={take_profit}, current_stop_loss={stop_loss} where order_id = "{order_id}"')
    else:
        cur.execute(f'insert into take_profit_stop_loss (order_id, bought_price, current_take_profit, current_stop_loss) values ("{order_id}",{bought_price},{take_profit},{stop_loss})')
    conn.commit()

def get_current_tp_sl(order_id):
    cur.execute(f'select current_take_profit from take_profit_stop_loss where order_id = {order_id}')
    tp = float(str(cur.fetchone()).replace('(','').replace(')','').replace(',',''))
    cur.execute(f'select current_stop_loss from take_profit_stop_loss where order_id = {order_id}')
    sl = float(str(cur.fetchone()).replace('(','').replace(')','').replace(',',''))
    conn.commit()
    return tp, sl

def close_position(trading_symbol,order_id):
    session.close_position(symbol=trading_symbol)
    cur.execute(f'delete from take_profit_stop_loss where order_id ="{order_id}"')
    conn.commit()

def check_open_position():
    position = pd.DataFrame(session.my_position(symbol=trading_symbol)['result'])
    position.to_sql(con=conn,name='Position',if_exists='replace')
    open_position = position[position.columns[0]].count()
    cur.execute(f'select sum(size) from Position')
    open_position = float(str(cur.fetchone()).replace('(','').replace(')','').replace(',',''))
    return open_position

def get_trend():
    ## Currently unused
    trend_query = """with first_row as (select "index",id,close,FastSMA,SlowSMA 
                            from Candles
                            where FastSMA is not null and SlowSMA is not null)
        , second_row as (select "index",id,close,FastSMA,SlowSMA
                            from Candles
                            where FastSMA is not null and SlowSMA is not null)
        , trend as (select case when fr.SlowSMA < sr.SlowSMA then 'down' when fr.SlowSMA > sr.SlowSMA then 'up' else 'undetermined' end as trend from first_row fr 
                    inner join second_row sr
                    on fr."index" = sr."index"+1)
        , overall_trend as (select trend, count(trend) trend_count from trend t
                    group by trend)
        select trend from overall_trend
        order by trend_count DESC
        limit 1;"""
    cur.execute(trend_query)
    trend = str(cur.fetchone()).replace('(','').replace(')','').replace(',','')
    return trend


if __name__ == '__main__':
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
    open_position = check_open_position()
    if not open_position > 0.0: #If a position is NOT open, e.g. not open else wait for tp and sl
        sma_cross_strategy(fast_sma,slow_sma,trading_symbol,close_price,trailing_stop_take_profit)
    open_position = check_open_position()
    if not open_position > 0.0: #If a position is NOT open, e.g. not open else wait for tp and sl
        sma_bounce_strategy(fast_sma,slow_sma,trading_symbol,close_price,trailing_stop_take_profit)
    if open_position > 0.0 and trailing_stop_take_profit:
        trailing_sl = trailing_stop_loss(trading_symbol,close_price,fast_sma,slow_sma)
    cur.close()
    conn.close()
    conn = sql.connect('bybit_sma')
    cur = conn.cursor()
    PandL =  pd.DataFrame(session.closed_profit_and_loss(symbol=trading_symbol)['result']['data'])
    PandL.created_at = pd.to_datetime(PandL.created_at, unit='s') + pd.DateOffset(hours=1)
    PandL.to_sql(con=conn,name='Profit_Loss',if_exists='replace')
    print()

