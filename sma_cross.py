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
cur.execute('CREATE TABLE IF NOT EXISTS Logs (id integer PRIMARY KEY AUTOINCREMENT, symbol text, close decimal, fast_sma decimal, slow_sma decimal, cross text, last_cross text, buy_sell text, buy_price decimal, sell_price decimal, kline decimal, dline decimal, macd decimal, previous_kline decimal, previous_previous_kline decimal, market_date timestamp DEFAULT current_timestamp)')
cur.execute('INSERT OR REPLACE INTO Logs (id,symbol,close,fast_sma,slow_sma,cross,last_cross,buy_sell,buy_price,sell_price,kline,dline,macd,previous_kline,previous_previous_kline) VALUES (1,NULL,0,0,0,"wait","na","na",0,0,0,0,0,0,0)')
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
    df['%K'] = ta.momentum.stoch(df.high,df.low,df.close,window=14,smooth_window=3)
    df['%D'] = df['%K'].rolling(3).mean()
    df['rsi'] = ta.momentum.rsi(df.close,window=14)
    df['macd'] = ta.trend.macd_diff(df.close)
    df.dropna(inplace=True)
    return df

def get_bybit_bars(trading_symbol, interval, startTime, apply_technicals):
    startTime = str(int(startTime.timestamp()))
    response = session.query_kline(symbol=trading_symbol,interval=interval,from_time=startTime)
    df = pd.DataFrame(response['result'])
    df.start_at = pd.to_datetime(df.start_at, unit='s') + pd.DateOffset(hours=1)
    df.open_time = pd.to_datetime(df.open_time, unit='s') + pd.DateOffset(hours=1)
    if apply_technicals:
        applytechnicals(df)
    return df

def insert_log(trading_symbol,close_price,fast_sma,slow_sma,cross,last_cross,buy_sell,buy_price,sell_price,kline,dline,macd,previous_kline,previous_previous_kline):
    if str(buy_sell).upper() not in ('LONG','SHORT'):
        buy_sell == None
    insert_query = f'INSERT INTO Logs (symbol,close,fast_sma,slow_sma,cross,last_cross,buy_sell,buy_price,sell_price,kline,dline,macd,previous_kline,previous_previous_kline) VALUES ("{trading_symbol}",{close_price},{fast_sma},{slow_sma},"{cross}","{last_cross}","{buy_sell}",{buy_price},{sell_price},{kline},{dline},{macd},{previous_kline},{previous_previous_kline})'
    cur.execute(insert_query)
    conn.commit()

def constant_log(time_stamp,field,value):
    log_dict={'time_stamp':time_stamp,'field':field,'value':value}
    df = pd.DataFrame([log_dict])
    df.to_sql(name='constant_log',con=conn,if_exists='append')

def read_last_log():
    query = 'SELECT id,symbol,close,fast_sma,slow_sma,cross,last_cross,market_date,buy_sell,buy_price,sell_price,kline,dline,macd,previous_kline FROM logs ORDER BY id DESC LIMIT 1 '
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
    kline = output[11]
    dline = output[12]
    macd = output[13]
    previous_kline = output[14]
    return id,symbol,close,fast_sma,slow_sma,cross,last_cross,market_date,buy_sell,buy_price,sell_price,kline,dline,macd,previous_kline

def print_Last_log():
    log = read_last_log()
    print(f'{now_today}:id:{log[0]}')
    print(f'{now_today}:symbol:{log[1]}')
    print(f'{now_today}:close:{log[2]}')
    print(f'{now_today}:fast_sma:{log[3]}')
    print(f'{now_today}:slow_sma:{log[4]}')
    print(f'{now_today}:cross:{log[5]}')
    print(f'{now_today}:last_cross:{log[6]}')
    print(f'{now_today}:previous_kline:{log[14]}')
    print(f'{now_today}:kline:{log[11]}')
    print(f'{now_today}:macd:{log[13]}')
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
        stock_trade = False
        trend = str(get_trend(trading_symbol)).replace("'","")
        print(f'{now_today}:last_cross:{last_cross}')
        print(f'{now_today}:cross:{cross}')
        print(f'{now_today}:trend:{trend}')
        
        if last_cross == 'down' and cross == 'up' and trend == 'up':
            print(f'{now_today}:LONG')
            buy_sell = 'LONG'
            buy_price = close_price
            take_profit_var = round(buy_price+(buy_price * 0.01),3) #1%
            stop_loss_var = round(buy_price-(buy_price * 0.015),3) #-1.5%
            quantity = get_quantity(close_price)
            place_order(trading_symbol,"Buy",quantity,buy_price,take_profit_var,stop_loss_var,trailing_stop_take_profit,stock_trade)

        if last_cross == 'up' and cross == 'down' and trend == 'down':
            print(f'{now_today}:SHORT')
            buy_sell == 'SHORT'
            buy_price = close_price
            take_profit_var = round(buy_price-(buy_price * 0.01),3) #1%
            stop_loss_var = round(buy_price+(buy_price * 0.015),3) #-1.5%
            quantity = get_quantity(close_price)
            place_order(trading_symbol,"Sell",quantity,buy_price,take_profit_var,stop_loss_var,trailing_stop_take_profit,stock_trade)

        insert_log(trading_symbol,close_price,fast_sma,slow_sma,cross,last_cross,buy_sell,buy_price,sell_price,0,0,0,0,0)

def amend_take_profit_stop_loss(order_id,bought_price,take_profit,stop_loss):
    print(f'{now_today}:Amending Stop')
    print(f'{now_today}:order_id:{order_id}')
    print(f'{now_today}:bought_price:{bought_price}')
    print(f'{now_today}:take_profit:{take_profit}')
    print(f'{now_today}:stop_loss:{stop_loss}')
    order_id = str(order_id).replace("'","")
    cur.execute(f'select count(*) from take_profit_stop_loss where order_id = "{order_id}"')
    row_exists = int(str(cur.fetchone()).replace('(','').replace(')','').replace(',',''))
    if row_exists == 1:
        cur.execute(f'update take_profit_stop_loss set current_take_profit={take_profit}, current_stop_loss={stop_loss} where order_id = "{order_id}"')
    else:
        cur.execute(f'insert into take_profit_stop_loss (order_id, bought_price, current_take_profit, current_stop_loss) values ("{order_id}",{bought_price},{take_profit},{stop_loss})')
    conn.commit()

def place_order(trading_symbol,order_side,quantity,buy_price,take_profit_var,stop_loss_var,trailing_stop_take_profit,stock_trade):
    if not stock_trade:
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
    else:
        order_df = pd.DataFrame(session.place_active_order(symbol=trading_symbol,
                                        side=f"{order_side}",
                                        order_type="Market",
                                        qty=quantity,
                                        price=buy_price,
                                        time_in_force="ImmediateOrCancel",
                                        reduce_only=False,
                                        close_on_trigger=False)['result'],index=[0])
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
    stock_trade = False

    try:
        cur.execute('select buy_sell,fast_sma, slow_sma from last_order order by timestamp DESC limit 1;')
        last_results = cur.fetchone()
        last_buy_sell = last_results[0]
        last_fast_sma = last_results[1]
        last_slow_sma = last_results[2]
    except Exception as E:
        print(f'{now_today}:last_results exception:')
        print(E)
        last_buy_sell = ''
        last_fast_sma = 0
        last_slow_sma = 0

    try:
        cur.execute('select status from status order by timestamp DESC limit 1;')
        current_status = cur.fetchone()
        ready_status = current_status[0]
    except Exception as E:
        ready_status = ''
        print(f'{now_today}:ready_status exception change:{ready_status}')
        print(E)

    print(f'{now_today}:ready_status:{ready_status}')

    if float(last_fast_sma) > float(last_slow_sma) and last_buy_sell == 'LONG' and ready_status != 'ready':
        print(f'Stage 1 ready Status Change - LONG')
        if float(close_price) > float(fast_sma) or float(slow_sma) > float(fast_sma):
            print(f'{now_today}:ready_status change:ready')
            waiting_dict = {'status':'ready','timestamp':now_today}
            status = pd.DataFrame([waiting_dict])
            status.to_sql(name='status',con=conn,if_exists='replace')

    if float(last_fast_sma) < float(last_slow_sma) and last_buy_sell == 'SHORT' and ready_status != 'ready':
        print(f'Stage 1 ready Status Change - SHORT')
        if float(close_price) < float(fast_sma) or float(slow_sma) < float(fast_sma):
            print(f'{now_today}:ready_status change:ready')
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

            place_order(trading_symbol,"Buy",quantity,buy_price,take_profit_var,stop_loss_var,trailing_stop_take_profit,stock_trade)

            order_dict = {'buy_sell':buy_sell,'buy_price':buy_price,'fast_sma':fast_sma,'slow_sma':slow_sma,'close_price':close_price,'timestamp':now_today}
            last_order_df = pd.DataFrame([order_dict])
            last_order_df.to_sql(name='last_order',con=conn,if_exists='replace')

            print(f'{now_today}:ready_status change:waiting')
            waiting_dict = {'status':'waiting','timestamp':now_today}
            status = pd.DataFrame([waiting_dict])
            status.to_sql(name='status',con=conn,if_exists='replace')

        if float(slow_sma) > float(fast_sma) and float(close_price) > float(slow_sma):
            print(f'{now_today}:SHORT')
            buy_sell = 'SHORT'
            buy_price = close_price
            take_profit_var = round(buy_price-(buy_price * 0.015),3) #-1.5%
            stop_loss_var = round(buy_price+(buy_price * 0.02),3) #+2%
            quantity = get_quantity(close_price)
            place_order(trading_symbol,"Sell",quantity,buy_price,take_profit_var,stop_loss_var,trailing_stop_take_profit,stock_trade)

            order_dict = {'buy_sell':buy_sell,'buy_price':buy_price,'fast_sma':fast_sma,'slow_sma':slow_sma,'close_price':close_price,'timestamp':now_today}
            last_order_df = pd.DataFrame([order_dict])
            last_order_df.to_sql(name='last_order',con=conn,if_exists='replace')

            print(f'{now_today}:ready_status change:waiting')
            waiting_dict = {'status':'waiting','timestamp':now_today}
            status = pd.DataFrame([waiting_dict])
            status.to_sql(name='status',con=conn,if_exists='replace')

    insert_log(trading_symbol,close_price,fast_sma,slow_sma,cross,last_cross,buy_sell,buy_price,sell_price,0,0,0,0,0)

def stock_macd_entry_strategy(trading_symbol,close_price,macd,kline,dline,previous_kline,previous_previous_kline):
    # Buy when: MACD > 0 and kline crosses UP 20
    # Sell when: MACD < 0 and kline crosses DOWN 80
    enter = False
    stock_trade = True
    trailing_stop_take_profit = False
    buy_sell = 'na'
    buy_price = 0
    #if macd > 0 and kline > 20 and (previous_kline < 20 or previous_previous_kline < 20):
    if macd > 0 and kline > 20 and previous_kline < 20:
        enter = True
        buy_sell = "Buy"
        print(f'{now_today}:MACD,kline,dline ON for LONG')
    #if macd < 0 and kline < 80 and (previous_kline > 80 or previous_previous_kline > 80):
    if macd < 0 and kline < 80 and previous_kline > 80:
        enter = True
        buy_sell = "Sell"
        print(f'{now_today}:MACD,kline,dline ON for SHORT')
    if enter:
        print(f'{now_today}:LONG')
        buy_price = close_price
        quantity = get_quantity(close_price)
        place_order(trading_symbol,buy_sell,quantity,buy_price,0,0,trailing_stop_take_profit,stock_trade)
    
    if buy_sell == "Sell":
        buy_sell_log = 'SHORT'
    if buy_sell == "Buy":
        buy_sell_log = 'LONG'
    if buy_sell == 'na':
        buy_sell_log = None
    insert_log(trading_symbol,close_price,0,0,'na',get_last_cross(),buy_sell_log,buy_price,0,kline,dline,macd,previous_kline,previous_previous_kline)
    


def stock_macd_exit_strategy(trading_symbol,close_price,macd,kline,dline,previous_kline,previous_previous_kline):
    order_id = get_last_order(trading_symbol)[0]
    print(f'{now_timestamp}:order_id:{order_id}')
    last_order_side = get_last_order(trading_symbol)[2]
    print(f'{now_timestamp}:last_order_side:{last_order_side}')
    bought_price = get_last_order(trading_symbol)[1]
    print(f'{now_timestamp}:bought_price:{bought_price}')
    if last_order_side == "'Buy'":
        if macd < 0 or kline > 80 or kline <= 20:
            close_position(trading_symbol,order_id)
    if last_order_side == "'Sell'":
        if macd > 0 and kline < 20 or kline >= 80:
            close_position(trading_symbol,order_id)
    insert_log(trading_symbol,close_price,0,0,'na',get_last_cross(),last_order_side,bought_price,0,kline,dline,macd,previous_kline,previous_previous_kline)

def trailing_stop_loss(trading_symbol,close_price,fast_sma,slow_sma):
    print(f'{now_today}:Open Position Trailing Stop')
    order_id = get_last_order(trading_symbol)[0]
    print(f'{now_today}:order_id:{order_id}')
    print(f'{now_today}:close_price:{close_price}')

    bought_price = get_last_order(trading_symbol)[1]
    print(f'{now_today}:bought_price:{bought_price}')

    last_order_side = get_last_order(trading_symbol)[2]
    print(f'{now_today}:last_order_side:{last_order_side}')

    current_tp = get_current_tp_sl(order_id)[0]
    print(f'{now_today}:current_tp:{current_tp}')

    current_sl = get_current_tp_sl(order_id)[1]
    print(f'{now_today}:current_sl:{current_sl}')
    
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

    insert_log(trading_symbol,close_price,fast_sma,slow_sma,'na',get_last_cross(),last_order_side,bought_price,0,0,0,0,0,0)
    
    return bought_price, last_order_side

def get_last_order(trading_symbol):
    cur.execute(f'select order_id from Orders where symbol="{trading_symbol}" order by updated_time desc')
    order_id = str(cur.fetchone()).replace('(','').replace(')','').replace(',','')
    cur.execute(f'select last_exec_price from Orders where order_id={order_id} order by updated_time desc')
    price = float(str(cur.fetchone()).replace('(','').replace(')','').replace(',',''))
    cur.execute(f'select side from Orders where order_id={order_id} order by updated_time desc')
    side = str(cur.fetchone()).replace('(','').replace(')','').replace(',','')
    return order_id, price, side

def get_current_tp_sl(order_id):
    print(f'{now_today}:get_current_tp_sl:{order_id}')
    cur.execute(f'select current_take_profit from take_profit_stop_loss where order_id = {order_id}')
    tp_str = str(cur.fetchone()).replace('(','').replace(')','').replace(',','')
    if tp_str != None and tp_str != 'None':
        tp = float(tp_str)
    else:
        print(f'{now_today}:No current_take_profit in take_profit_stop_loss')
        tp = get_last_order(trading_symbol)[1]
    cur.execute(f'select current_stop_loss from take_profit_stop_loss where order_id = {order_id}')
    sl_str = str(cur.fetchone()).replace('(','').replace(')','').replace(',','')
    if sl_str != None and sl_str != 'None':
        sl = float(sl_str)
    else:
        print(f'{now_today}:No current_take_profit in take_profit_stop_loss')
        if get_last_order(trading_symbol)[2] == "'Buy'": 
            sl = round(get_last_order(trading_symbol)[1]-(get_last_order(trading_symbol)[1] * 0.02),3)
        if get_last_order(trading_symbol)[2] == "'Sell'":
            sl = round(get_last_order(trading_symbol)[1]+(get_last_order(trading_symbol)[1] * 0.02),3)
        amend_take_profit_stop_loss(get_last_order(trading_symbol)[0],get_last_order(trading_symbol)[1],tp,sl)
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

def get_trend(trading_symbol):
    trend_start_date = now_today + dt.timedelta(days=-120)
    trend_start_date = dt.datetime(trend_start_date.year, trend_start_date.month, trend_start_date.day)
    interval='D'
    trend_candles = get_bybit_bars(trading_symbol,interval,trend_start_date,False)
    trend_candles.to_sql(con=conn,name='trend',if_exists='replace')
    trend_query = """with first_row as (select "index",id,close from trend)
                    , second_row as (select "index",id,close from trend)
                    , trend_cte as (select case when fr.close < sr.close then 'down' when fr.close > sr.close then 'up' else 'undetermined' end as trend from first_row fr inner join second_row sr on fr."index" = sr."index"+1)
                    , overall_trend as (select trend, count(trend) trend_count from trend_cte t group by trend)
                    select trend from overall_trend order by trend_count DESC limit 1;"""
    cur.execute(trend_query)
    trend = str(cur.fetchone()).replace('(','').replace(')','').replace(',','')
    return trend


if __name__ == '__main__':
    trading_symbol = "SOLUSDT"
    interval='60'
    trailing_stop_take_profit = True
    candles = get_bybit_bars(trading_symbol,interval,today,True)
    candles.to_sql(con=conn,name='Candles',if_exists='replace')
    most_recent = candles.iloc[-1]
    close_price = most_recent.close
    fast_sma = most_recent.FastSMA
    slow_sma = most_recent.SlowSMA
    kline = most_recent['%K']
    dline = most_recent['%D']
    rsi = most_recent['rsi']
    macd = most_recent['macd']
    orders = pd.DataFrame(session.get_active_order(symbol=trading_symbol)['result']['data'])
    orders.to_sql(con=conn,name='Orders',if_exists='replace')
    user_trade_records = pd.DataFrame(session.user_trade_records(symbol=trading_symbol)['result']['data'])
    user_trade_records.trade_time_ms = pd.to_datetime(user_trade_records.trade_time_ms, unit='ms') + pd.DateOffset(hours=1)
    user_trade_records.to_sql(con=conn,name='User_Trade_Records',if_exists='replace')

    ## Turning off SMA Cross Strategy
    open_position = check_open_position()
    if not open_position > 0.0: #If a position is NOT open, e.g. not open else wait for tp and sl
        sma_cross_strategy(fast_sma,slow_sma,trading_symbol,close_price,trailing_stop_take_profit)

    #open_position = check_open_position()
    #if not open_position > 0.0: #If a position is NOT open, e.g. not open else wait for tp and sl
    #    sma_bounce_strategy(fast_sma,slow_sma,trading_symbol,close_price,trailing_stop_take_profit)
    if open_position > 0.0 and trailing_stop_take_profit:
        trailing_sl = trailing_stop_loss(trading_symbol,close_price,fast_sma,slow_sma)
    #previous_close = candles.iloc[-2]
    #previous_kline = previous_close['%K']
    #previous_previous_close = candles.iloc[-3]
    #previous_previous_kline = previous_previous_close['%K']
    #print(f'open position:{open_position}')
    #if not open_position > 0.0:
    #    stock_macd_entry_strategy(trading_symbol,close_price,macd,kline,dline,previous_kline,previous_previous_kline)
#
    #if open_position > 0.0:
    #    stock_macd_exit_strategy(trading_symbol,close_price,macd,kline,dline,previous_kline,previous_previous_kline)

    cur.close()
    conn.close()
    conn = sql.connect('bybit_sma')
    cur = conn.cursor()
    PandL =  pd.DataFrame(session.closed_profit_and_loss(symbol=trading_symbol)['result']['data'])
    PandL.created_at = pd.to_datetime(PandL.created_at, unit='s') + pd.DateOffset(hours=1)
    PandL.to_sql(con=conn,name='Profit_Loss',if_exists='replace')
