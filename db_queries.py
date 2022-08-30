import sqlite3 as sql
conn = sql.connect('bybit_sma')
cur = conn.cursor()

def sql_out_replace(input,is_string:bool):
    replace_is_string = ('(',')',',')
    replace_is_int = ('(',')',',')
    if is_string:
        for rep in replace_is_string:
            input = str(input).replace(rep,'')
        return str(input)
    else:
        for rep in replace_is_int:
            input = str(input).replace(rep,'')
        return float(input)
        

PNL = """select case when sum(closed_pnl) is null then 0 else sum(closed_pnl) end as PNL from Profit_Loss where created_at >= (select min(market_date) from Logs);"""
cur.execute(PNL)
Profit_Loss = sql_out_replace(cur.fetchone(),False)
print('#### Overall Stats ####')
print(f'Total Profit and Loss:{Profit_Loss}')

TotalTrades = """select count(*) as TotalTrades from Profit_Loss where created_at >= (select min(market_date) from Logs);"""
cur.execute(TotalTrades)
Total_Trades = sql_out_replace(cur.fetchone(),False)
print(f'Total Trades:{Total_Trades}')

WinningTrades = """select count(*) as WinningTrades from Profit_Loss where created_at >= (select min(market_date) from Logs) and closed_pnl > 0;"""
cur.execute(WinningTrades)
Winning_Trades = sql_out_replace(cur.fetchone(),False)
print(f'Winning Trades:{Winning_Trades}')

LosingTrades = """select count(*) as LosingTrades from Profit_Loss where created_at >= (select min(market_date) from Logs) and closed_pnl < 0;"""
cur.execute(LosingTrades)
Losing_Trades = sql_out_replace(cur.fetchone(),False)
print(f'Losing Trades:{Losing_Trades}')

Current_Order = """
            with last_order as (select order_id from Orders order by created_time desc limit 1)
            , symbol as (select symbol from Orders where order_id = (select order_id from last_order) limit 1)
            , buy_price as (select last_exec_price from Orders where order_id = (select order_id from last_order) limit 1)
            , side as (select side from Orders where order_id = (select order_id from last_order) limit 1)
            , tp as (select current_take_profit from take_profit_stop_loss where order_id=(select order_id from last_order))
            , tp_perc as (select (current_take_profit/bought_price)/bought_price*100 as tp_perc from take_profit_stop_loss)
            , sl as (select current_stop_loss from take_profit_stop_loss where order_id=(select order_id from last_order))
            , current_price as (select close from Logs where symbol = (select symbol from symbol) order by market_date desc limit 1)
            select --(select order_id from last_order) as order_id,
            (select symbol from symbol) as symbol,
            (select side from side) as side,
            (select last_exec_price from buy_price) as buy_price,
            (select current_take_profit from tp) as take_profit,
            (select current_stop_loss from sl) as stop_loss,
            (select close from current_price) as close,
            round((select last_exec_price from buy_price) - (select close from current_price),3) as PL,
            round((round((select last_exec_price from buy_price) - (select close from current_price),3) / (select last_exec_price from buy_price))*100,3) as PL_Perc
"""
cur.execute(Current_Order)
#Current_Order_Stats = sql_out_replace(cur.fetchone(),True)
Current_Order_Stats = cur.fetchone()
print()
print('#### Open Trade ####')
print(f'Symbol:{Current_Order_Stats[0]}')
print(f'side:{Current_Order_Stats[1]}')
print(f'buy_price:{Current_Order_Stats[2]}')
print(f'take_profit:{Current_Order_Stats[3]}')
print(f'stop_loss:{Current_Order_Stats[4]}')
print(f'close:{Current_Order_Stats[5]}')
print(f'PL:{Current_Order_Stats[6]}')
print(f'PL%:{Current_Order_Stats[7]}')


#OpenTrades = """select count(*) as OpenTrades from Orders o left join Profit_Loss p on o.order_id = p.order_id where p.order_id is null;"""
#cur.execute(OpenTrades)
#Open_Trades = sql_out_replace(cur.fetchone(),False)
#print(f'Total Profit and Loss:{Open_Trades}')

last_log = """select symbol,close,fast_sma,slow_sma,cross,last_cross,buy_sell,buy_price,sell_price,kline,dline,macd,previous_kline from Logs order by market_date desc limit 1"""
cur.execute(last_log)
last_log_result = cur.fetchone()
print()
print('#### Last Log ####')
print(f'Symbol:{last_log_result[1]}')
print(f'close:{last_log_result[2]}')
#print(f'fast_sma:{last_log_result[3]}')
#print(f'slow_sma:{last_log_result[4]}')
#print(f'last_cross:{last_log_result[5]}')
print(f'buy_sell:{last_log_result[6]}')
print(f'buy_price:{last_log_result[7]}')
print(f'sell_price:{last_log_result[8]}')
print(f'kline:{last_log_result[9]}')
#print(f'dline:{last_log_result[10]}')
print(f'macd:{last_log_result[11]}')
print(f'previous_kline:{last_log_result[12]}')
