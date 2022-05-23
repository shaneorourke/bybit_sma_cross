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
        return int(input)
        

PNL = """select case when sum(closed_pnl) is null then 0 else sum(closed_pnl) end as PNL from Profit_Loss where created_at >= (select min(market_date) from Logs);"""
cur.execute(PNL)
Profit_Loss = sql_out_replace(cur.fetchone(),False)
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

#OpenTrades = """select count(*) as OpenTrades from Orders o left join Profit_Loss p on o.order_id = p.order_id where p.order_id is null;"""
#cur.execute(OpenTrades)
#Open_Trades = sql_out_replace(cur.fetchone(),False)
#print(f'Total Profit and Loss:{Open_Trades}')