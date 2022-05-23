# bybit_sma_cross

An automated strategy executor using the ByBit API (PyBit)

This simple strategy is SMA cross on 15 minute time frame for SOLUSDT. When the Fast SMA (7) cross over or under the Slow SMA (25) then place an order.
The order has a Take Profit of 1% and a Stop Loss of 1.5%. The order will close when either of these prices are met.
While an order is active, the strategy is paused (to prevent repeat orders)

A crontab is to be created to execute this script every minute

To be added:
1. Database Queries
2. Update Logs with order closure details (currently the TP and the SL aren't recorded, to monitor performance, this will be needed)
3. Ability to switch coin pairs (will currently cause issues with rounding of qty, tp and sl)
4. Option to turn on trailing stop loss (potentially higher gains than 1%) 
