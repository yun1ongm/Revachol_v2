# Rivachol_v2
Crypto Binance trading bot

Main Workflow:

(market) --kdata--  --update--


(alpha_1) -- indicator_set1  -- order_signals-- (model)
(alpha_2) -- indicator_set2  -- order_signals-- (model)
...
(alpha_n) -- indicator_setn  -- order_signals-- (model)


(model)  -- (execution)

(exucution) -- (monitor)  'to be expected'


every model with fixed wlr & atr_multiplier and unique management
