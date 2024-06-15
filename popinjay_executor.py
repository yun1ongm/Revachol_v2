import logging
import warnings
warnings.filterwarnings("ignore")
import sys
main_path = "/Users/rivachol/Desktop/Rivachol_v2"
sys.path.append(main_path)
from production.binance_execution.traders import Traders
from production.kline import KlineGenerator
import contek_timbersaw as timbersaw
import time 
import pandas as pd
import pandas_ta as pta
from datetime import datetime
from retry import retry

class ExecPopinjay(Traders):
    """
    Executor 

    """
    executor = "exec_Popinjay"
    equity = 2000
    leverage = 5 

    params = {'sptr_len':90, 'sptr_k': 3.5, 'atr_len': 21, 'thd': 0.8}
    logger = logging.getLogger(executor)

    def __init__(self, market, timeframe) -> None:
        super().__init__(market)
        self._init_from_market()
        self.candle = KlineGenerator(self.symbol, timeframe)
        self.timeframe = timeframe
        self.interval = 15
        self.max_position = self.params['thd'] * self.equity * self.leverage

    def _init_from_market(self) -> None:
        if self.market in ["BTCUSDT", "BTCUSDC"]:
            self.symbol = "BTCUSDC"
            self.digit = 1
            min_sizer = 0.002
        elif self.market in ["ETHUSDT", "ETHUSDC"]:
            self.symbol = "ETHUSDT"
            self.digit = 2
            min_sizer = 0.007
        else:
            self.symbol = "SOLUSDT"
            self.digit = 3
            min_sizer = 0.005
        self.lot = min_sizer

    def _supertrend(self) -> pd.DataFrame:
        kdf = self.candle.kdf
        supertrend = pta.supertrend(
            kdf["high"], kdf["low"], kdf["close"],  self.params['sptr_len'],  self.params['sptr_k']
        )
        supertrend.columns = ["stop_price", "direction", "lbound", "ubound"]

        return supertrend[["stop_price", "direction"]]
    
    @retry(tries=3, delay=1)
    def check_candle_refresh(self) -> bool:
        timeframe_int = {
            '1m': 1,
            '5m': 5,
            '15m': 15,
            '1h': 60
        }.get(self.timeframe, 1)
        if datetime.utcnow() - self.candle.kdf.closetime[-1] < pd.Timedelta(minutes=timeframe_int):
            return False
        else:
            self.candle.update_klines()
            if datetime.utcnow() -  self.candle.kdf.closetime[-1] > pd.Timedelta(minutes=timeframe_int):
                return False
            else:
                self.logger.info(f"New candle data is refreshed.")吧  
                if self.over_boundary():
                    self.restart_program()
                return True

    def calc_levels(self) -> dict:
        kdf = self.candle.kdf
        supertrend = self._supertrend()
        kdf['atr'] = pta.atr(kdf["high"], kdf["low"], kdf["close"], length = self.params['atr_len'], mamode = "EMA")
        kdf = pd.concat([kdf, supertrend], axis=1)

        current_price = kdf["close"][-1]
        trend = kdf["direction"][-1]
        stop_price = kdf["stop_price"][-1]
        self.grid = self.calc_grid()
        self.logger.info(f"trend: {trend}, stop price: {stop_price}, grid: {self.grid}")

        if kdf["direction"][-1] == 1:
            level1 = current_price + 2 * self.grid
            level2 = current_price + 4 * self.grid
            side = "SELL"
        else:
            level1 = current_price - 2 * self.grid
            level2 = current_price - 4 * self.grid
            side = "BUY"
        return {
            "side": side,
            "level1": level1,
            "level2": level2,
        }

    def update_levels(self) -> bool:
        try:
            high = self.latest_kdf.high[-1]
            low = self.latest_kdf.low[-1]
            close = self.latest_kdf.close[-1]
            if high >= self.levels["upper_level1"]:
                self.grid = self.calc_grid()
                if high >= self.levels["upper_level2"]:
                    # if the upper level2 is penetrated, refresh the curr_level based on the current price
                    self.levels["curr_level"] = close
                    self.levels['upper_level1'] = self.levels["curr_level"] + self.grid
                    self.levels['lower_level1'] = self.levels["curr_level"] - 3 * self.grid
                    self.levels['upper_level2'] = self.levels["curr_level"] + 2 * self.grid
                    self.levels['lower_level2'] = self.levels["curr_level"] - 4 * self.grid
                    self.logger.warning(f"upper_level2 is penetrated. Levels are updated.")
                else:
                    self.levels['curr_level'] = self.levels["upper_level1"]
                    self.levels['upper_level1'] = self.levels['curr_level'] + self.grid
                    self.levels['lower_level1'] = self.levels['curr_level'] - 2 * self.grid
                    self.levels['upper_level2'] = self.levels['curr_level'] + 2 * self.grid
                    self.levels['lower_level2'] = self.levels['curr_level'] - 3 * self.grid
                    self.logger.info(f"Levels are updated.")
                return True
                
            elif low <= self.levels["lower_level1"]:
                self.grid = self.calc_grid()
                if low <= self.levels["lower_level2"]:
                    self.levels["curr_level"] = close
                    self.levels['upper_level1'] = self.levels["curr_level"] + 3 * self.grid
                    self.levels['lower_level1'] = self.levels["curr_level"] - self.grid
                    self.levels['upper_level2'] = self.levels["curr_level"] + 4 * self.grid
                    self.levels['lower_level2'] = self.levels["curr_level"] - 2 * self.grid
                    self.logger.warning(f"level2 is penetrated. Levels are updated.")
                else:
                    self.levels['curr_level'] = self.levels["lower_level1"]
                    self.levels['upper_level1'] = self.levels['curr_level'] + 2 * self.grid
                    self.levels['lower_level1'] = self.levels['curr_level'] - self.grid
                    self.levels['upper_level2'] = self.levels['curr_level'] + 3 * self.grid
                    self.levels['lower_level2'] = self.levels['curr_level'] - 2 * self.grid  
                    self.logger.info(f"Levels are updated.")
                return True
            else:
                self.logger.info(f"Levels are not updated.")
                return False  
        except Exception as error:
            self.logger.error(error)
            return False
        
    @retry(tries=3, delay=1)
    def calc_grid(self) -> float:
        kdf = self.latest_kdf   
        atr = pta.atr(kdf["high"], kdf["low"], kdf["close"], length = self.params['atr_len'], mamode = "EMA")
        grid = atr.mean()
        return grid
        
    def over_boundary(self) -> bool:
        """ check if the price is outside the boundary"""
        try:
            current_price = self.latest_kdf.close[-1]
            if current_price > self.highest or current_price < self.lowest:
                self.logger.critical(f"*** The price is over the boundary.restart the bot ***")
                return True
            else:
                return False
        except Exception as error:
            self.logger.error(error)
            return False
        
    def over_threshold(self) -> bool:
        """ check if the position is over the threshold"""
        try:
            unpnl_float, abs_notional = self.fetch_positions()
            if abs_notional > self.max_position:
                self.logger.critical(f"*** notional is over the threshold. restart the bot ***")
                return True
            elif abs(unpnl_float) > 100:
                self.logger.critical(f"*** unrealizedPnl is over the threshold. restart the bot ***")
                return True
            else:
                return False
        except Exception as error:
            self.logger.error(error)
            return False

    def put_orders_to_market(self) -> None:
        """this function is to put orders to the market based on calculated levels."""
        orders_df = pd.DataFrame(self.levels, index=[self.market])
        orders_df["lot"] = self.lot
        orders_df["market"] = self.market
        orders_df["buy1"] = self.levels["lower_level1"]
        orders_df["buy2"] = self.levels["lower_level2"]
        orders_df["sell1"] = self.levels["upper_level1"]
        orders_df["sell2"] = self.levels["upper_level2"]
        response = self.send_batch_order(orders_df)
        self.last_order = pd.DataFrame(response)
    
    def check_if_last_order_filled(self) -> bool:
        """this function is to check if the order is filled."""
        if self.last_order is None:
            return False
        else:
            filled = False
            for _, row in self.last_order.iterrows():
                orderid = row['orderId']
                order_info = self.get_order_info(orderid)
                if not order_info:
                    self.logger.error(f"Order {orderid} is not found.")
                    continue
                elif order_info['status'] == "FILLED":
                    direction = order_info["side"]
                    self.logger.info(f"{direction} order {orderid} is filled.")
                    self._put_harvest_maker_order(order_info)
                    self.last_order = self.last_order[self.last_order.orderId != orderid]
                    filled = True
            if filled:
                self.cancel_rest_orders()
                self.last_order = None
                return filled
            else:
                return filled
        
    def cancel_rest_orders(self) -> None:
        """this function is to cancel the rest of the orders."""
        for _, row in self.last_order.iterrows():
            orderid = row['orderId']
            self.cancel_order_by_id(orderid)

    def _put_harvest_maker_order(self, order_info:dict) -> dict:
        """this function is to put close orders to the market based on filled orders."""
        if order_info["side"] == "BUY":
            try:
                target_price = float(order_info["price"]) + self.grid
                amount = float(order_info["executedQty"]) 
                target_price = round(target_price, self.digit)
                response = self.client.new_order(
                    symbol=self.market,
                    side="SELL",
                    type="LIMIT",
                    quantity=amount,
                    timeInForce="GTC",
                    price = target_price,
                )
                self.logger.info(f"Target sell price:{target_price} is sent to the market.")
                return response  
            except Exception as error:
                self.logger.error(error)

        else:
            try:
                target_price = float(order_info["price"]) - self.grid
                amount = float(order_info["executedQty"]) 
                target_price = round(target_price, self.digit)
                response = self.client.new_order(
                    symbol=self.market,
                    side="BUY",
                    type="LIMIT",
                    quantity=amount,
                    timeInForce="GTC",
                    price = target_price,
                )
                self.logger.info(f"Target buy price:{target_price} is sent to the market.")
                return response
            except Exception as error:
                self.logger.error(error)

    def recycle_level_order(self) -> None:
        """this function is to recycle orders from the market based on the response."""
        if self.over_threshold():
            self.restart_program()
        elif self.last_order is None:
            if self.update_levels():
                self.put_orders_to_market()

    def restart_program(self) -> None:
        self.cancel_open_orders()
        self.close_position()
        time.sleep(4*60*60)

        self.levels = self.calc_levels()
        self.latest_kdf = self.get_newest_candle(50)
        self.put_orders_to_market()
        
    def run(self) -> None: 
        while True:
            self.check_if_last_order_filled()
            if self.check_candle_refresh():
                self.recycle_level_order()
                self.logger.info(f"Task completed.\n------------------")
                time.sleep(self.interval)
            else:
                time.sleep(self.interval/3) 
 
if __name__ == "__main__":
    timbersaw.setup()
    executor = ExecFishnet(market = "BTCUSDC", timeframe = "5m")
    executor.run()
