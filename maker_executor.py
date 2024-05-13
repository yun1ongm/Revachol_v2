import logging
import warnings
warnings.filterwarnings("ignore")
import sys
main_path = "/Users/rivachol/Desktop/Rivachol_v2"
sys.path.append(main_path)
from production.binance_execution.traders import Traders
import contek_timbersaw as timbersaw
import time 
import pandas as pd
import pandas_ta as pta
from datetime import datetime
from retry import retry

class ExecFishnet(Traders):
    """
    Executor 

    """
    executor = "exec_Fishnet"
    equity = 2000
    leverage = 5 

    params = {'atr_len': 9, 'thd': 0.8}
    logger = logging.getLogger(executor)

    def __init__(self, market, timeframe) -> None:
        super().__init__(market)
        self._init_from_market()
        
        self.timeframe = timeframe
        self.interval = 5
        self.max_position = self.params['thd'] * self.equity * self.leverage
        self.levels = self.calc_levels()
        self.latest_kdf = self.get_newest_candle()
        self.put_orders_to_market()
        self.restart = False

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
       
    @retry(tries=2, delay=3)
    def get_candle(self) -> pd.DataFrame:  
        try:
            klines = self.client.continuous_klines(self.market, "PERPETUAL", self.timeframe, limit=240)
            klines.pop() # remove unfinished candle
            kdf = pd.DataFrame(
                klines,
                columns=self.columns,
            )
            kdf = self._convert_kdf_datatype(kdf)
            return kdf
        except Exception as error:
            self.logger.error(error)

    @retry(tries=2, delay=3)
    def get_newest_candle(self) -> pd.DataFrame | None:
        try:
            latest_ohlcv = self.client.continuous_klines(self.market, "PERPETUAL", self.timeframe, limit=2)
            unfin_ohlcv = latest_ohlcv.pop()
            latest_kdf = pd.DataFrame(
                latest_ohlcv,
                columns=self.columns,
            )
            unfin_kdf = pd.DataFrame(
                [unfin_ohlcv],
                columns=self.columns,
            )
            latest_kdf = self._convert_kdf_datatype(latest_kdf)
            unfin_kdf = self._convert_kdf_datatype(unfin_kdf)

            self.logger.info(f"Newest price:{unfin_kdf.close[-1]} time:{unfin_kdf.closetime[-1]}")
            return latest_kdf
        except Exception as error:
            self.logger.error(error)
    
    def _convert_kdf_datatype(self, kdf) -> pd.DataFrame:
        kdf.opentime = [
            datetime.utcfromtimestamp(int(x) / 1000.0) for x in kdf.opentime
        ]
        kdf.open = kdf.open.astype("float")
        kdf.high = kdf.high.astype("float")
        kdf.low = kdf.low.astype("float")
        kdf.close = kdf.close.astype("float")
        kdf.volume = kdf.volume.astype("float")
        kdf.closetime = [
            datetime.utcfromtimestamp(int(x) / 1000.0) for x in kdf.closetime
        ]
        kdf.volume_U = kdf.volume_U.astype("float")
        kdf.num_trade = kdf.num_trade.astype("int")
        kdf.taker_buy = kdf.taker_buy.astype("float")
        kdf.taker_buy_volume_U = kdf.taker_buy_volume_U.astype("float")
        kdf.ignore = kdf.ignore.astype("float")
        kdf.set_index("opentime", inplace=True)

        return kdf
    
    @retry(tries=3, delay=3)
    def check_candle_refresh(self) -> bool:
        timeframe_int = {
            '1m': 1,
            '5m': 5,
            '15m': 15,
            '1h': 60
        }.get(self.timeframe, 1)
        if datetime.utcnow() - self.latest_kdf.closetime[-1] < pd.Timedelta(minutes=timeframe_int):
            return False
        else:
            self.latest_kdf = self.get_newest_candle()
            if len(self.latest_kdf) == 0:
                self.restart = True
                return False
            elif datetime.utcnow() - self.latest_kdf.closetime[-1] > pd.Timedelta(minutes=timeframe_int):
                return False
            else:
                self.logger.info(f"New candle data is refreshed.")
                return True

    def calc_levels(self) -> dict:
        kdf = self.get_candle()
        atr = pta.atr(kdf["high"], kdf["low"], kdf["close"], length = self.params['atr_len'], mamode = "EMA")
        highest = kdf["high"].max()
        lowest = kdf["low"].min()
        self.grid = 2 * atr.mean()
        mid_pivot = (highest + lowest) / 2
        current_price = kdf["close"][-1]
        self.logger.info(f"highst: {highest}, lowest: {lowest}, mid_pivot:{mid_pivot}, grid: {self.grid}")
        if current_price > mid_pivot:
            self.curr_level = mid_pivot + ((current_price - mid_pivot) // self.grid + 1) * self.grid
            upper_level1 = self.curr_level + self.grid
            lower_level1 = self.curr_level - self.grid
            upper_level2 = upper_level1 + self.grid
            lower_level2 = lower_level1 - self.grid
        
        else:
            self.curr_level = mid_pivot - ((mid_pivot - current_price) // self.grid + 1) * self.grid
            upper_level1 = self.curr_level + self.grid
            lower_level1 = self.curr_level - self.grid
            upper_level2 = upper_level1 + self.grid
            lower_level2 = lower_level1 - self.grid
        return {
            "curr_level": self.curr_level,
            "upper_level1": upper_level1,
            "lower_level1": lower_level1,
            "upper_level2": upper_level2,
            "lower_level2": lower_level2
        }

    def update_levels(self) -> bool:
        try:
            high = self.latest_kdf.high[-1]
            low = self.latest_kdf.low[-1]
            close = self.latest_kdf.close[-1]
            if high >= self.levels["upper_level1"]:
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
        
    def over_threshold(self) -> bool:
        """ check if the position is over the threshold"""
        try:
            unpnl_float, abs_notional = self.fetch_positions()
            if unpnl_float < -100 or abs_notional > self.max_position:
                self.logger.critical(f"*** unrealizedPnl or notional is over the threshold. Close the position ***")
                return True
            elif unpnl_float > 100 or abs_notional > self.max_position:
                self.logger.critical(f"*** unrealizedPnl or notional is over the threshold. Close the position ***")
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
        filled_orders = []
        for _, row in self.last_order.iterrows():
            orderid = row['orderId']
            order_info = self.get_order_info(orderid)
            if order_info['status'] == "FILLED":
                self._put_harvest_maker_order(order_info)
                self.last_order = self.last_order[self.last_order.orderId != orderid]
                filled_orders.append(order_info)

        if len(filled_orders) == 0:
            return False
        else:
            self.cancel_rest_orders()
            return True

    
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

    def recycle_orders_from_market(self) -> None:
        """this function is to recycle orders from the market based on the response."""
        self.check_if_last_order_filled()
        level_flag = self.update_levels()
        if self.over_threshold():
            self.cancel_open_orders()
            self.close_position()
        elif level_flag:
            self.put_orders_to_market()
        
    def run(self) -> None: 
        while True:
            if self.check_candle_refresh():
                self.recycle_orders_from_market()
                self.logger.info(f"Task completed.\n------------------")
            else:
                if self.restart:
                    self.logger.critical(f"Restart the program.\n------------------")
                    self.restart = False  # Reset restart flag
                    time.sleep(self.interval) 
                    self.latest_kdf = self.get_newest_candle()
                    continue  # Restart the loop
            time.sleep(self.interval) 

if __name__ == "__main__":
    timbersaw.setup()
    executor = ExecFishnet(market = "BTCUSDC", timeframe = "1m")
    executor.run()
