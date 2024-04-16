import logging
import warnings
warnings.filterwarnings("ignore")
import time
import sys
main_path = "/Users/rivachol/Desktop/Rivachol_v2"
sys.path.append(main_path)
from production.binance_execution.traders import Traders
import contek_timbersaw as timbersaw
import pandas as pd
import pandas_ta as pta
from datetime import datetime, timedelta
from retry import retry

class ExecPopinjay(Traders):
    """
    Executor 

    """
    executor = "exec_Popinjay"
    equity = 2000
    leverage = 5 
    columns = [
            "opentime",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "closetime",
            "volume_U",
            "num_trade",
            "taker_buy",
            "taker_buy_volume_U",
            "ignore",
            ]
    params = {'atr_len': 9, 'thd': 0.07}
    logger = logging.getLogger(executor)

    def __init__(self, symbol, timeframe) -> None:
        super().__init__(symbol)
        self.timeframe = timeframe
        self.interval = 3
        if self.symbol == "BTCUSDC":
            self.digit = 1
            min_sizer = 0.002
        if self.symbol == "ETHUSDC":
            self.digit = 2
            min_sizer = 0.007
        else:
            self.digit = 3
            min_sizer = 0.005

        self.lot = min_sizer
        self.threshold = self.params['thd'] * self.equity
        self.kdf = self.get_candle()

    def get_candle(self) -> pd.DataFrame:       
        try:
            klines = self.client.continuous_klines(self.symbol, "PERPETUAL", self.timeframe, limit=240)
            klines.pop() # remove unfinished candle
            kdf = pd.DataFrame(
                klines,
                columns=self.columns,
            )
            kdf = self._convert_kdf_datatype(kdf)
            self.logger.info(f"Initial candle data: {kdf.closetime[-1]}")
            return kdf
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

    @retry(tries=1, delay=1)
    def update_klines(self) -> bool:
        try:
            latest_ohlcv = self.client.continuous_klines(
                self.symbol, "PERPETUAL", self.timeframe, limit=2
            )
            unfin_candle = latest_ohlcv.pop()
            latest_kdf = pd.DataFrame(
                latest_ohlcv,
                columns=self.columns,
            )
            latest_kdf = self._convert_kdf_datatype(latest_kdf)

            if latest_kdf.index[-1] == self.kdf.index[-1]:
                self.logger.info(f"Candle price: {float(unfin_candle[4])}.")
                return False
            else:
                self.kdf = pd.concat([self.kdf, latest_kdf])
                if len(self.kdf) > 60*12:
                    self.kdf = self.get_candle()
                    self.logger.warning(f"Market bot refresh candle data")
                self.logger.info(
                    f"Candle close time: {self.kdf.closetime[-1]} Updated Close: {self.kdf.close[-1]} Volume(U): {round(float(self.kdf.volume_U[-1])/1000000,2)}mil"
                )
                return True
        except Exception as error:
            self.logger.error(error)
            return False
    def _check_time_match(self) -> bool:
        """check if the time of the candle matches the current time"""
        now = datetime.utcnow().replace(second=0, microsecond=0)
        candle_time = self.kdf.index[-1].replace(second=0, microsecond=0)
        if now - timedelta(minutes=1) == candle_time:
            return True
        else:
            return False

    def generate_order_price(self, kdf:pd.DataFrame) -> pd.DataFrame:  
        order_df = pd.DataFrame()
        kdf["atr"] = pta.atr(kdf["high"], kdf["low"], kdf["close"], length = self.params['atr_len'], mamode = "EMA")
        order_df['buy1'] = kdf['close'].shift(1) - kdf['atr'].shift(1) *2
        order_df['sell1'] = kdf['close'].shift(1) + kdf['atr'].shift(1) *2
        order_df['buy2'] = kdf['close'].shift(1) - kdf['atr'].shift(1) * 4
        order_df['sell2'] = kdf['close'].shift(1) + kdf['atr'].shift(1) * 4
        order_df['close'] = kdf['close']  
        order_df['symbol'] = self.symbol   
        order_df['lot'] = self.lot  
        return order_df[['symbol', 'lot', 'close', 'buy1', 'sell1', 'buy2', 'sell2']]
    
    def task(self) -> bool:
        """main task of the executor
        1. cancel open orders
        2. check threshold
        3. update klines
        4. generate order price
        5. send orders
        """
        self.cancel_open_orders()
        flag = self.check_threshold(self.threshold)
        if flag:
            self.close_position()
        else:
            order_df = self.generate_order_price(self.kdf)
            response = self.send_batch_order(order_df)
            return True
        return False
        
    def run(self) -> None:
        while True:
            if self._check_time_match():
                time.sleep(self.interval)
            else:
                flag = self.update_klines()
                if flag:
                    compelete = self.task()
                    if compelete:
                        self.logger.info(f"Task completed. Waiting for next task.\n------------------")
                    else:
                        self.logger.error(f"Task failed. Waiting for next task.\n------------------")
                    time.sleep(self.interval) 
                else:
                    time.sleep(self.interval/3)

if __name__ == "__main__":
    timbersaw.setup()
    executor = ExecPopinjay(symbol = "ETHUSDC", timeframe = "1m")
    executor.run()
