from datetime import datetime, timedelta
import time
import logging
from retry import retry
import pandas as pd
from binance.um_futures import UMFutures
import sys
sys.path.append("/Users/rivachol/Desktop/Rivachol_v2/")
import contek_timbersaw as timbersaw
import warnings
warnings.filterwarnings("ignore")

class KlineGenerator:
    kdf_columns = [
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
    logger = logging.getLogger(__name__)

    def __init__(self, symbol_list, timeframe, mode = 1, start = None, window_days = None) -> None:
        """
        Args:
            symbol (str): symbol
            timeframe (str): timeframe
            mode (int): 0 for backtest, 1 for live trading
            start (datetime): start time for backtest
            window_days (int): window days for backtest
        """
        self.symbols = symbol_list
        self.timeframe = timeframe
        self.window_days = window_days
        self.start = start
        self.client = UMFutures(timeout=3)
        self.kdf = self._get_klines_df()
    
    @retry(tries=2, delay=1)
    def _get_klines_df(self) -> pd.DataFrame:
        try:
            ohlcv = self.client.continuous_klines(
                self.symbol, "PERPETUAL", self.timeframe, limit=100
            )
            unfin_candle = ohlcv.pop() # remove unfinished candle
            self.logger.info(f"Market bot initiate with candle of {datetime.utcfromtimestamp(int(unfin_candle[0])/1000)}.\n------------------")
            kdf = pd.DataFrame(
                ohlcv,
                columns=self.kdf_columns,
            )
            kdf = self._convert_kdf_datatype(kdf)

            return kdf
        except Exception as e:
            self.logger.exception(e)

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
    def update_klines(self, timeframe = 5) -> bool:
        try:
            latest_ohlcv = self.client.continuous_klines(
                self.symbol, "PERPETUAL", self.timeframe, limit=2
            )
            unfin_ohlcv = latest_ohlcv.pop()
            latest_kdf = pd.DataFrame(
                latest_ohlcv,
                columns=self.kdf_columns,
            )
            unfin_kdf = pd.DataFrame(
                unfin_ohlcv,
                columns=self.kdf_columns,
            )
            latest_kdf = self._convert_kdf_datatype(latest_kdf)
            unfin_kdf = self._convert_kdf_datatype(unfin_kdf)
            self.kdf = pd.concat([self.kdf, latest_kdf])
            if len(self.kdf) > 7*24*60/timeframe:
                self.kdf = self._get_klines_df()
                self.logger.warning(f"Market bot refresh candle data.")
            self.logger.info(
                f"Candle close time: {self.kdf.closetime[-1]} Updated price: {unfin_kdf.close[0]} Volume(U): {round(float(unfin_kdf.volume_U[-1])/1000000,2)}mil\n------------------"
            )
            return True
        
        except Exception as e:
            self.logger.exception(e)
            return False

if __name__ == "__main__":
    timbersaw.setup()
    test = KlineGenerator("BTCUSDT", "1m")
    while True:
        try:
            test.update_klines()
            time.sleep(5)
        except Exception as e:
            time.sleep(5)

