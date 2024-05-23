from datetime import datetime, timedelta
import time
import logging
from retry import retry
import pandas as pd
from binance.um_futures import UMFutures
import sys
sys.path.append("/Users/rivachol/Desktop/Rivachol_v2/")
import warnings
import os
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

    def __init__(self, symbol_list, timeframe, start = None, window_days = None) -> None:
        """
        Args:
            symbol (list): symbol list
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

    @retry(tries=2, delay=1)
    def generate_testdata(self) -> pd.DataFrame:
        start_time = time.time()
        for symbol in self.symbols:  
            data = []
            for i in range(0, self.window_days):
                istart = self.start + timedelta(days=i)
                iend = istart + timedelta(days=1)
                starttime = int(istart.timestamp() * 1000)
                endtime = int(iend.timestamp() * 1000)
                ohlcv = self.client.continuous_klines(
                    symbol,
                    "PERPETUAL",
                    self.timeframe,
                    startTime=starttime,
                    endtime=endtime,
                )
                if len(ohlcv) == 0:
                    raise ValueError("No data returned from Binance")
                ohlcv.pop()
                data = data + ohlcv
            raw_kdf = pd.DataFrame(
                data,
                columns=self.kdf_columns,
                )
            kdf = self._convert_kdf_datatype(raw_kdf)
            self._dump_df_to_csv(kdf, symbol)
        print(f"Time used to generate test data: {time.time() - start_time} seconds")

    def _dump_df_to_csv(self, kdf:pd.DataFrame, symbol) -> None:
        os.makedirs("test_data", exist_ok=True)
        kdf.to_csv(f"test_data/{symbol}_{self.timeframe}.csv")

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


if __name__ == "__main__":
    test = KlineGenerator(['BTCUSDT', 'ETHUSDT', 'SOLUSDT'], "1m", start = datetime(2024, 4, 20, 0, 0, 0), window_days=30)
    test.generate_testdata()
