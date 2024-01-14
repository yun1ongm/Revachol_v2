from datetime import datetime
import time
import pandas as pd
import warnings
from retry import retry
from binance.um_futures import UMFutures
import logging
import os

warnings.filterwarnings("ignore")


class MarketEngine:
    kdf_columns = [
        "opentime",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "closetime",
        "volume_USDT",
        "num_trade",
        "taker_buy",
        "taker_buy_quota_volume",
        "ignore",
    ]

    def __init__(self, symbol: str, timeframe: str) -> None:
        self.symbol = symbol
        self.timeframe = timeframe
        self.client = UMFutures(timeout=3)
        self._init_logger()
        self.kdf = self._get_CKlines_df()

    def _init_logger(self) -> None:
        self.logger = logging.getLogger("MarketBot")
        self.logger.setLevel(logging.INFO)
        log_file = "log_book/market_bot.log"
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s, %(message)s")
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

    def _log(self, string) -> None:
        self.logger.info(string)

    @retry(tries=3, delay=1)
    def _get_CKlines_df(self) -> pd.DataFrame:
        try:
            ohlcv = self.client.continuous_klines(
                self.symbol, "PERPETUAL", self.timeframe, limit=1500
            )
            # remove unfinished candle
            ohlcv.pop()
            kdf = pd.DataFrame(
                ohlcv,
                columns=self.kdf_columns,
            )
            kdf = self._convert_kdf_datatype(kdf)
            self._log(f"Market bot started with candle {kdf.closetime[-1]}.")

            return kdf
        except Exception as e:
            self._log(e)

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
        kdf.volume_USDT = kdf.volume_USDT.astype("float")
        kdf.num_trade = kdf.num_trade.astype("int")
        kdf.taker_buy = kdf.taker_buy.astype("float")
        kdf.taker_buy_quota_volume = kdf.taker_buy_quota_volume.astype("float")
        kdf.ignore = kdf.ignore.astype("float")
        kdf.set_index("opentime", inplace=True)

        return kdf

    @retry(tries=1, delay=1)
    def update_CKlines(self) -> None:
        latest_ohlcv = self.client.continuous_klines(
            self.symbol, "PERPETUAL", self.timeframe, limit=2
        )
        latest_ohlcv.pop(0)
        latest_kdf = pd.DataFrame(
            latest_ohlcv,
            columns=self.kdf_columns,
        )
        latest_kdf = self._convert_kdf_datatype(latest_kdf)

        if latest_kdf.index[-1] == self.kdf.index[-1]:
            pass

        else:
            self.kdf = pd.concat([self.kdf, latest_kdf])
            self.kdf = self.kdf.iloc[1:]
            self._log(
                f"Candle close time: {self.kdf.closetime[-1]} Updated Close: {self.kdf.close[-1]} Volume(USDT): {self.kdf.volume_USDT[-1]}\n-----------"
            )

    def get_aggrtrade(self) -> pd.DataFrame:
        aggtrades = self.client.agg_trades(self.symbol)
        aggtdf = pd.DataFrame(aggtrades)
        aggtdf.columns = [
            "id",
            "price",
            "volume",
            "first_id",
            "last_id",
            "time",
            "taker_buy",
        ]
        aggtdf.price = aggtdf.price.astype("float")
        aggtdf.volume = aggtdf.volume.astype("float")

        return aggtdf


if __name__ == "__main__":
    test = MarketEngine("BTCUSDT", "1m")
    while True:
        try:
            test.update_CKlines()
            time.sleep(5)
        except Exception as e:
            time.sleep(5)
