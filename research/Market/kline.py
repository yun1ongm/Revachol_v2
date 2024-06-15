from datetime import datetime, timedelta
import time
import logging
import requests
import pandas as pd
import os
import sys

sys.path.append("/Users/rivachol/Desktop/Rivachol_v2/")
import warnings

warnings.filterwarnings("ignore")


class KlineGenerator:
    base_url = "https://fapi.binance.com"
    kline_columns = [
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

    def __init__(self, pairs: list, timeframe, start=None, window_days=None) -> None:
        self.symbols = [(pair + "T") for pair in pairs]
        self.timeframe = timeframe
        self.window_days = window_days
        self.start = start

    def generate_testdata(self) -> pd.DataFrame:
        start_time = time.time()
        for symbol in self.symbols:
            symbol_data = []
            for i in range(0, self.window_days):
                istart = self.start + timedelta(days=i)
                starttime = int(istart.timestamp() * 1000)
                params = {
                    "pair": symbol,
                    "contractType": "PERPETUAL",
                    "interval": self.timeframe,
                    "startTime": starttime,
                    "limit": 60 * 24,
                }
                url = f"{self.base_url}/fapi/v1/continuousKlines"
                res = requests.get(url, params=params)
                ohlcv = res.json()
                if len(ohlcv) == 0:
                    raise ValueError("No data returned from Binance")
                symbol_data.extend(ohlcv)
            kdf = self._format_candle(symbol_data)
            self._dump_df_to_csv(kdf, symbol)
        print(f"Time used to generate test data: {time.time() - start_time} seconds")

    def _dump_df_to_csv(self, kdf: pd.DataFrame, symbol) -> None:

        os.makedirs("test_data", exist_ok=True)
        kdf.to_csv(f"test_data/{symbol}_{self.timeframe}.csv")

    def _format_candle(self, ohlcv: list) -> pd.DataFrame:
        ohlcv.pop()  # remove unfinished candle
        kdf = pd.DataFrame(
            ohlcv,
            columns=self.kline_columns,
        )
        kdf = kdf.drop(columns=["ignore"])
        kdf.opentime = pd.to_datetime(kdf.opentime, unit="ms").dt.floor("s")
        kdf.closetime = pd.to_datetime(kdf.closetime, unit="ms").dt.floor("s")
        kdf = kdf.astype(
            {
                "open": float,
                "high": float,
                "low": float,
                "close": float,
                "volume": float,
                "volume_U": float,
                "num_trade": int,
                "taker_buy": float,
                "taker_buy_volume_U": float,
            }
        )
        kdf = self._calculate_avergae_volume(kdf)
        kdf.set_index("opentime", inplace=True)

        return kdf

    def _calculate_avergae_volume(self, kdf: pd.DataFrame) -> float:
        kdf["1h_volume_U"] = kdf["volume_U"].rolling(window=60).sum()
        kdf["24h_volume_U"] = kdf["volume_U"].rolling(window=1440).sum()
        kdf["1h_volume_U"].fillna(method="bfill", inplace=True)
        kdf["24h_volume_U"].fillna(method="bfill", inplace=True)
        return kdf


if __name__ == "__main__":
    test = KlineGenerator(
        ["BTCUSD", "ETHUSD", "SOLUSD"],
        "1m",
        start=datetime(2024, 5, 10, 0, 0, 0),
        window_days=30,
    )
    test.generate_testdata()
