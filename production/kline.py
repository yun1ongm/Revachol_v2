import sys
import os

main_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
sys.path.append(main_path)
import warnings

warnings.filterwarnings("ignore")

# -*- coding: utf-8 -*-
from datetime import datetime
import time
import logging
import pandas as pd
import requests
import contek_timbersaw as timbersaw
import yaml
import requests
import json
import asyncio
import aiohttp


class KlineGenerator:
    base_url = "https://fapi.binance.com"
    logger = logging.getLogger(__name__)

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

    def __init__(self, pairs, timeframe) -> None:
        self.limit = 300
        self.symbols = [pair + "T" for pair in pairs]
        self.timeframe = timeframe
        self.timeframe_int = {"1m": 1, "5m": 5, "15m": 15, "1h": 60}.get(
            self.timeframe, 1
        )
        self._export_kline_csv()

    def get_24h_ticker(self) -> pd.DataFrame:
        url = f"{self.base_url}/fapi/v1/ticker/24hr"
        res = requests.get(url)
        ticker = pd.DataFrame(res.json())
        ticker.columns = [
            "symbol",
            "price_change",
            "price_change_percent",
            "weighted_avg_price",
            "last_price",
            "last_qty",
            "open_price",
            "high_price",
            "low_price",
            "volume",
            "quote_volume",
            "open_time",
            "close_time",
            "first_id",
            "last_id",
            "count",
        ]
        ticker = ticker.astype(
            {
                "price_change": float,
                "price_change_percent": float,
                "weighted_avg_price": float,
                "last_price": float,
                "last_qty": float,
                "open_price": float,
                "high_price": float,
                "low_price": float,
                "volume": float,
                "quote_volume": float,
                "open_time": lambda x: datetime.utcfromtimestamp(int(x) / 1000.0),
                "close_time": lambda x: datetime.utcfromtimestamp(int(x) / 1000.0),
                "first_id": int,
                "last_id": int,
                "count": int,
            }
        )

        return ticker

    def get_qulified_symbols(self) -> list:
        ticker = self.get_24h_ticker()
        v5_ticker = ticker.sort_values("quote_volume", ascending=False).head(5)
        p5_ticker = ticker.sort_values("price_change_percent", ascending=False).head(5)
        qulified_ticker = pd.merge(v5_ticker, p5_ticker, on="symbol")
        if len(qulified_ticker) == 0:
            self.logger.warning("No qulified ticker found.")
            return None
        self.logger.info(f"Qulified ticker found: {qulified_ticker['symbol'].tolist()}")
        return qulified_ticker["symbol"].tolist()

    def _export_kline_csv(self) -> None:
        url = f"{self.base_url}/fapi/v1/continuousKlines"
        export_dir = main_path + "/production/data/"
        if not os.path.exists(export_dir):
            os.makedirs(export_dir)
        for symbol in self.symbols:
            export_path = export_dir + f"{symbol}_{self.timeframe}.csv"
            session = requests.Session()
            params = {
                "pair": symbol,
                "contractType": "PERPETUAL",
                "interval": self.timeframe,
                "limit": self.limit,
            }
            try:
                res = session.get(url, params=params)
                ohlcv = res.json()
                kdf = self._format_candle(ohlcv)
                update_time = kdf.closetime[-1]
                kdf.to_csv(export_path)
                self.logger.info(
                    f"{symbol}:{self.limit} candles time to {update_time} exported.\n------------------"
                )
                self.push_discord(
                    {
                        "content": f"{symbol}:{self.limit} candles time to {update_time} exported.\n------------------"
                    }
                )
            except Exception as e:
                self.logger.error(e)
                sys.exit(1)
            finally:
                session.close()

    def _format_candle(self, ohlcv: list) -> pd.DataFrame:
        ohlcv.pop()  # remove unfinished candle
        kdf = pd.DataFrame(
            ohlcv,
            columns=self.kline_columns,
        )
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
                "ignore": float,
            }
        )
        kdf.set_index("opentime", inplace=True)

        return kdf

    async def update_klines(self) -> None:
        url = f"{self.base_url}/fapi/v1/continuousKlines"
        for symbol in self.symbols:
            self.export_path = (
                main_path + f"/production/data/{symbol}_{self.timeframe}.csv"
            )
            with open(self.export_path, "r") as file:
                kdf = pd.read_csv(file, index_col=0)
                if len(kdf) > 12 * 60 / self.timeframe_int:
                    self._export_kline_csv()
                    self.logger.warning(
                        f"{symbol} data refreshed up at {datetime.now()}.\n------------------"
                    )
                    return False
                kdf.closetime = pd.to_datetime(kdf.closetime)
                back_time = kdf.closetime[-1]

            async with aiohttp.ClientSession() as session:
                params = {
                    "pair": symbol,
                    "contractType": "PERPETUAL",
                    "interval": self.timeframe,
                    "startTime": int(back_time.timestamp() * 1000),
                    "endTime": int(datetime.now().timestamp() * 1000),
                }
                try:
                    async with session.get(url, params=params, timeout=10) as response:
                        ohlcv = await response.json()
                        latest_kdf = self._format_candle(ohlcv)

                        if len(latest_kdf) >= 2:
                            # remove unfinished candle
                            latest_kdf = latest_kdf.iloc[:-1]
                            latest_kdf.to_csv(self.export_path, mode="a", header=False)
                            self.logger.info(
                                f"{symbol}:{len(latest_kdf)} canlde to {latest_kdf.closetime[-1]} added."
                            )
                            await self.push_discord(
                                {
                                    "content": f"{symbol}:{len(latest_kdf)} canlde to {latest_kdf.closetime[-1]} added.\n------------------"
                                }
                            )
                except Exception as e:
                    self.logger.error(e)
                    return False

    async def push_discord(self, payload: dict, rel_path="/production/config.yaml"):
        try:
            with open(main_path + rel_path, "r") as stream:
                config_dict = yaml.safe_load(stream)
                url = config_dict["discord_webhook"]["url"]
                headers = {"Content-Type": "application/json"}
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        url, data=json.dumps(payload), headers=headers
                    ) as response:
                        pass
        except Exception as e:
            self.logger.exception(e)


if __name__ == "__main__":
    timbersaw.setup()
    test = KlineGenerator(["BTCUSD", "ETHUSD", "SOLUSD"], "1m")
    loop = asyncio.get_event_loop()
    while True:
        if loop.run_until_complete(test.update_klines()):
            time.sleep(20)
        else:
            time.sleep(10)
