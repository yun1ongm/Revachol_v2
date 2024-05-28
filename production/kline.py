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

    def __init__(self, symbol, timeframe) -> None:
        """
        Args:
            symbol (str): symbol
            timeframe (str): timeframe
        """
        self.symbol = symbol
        self.timeframe = timeframe
        self.timeframe_int = {
            '1m': 1,
            '5m': 5,
            '15m': 15,
            '1h': 60
        }.get(self.timeframe, 1)
        self._export_kline_csv()
    
    def _export_kline_csv(self) -> None:
        url = f"{self.base_url}/fapi/v1/continuousKlines"
        export_dir = main_path + "/production/data/"
        if not os.path.exists(export_dir):
            os.makedirs(export_dir)
        self.export_path = export_dir + f"{self.symbol}_{self.timeframe}.csv"
        self.limit = 300
        session = requests.Session()
        params = {
            "pair": self.symbol,
            "contractType": "PERPETUAL",
            "interval": self.timeframe,
            "limit": self.limit
        }
        try:
            res = session.get(url, params=params)
            ohlcv = res.json()
            unfin_candle = ohlcv.pop() # remove unfinished candle
            kdf = pd.DataFrame(
                ohlcv,
                columns=self.kline_columns,
            )
            kdf = self._convert_kdf_datatype(kdf)
            kdf.to_csv(self.export_path)
            self.logger.info(f"{self.limit} candles time to {datetime.utcfromtimestamp(int(unfin_candle[0])/1000)} exported.\n------------------")
            self.push_discord({"content": f"Market:{self.limit} candles time to {datetime.utcfromtimestamp(int(unfin_candle[0])/1000)} exported.\n------------------"})
        except Exception as e:
            self.logger.error(e)
        finally:
            session.close()

    def _convert_kdf_datatype(self, kdf) -> pd.DataFrame:
        kdf.opentime = [
            datetime.utcfromtimestamp(int(x) / 1000.0).replace(microsecond=0) for x in kdf.opentime
        ]
        kdf.open = kdf.open.astype("float")
        kdf.high = kdf.high.astype("float")
        kdf.low = kdf.low.astype("float")
        kdf.close = kdf.close.astype("float")
        kdf.volume = kdf.volume.astype("float")
        kdf.closetime = [
            datetime.utcfromtimestamp(int(x) / 1000.0).replace(microsecond=0) for x in kdf.closetime
        ]
        kdf.volume_U = kdf.volume_U.astype("float")
        kdf.num_trade = kdf.num_trade.astype("int")
        kdf.taker_buy = kdf.taker_buy.astype("float")
        kdf.taker_buy_volume_U = kdf.taker_buy_volume_U.astype("float")
        kdf.ignore = kdf.ignore.astype("float")
        kdf.set_index("opentime", inplace=True)

        return kdf
    
    async def update_klines(self) -> bool:
        url = f"{self.base_url}/fapi/v1/continuousKlines"

        with open(self.export_path, 'r') as file:
            kdf = pd.read_csv(file, index_col=0)
            if len(kdf) > 24*60/self.timeframe_int:
                self._export_kline_csv()
                self.logger.warning(f"Data refreshed up at {datetime.now()}.\n------------------")
                return False
            kdf.closetime = pd.to_datetime(kdf.closetime)
            back_time = kdf.closetime[-1]

        async with aiohttp.ClientSession() as session:
            params = {
                "pair": self.symbol,
                "contractType": "PERPETUAL",
                "interval": self.timeframe,
                "startTime": int(back_time.timestamp() * 1000),
                "endTime": int(datetime.now().timestamp() * 1000)
            }
            try:
                async with session.get(url, params=params, timeout=10) as response:
                    ohlcv = await response.json()
                    latest_kdf = pd.DataFrame(
                            ohlcv,
                            columns= self.kline_columns,
                        )
                    latest_kdf = self._convert_kdf_datatype(latest_kdf)

                    if len(latest_kdf) >= 2:
                        # remove unfinished candle
                        latest_kdf = latest_kdf.iloc[:-1]
                        latest_kdf.to_csv(self.export_path, mode='a', header=False)
                        self.logger.info(f"{len(latest_kdf)} canlde to {latest_kdf.closetime[-1]} added.")
                        await self.push_discord({"content": 
                                       f"Market:{len(latest_kdf)} canlde to {latest_kdf.closetime[-1]} added.\n------------------"})
                return True

            except Exception as e:
                self.logger.error(e)
                return False
        
    async def push_discord(self, payload:dict, rel_path = "/production/config.yaml"):
        try:
            with open(main_path + rel_path, 'r') as stream:
                config_dict = yaml.safe_load(stream)
                url = config_dict['discord_webhook']["url"]
                headers = {'Content-Type': 'application/json'}
                async with aiohttp.ClientSession() as session:
                    async with session.post(url, data=json.dumps(payload), headers=headers) as response:
                        pass
        except Exception as e:
            self.logger.exception(e)

if __name__ == "__main__":
    timbersaw.setup()
    test = KlineGenerator("BTCUSDT", "1m")
    loop = asyncio.get_event_loop()
    while True:
        if loop.run_until_complete(test.update_klines()):
            time.sleep(20)
        else:
            time.sleep(10)
