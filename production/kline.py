import sys
main_path = "/Users/rivachol/Desktop/Rivachol_v2"
sys.path.append(main_path)
import warnings
warnings.filterwarnings("ignore")

# -*- coding: utf-8 -*-
from datetime import datetime
import time
import logging
from retry import retry
import pandas as pd
import requests
import contek_timbersaw as timbersaw
import yaml
import requests
import json

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
        self.kdf = self._get_klines_df()
        self.timeframe_int = {
            '1m': 1,
            '5m': 5,
            '15m': 15,
            '1h': 60
        }.get(self.timeframe, 1)
    
    @retry(tries=2, delay=1)
    def _get_klines_df(self) -> pd.DataFrame:
        url = f"{self.base_url}/fapi/v1/continuousKlines"
        params = {
            "pair": self.symbol,
            "contractType": "PERPETUAL",
            "interval": self.timeframe,
            "limit": 200
        }
        try:
            res = requests.get(url, params=params)
            ohlcv = res.json()
            unfin_candle = ohlcv.pop() # remove unfinished candle
            self.logger.info(f"Market bot initiate with candle of {datetime.utcfromtimestamp(int(unfin_candle[0])/1000)}.\n------------------")
            kdf = pd.DataFrame(
                ohlcv,
                columns=self.kline_columns,
            )
            kdf = self._convert_kdf_datatype(kdf)

            return kdf
        except Exception as e:
            self.logger.exception(e)

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
    
    def update_klines(self) -> bool:
        back_time = self.kdf.closetime[-1]
        url = f"{self.base_url}/fapi/v1/continuousKlines"
        params = {
            "pair": self.symbol,
            "contractType": "PERPETUAL",
            "interval": self.timeframe,
            "startTime": int(back_time.timestamp() * 1000),
            "endTime": int(datetime.now().timestamp() * 1000)
        }
        try:
            res = requests.get(url, params=params)
            ohlcv = res.json()
            latest_kdf = pd.DataFrame(
                    ohlcv,
                    columns= self.kline_columns,
                )
            latest_kdf = self._convert_kdf_datatype(latest_kdf)

            if len(latest_kdf) >= 2:
                # remove unfinished candle
                latest_kdf = latest_kdf.iloc[:-1]
                self.kdf = pd.concat([self.kdf, latest_kdf])
                self.kdf.drop_duplicates(inplace=True)
                self.logger.info(f"{len(latest_kdf)} canlde to {latest_kdf.closetime[-1]} added.")
                self.push_discord({"content": 
                                   f"Market:{len(latest_kdf)} canlde to {latest_kdf.closetime[-1]} added.\n------------------"})
            elif len(self.kdf) > 7*24*60/self.timeframe_int:
                self.kdf = self._get_klines_df()
                self.logger.warning(f"Market bot reboot candle data.")
            return True
        
        except Exception as e:
            self.logger.exception(e)
            return False
        
    def push_discord(self, payload:dict, rel_path = "/production/config.yaml"):
        try:
            with open(main_path + rel_path, 'r') as stream:
                config_dict = yaml.safe_load(stream)
                url = config_dict['discord_webhook']["url"]
                headers = {'Content-Type': 'application/json'}
                response = requests.post(url, data=json.dumps(payload), headers=headers)
        except Exception:
            self.logger.exception(response.status_code)

if __name__ == "__main__":
    timbersaw.setup()
    test = KlineGenerator("BTCUSDT", "1m")
    while True:
        if test.update_klines():
            time.sleep(10)
        else:
            time.sleep(5)

