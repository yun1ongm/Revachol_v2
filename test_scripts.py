import pandas as pd
from datetime import datetime
import time
import requests
import warnings

warnings.filterwarnings("ignore")

base_url = "https://fapi.binance.com"
klines_col = [
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


def get_klines(symbol, timeframe, limit) -> pd.DataFrame:
    url = f"{base_url}/fapi/v1/continuousKlines"
    params = {
        "pair": symbol,
        "contractType": "PERPETUAL",
        "interval": timeframe,
        "limit": limit,
    }
    try:
        res = requests.get(url, params=params)
        ohlcv = res.json()
        kdf = pd.DataFrame(ohlcv, columns=klines_col)
        kdf = convert_kdf_datatype(kdf)

        return kdf
    except Exception as e:
        print(e)
        return None


def convert_kdf_datatype(kdf) -> pd.DataFrame:
    kdf.opentime = [
        datetime.utcfromtimestamp(int(x) / 1000.0).replace(microsecond=0)
        for x in kdf.opentime
    ]
    kdf.open = kdf.open.astype("float")
    kdf.high = kdf.high.astype("float")
    kdf.low = kdf.low.astype("float")
    kdf.close = kdf.close.astype("float")
    kdf.volume = kdf.volume.astype("float")
    kdf.closetime = [
        datetime.utcfromtimestamp(int(x) / 1000.0).replace(microsecond=0)
        for x in kdf.closetime
    ]
    kdf.volume_U = kdf.volume_U.astype("float")
    kdf.num_trade = kdf.num_trade.astype("int")
    kdf.taker_buy = kdf.taker_buy.astype("float")
    kdf.taker_buy_volume_U = kdf.taker_buy_volume_U.astype("float")
    kdf.ignore = kdf.ignore.astype("float")
    kdf.set_index("opentime", inplace=True)

    return kdf


def update_klines(kdf, symbol, timeframe) -> pd.DataFrame:
    back_time = kdf.closetime[-1]
    url = f"{base_url}/fapi/v1/continuousKlines"
    params = {
        "pair": symbol,
        "contractType": "PERPETUAL",
        "interval": timeframe,
        "startTime": int(back_time.timestamp() * 1000),
        "endTime": int(datetime.now().timestamp() * 1000),
    }
    try:
        res = requests.get(url, params=params)
        ohlcv = res.json()
        latest_kdf = pd.DataFrame(
            ohlcv,
            columns=klines_col,
        )
        latest_kdf = convert_kdf_datatype(latest_kdf)

        if len(latest_kdf) >= 2:
            # remove unfinished candle
            latest_kdf = latest_kdf.iloc[:-1]
            kdf = pd.concat([kdf, latest_kdf])
            kdf.drop_duplicates(inplace=True)
            print(f"{len(latest_kdf)} canlde to {latest_kdf.closetime[-1]} added.")

        return kdf
    except Exception as e:
        print(e)
        time.sleep(5)
        return kdf


if __name__ == "__main__":
    symbol = "BTCUSDT"
    timeframe = "1m"
    kdf = get_klines(symbol, timeframe)
    print(kdf)
    while True:
        kdf = update_klines(kdf, symbol, timeframe, 1440)
        time.sleep(5)
