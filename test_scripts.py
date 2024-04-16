from binance.um_futures import UMFutures
import pandas as pd
from datetime import datetime

def get_kdf() -> pd.DataFrame:       
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

    client = UMFutures(timeout=3)
    symbol = "BTCUSDT"
    timeframe = "1m"
    klines = client.continuous_klines(symbol, "PERPETUAL", timeframe, limit=1000)
    kdf = pd.DataFrame(klines, columns=columns)
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

kdf = get_kdf()