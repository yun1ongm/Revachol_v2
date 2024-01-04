import sys
import time
import warnings

import pandas as pd
import pandas_ta as ta
import numpy as np

temp_path = "/Users/rivachol/Desktop/Elysium"
sys.path.append(temp_path)
from market.market_bot import MarketEngine
from alpha.alpha_base import AtrCalculator


warnings.filterwarnings("ignore")


class Indicators:
    def engulfing(kdf):
        engulf_df = kdf[["open", "high", "low", "close"]]
        engulf_df["engulf_signal"] = np.where(
            (engulf_df["close"] > engulf_df["open"])
            & (engulf_df["low"] < engulf_df["low"].shift(1))
            & (engulf_df["close"] > engulf_df["high"].shift(1)),
            1,
            np.where(
                (engulf_df["close"] < engulf_df["open"])
                & (engulf_df["high"] > engulf_df["high"].shift(1))
                & (engulf_df["close"] < engulf_df["low"].shift(1)),
                -1,
                0,
            ),
        )

        return engulf_df


class AlpEngulfingAtr(AtrCalculator):
    alpha_name = "alp_engulfing_atr"
    symbol = "ETHUSDT"
    timeframe = "5m"
    sizer = 0.1

    v_len = 10
    vol_k = 2
    atr_len = 13
    atr_k = 2
    wlr = 2

    def __init__(self):
        super().__init__(self.alpha_name, self.sizer, self.atr_k, self.wlr)

    def gen_index_signal(self, kdf) -> pd.DataFrame:
        try:
            engulf_df = Indicators.engulfing(kdf)
            engulf_df["volume"] = kdf["volume"]
            engulf_df["volume_ema"] = ta.ema(engulf_df["volume"], length=self.v_len)
            engulf_df["signal"] = 0
            engulf_df["atr"] = ta.atr(
                engulf_df["high"],
                engulf_df["low"],
                engulf_df["close"],
                length=self.atr_len,
            )

            engulf_df.loc[
                (engulf_df["volume"] > engulf_df["volume_ema"] * self.vol_k)
                & (engulf_df["engulf_signal"] == 1),
                "signal",
            ] = 1

            engulf_df.loc[
                (engulf_df["volume"] > engulf_df["volume_ema"] * self.vol_k)
                & (engulf_df["engulf_signal"] == -1),
                "signal",
            ] = -1
            return engulf_df[["high", "low", "close", "signal", "atr"]]
        except Exception as e:
            self._log(e)


if __name__ == "__main__":
    alpha = AlpEngulfingAtr()
    market = MarketEngine(alpha.symbol, alpha.timeframe)
    interval = 10
    while True:
        try:
            market.update_CKlines()
            index_signal = alpha.gen_index_signal(market.kdf)
            alpha.alpha_signal = alpha.generate_signal(index_signal)
            time.sleep(interval)
        except Exception as e:
            print(e)
            time.sleep(interval / 2)
