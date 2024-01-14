import time
import warnings

import pandas as pd
import pandas_ta as ta
import numpy as np

import sys
temp_path = "/Users/rivachol/Desktop/Rivachol_v2/Elysium"
sys.path.append(temp_path)
import warnings
warnings.filterwarnings("ignore")
from market.market_bot import MarketEngine
from alpha.alpha_base import DematrCalculator


class Indicators:
    def macd(kdf, fast, slow, signaling):
        macd_df = ta.macd(kdf["close"], fast=fast, slow=slow, signal=signaling)
        macd_df.columns = ["diff", "macd", "dea"]
        condition1 = macd_df["diff"] > macd_df["dea"]
        condition2 = macd_df["diff"].shift(1) <= macd_df["dea"].shift(1)
        macd_df["GXvalue"] = np.where(condition1 & condition2, macd_df["dea"], 0)
        condition3 = macd_df["diff"] < macd_df["dea"]
        condition4 = macd_df["diff"].shift(1) >= macd_df["dea"].shift(1)
        macd_df["DXvalue"] = np.where(condition3 & condition4, macd_df["dea"], 0)

        return macd_df

    def double_atr(kdf, atr_f, atr_s):
        atr_fast = ta.atr(
            kdf["high"], kdf["low"], kdf["close"], length=atr_f, mamode="ema"
        )
        atr_slow = ta.atr(
            kdf["high"], kdf["low"], kdf["close"], length=atr_s, mamode="ema"
        )
        datr = pd.concat([atr_fast, atr_slow], axis=1)
        datr.columns = ["atr_fast", "atr_slow"]
        datr["Xvalue"] = np.where(
            (datr["atr_fast"] > datr["atr_slow"])
            & (datr["atr_fast"].shift(1) < datr["atr_slow"].shift(1)),
            datr["atr_slow"],
            np.where(
                (datr["atr_fast"] < datr["atr_slow"])
                & (datr["atr_fast"].shift(1) > datr["atr_slow"].shift(1)),
                datr["atr_slow"],
                0,
            ),
        )
        datr["Xvalue"] = datr["Xvalue"].replace(0, method="ffill")
        return datr


class AlpMacdDematr(DematrCalculator):
    alpha_name = "macd_dematr"
    symbol = "ETHUSDT"
    timeframe = "15m"
    sizer = 0.3

    fast = 16
    slow = 33
    signaling = 9
    threshold = 0.4
    dema_len = 31
    atr_profit = 7
    atr_loss = 6

    def __init__(self) -> None:
        super().__init__(self.alpha_name, self.sizer, self.atr_profit, self.atr_loss)

    def gen_index_signal(self, kdf) -> pd.DataFrame:
        macd = Indicators.macd(kdf, self.fast, self.slow, self.signaling)
        macd = Indicators.macd(kdf, self.fast, self.slow, self.signaling)
        kdf_sig = pd.concat([kdf, macd], axis=1)
        kdf_sig["dema"] = ta.dema(kdf_sig["close"], length=self.dema_len)
        datr = Indicators.double_atr(kdf_sig, self.fast, self.slow)
        kdf_sig["atr"] = datr["Xvalue"]
        kdf_sig["signal"] = 0
        # 零上金叉
        kdf_sig.loc[
            (kdf_sig["GXvalue"] < self.threshold) & (kdf_sig["GXvalue"] > 0), "signal"
        ] = 1
        # 零下死叉
        kdf_sig.loc[
            (kdf_sig["DXvalue"] > -self.threshold) & (kdf_sig["DXvalue"] < 0), "signal"
        ] = -1

        return kdf_sig[["high", "low", "close", "atr", "signal", "dema"]]


if __name__ == "__main__":
    alpha = AlpMacdDematr()
    market = MarketEngine(alpha.symbol, alpha.timeframe)
    interval = 10
    while True:
        try:
            market.update_CKlines()
            index_signal = alpha.gen_index_signal(market.kdf)
            alpha_signal = alpha.generate_signal_position(index_signal)
            time.sleep(interval)
        except Exception as e:
            print(e)
            time.sleep(interval / 2)
