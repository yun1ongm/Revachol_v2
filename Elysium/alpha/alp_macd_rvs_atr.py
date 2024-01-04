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

    def dematr(kdf, atr_len, dema_len):
        atr = ta.atr(
            kdf["high"], kdf["low"], kdf["close"], length=atr_len, mamode="ema"
        )
        ema = ta.ema(atr, length=dema_len)
        dematr = pd.concat([atr, ema], axis=1)
        dematr.columns = ["atr_ema", "atr_dema"]
        dematr["Xvalue"] = np.where(
            (dematr["atr_ema"] > dematr["atr_dema"])
            & (dematr["atr_ema"].shift(1) < dematr["atr_dema"].shift(1)),
            dematr["atr_dema"],
            0,
        )
        dematr["Xvalue"] = np.where(
            (dematr["atr_ema"] < dematr["atr_dema"])
            & (dematr["atr_ema"].shift(1) > dematr["atr_dema"].shift(1)),
            dematr["atr_dema"],
            0,
        )
        dematr["Xvalue"] = dematr["Xvalue"].replace(0, method="ffill")
        return dematr


class AlpMacdRvsAtr(AtrCalculator):
    alpha_name = "alp_macd_rvs_atr"
    symbol = "ETHUSDT"
    timeframe = "5m"
    sizer = 0.1

    fast = 6
    slow = 18
    signaling = 17
    threshold = 1
    atr_len = 27
    atr_k = 4
    wlr = 1.5

    def __init__(self) -> None:
        super().__init__(self.alpha_name, self.sizer, self.atr_k, self.wlr)

    def gen_index_signal(self, kdf) -> pd.DataFrame:
        macd = Indicators.macd(kdf, self.fast, self.slow, self.signaling)
        kdf_sig = pd.concat([kdf, macd], axis=1)
        dematr = Indicators.dematr(kdf_sig, self.atr_len, self.slow)
        kdf_sig["atr"] = dematr["Xvalue"]
        kdf_sig["signal"] = 0
        # 零下金叉背离
        last_gx = 0
        last_price = 0
        for index, value in kdf_sig["GXvalue"].items():
            price = kdf_sig["low"].at[index]
            if value < last_gx and price > last_price:
                kdf_sig.at[index, "signal"] = 1
            if value < -self.threshold:
                last_gx = value
                last_price = kdf_sig["close"].at[index]
        # 零上死叉背离
        last_dx = 0
        last_price = 0
        for index, value in kdf_sig["DXvalue"].items():
            price = kdf_sig["high"].at[index]
            if value > last_dx and price < last_price:
                kdf_sig.at[index, "signal"] = -1
            if value > self.threshold:
                last_dx = value
                last_price = kdf_sig["close"].at[index]

        return kdf_sig[["high", "low", "close", "signal", "atr"]]


if __name__ == "__main__":
    alpha = AlpMacdRvsAtr()
    market = MarketEngine(alpha.symbol, alpha.timeframe)
    interval = 15
    while True:
        try:
            market.update_CKlines()
            index_signal = alpha.gen_index_signal(market.kdf)
            alpha_signal = alpha.generate_signal_position(index_signal)
            time.sleep(interval)
        except Exception as e:
            print(e)
            time.sleep(interval / 2)
