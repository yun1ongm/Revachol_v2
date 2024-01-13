import time
import warnings
import pandas as pd
import pandas_ta as ta
import numpy as np

import sys
temp_path = "/Users/rivachol/Desktop/Elysium"
sys.path.append(temp_path)
import warnings
warnings.filterwarnings("ignore")

from market.market_bot import MarketEngine
from alpha.alpha_base import DematrCalculator


class Indicators:
    def adx(kdf, period):
        adx = ta.adx(kdf["high"], kdf["low"], kdf["close"], length=period)
        adx.columns = ["adx", "plus", "minus"]
        return adx

    def stochrsi(kdf, period, kd):
        stochrsi = ta.stochrsi(kdf["close"], rsi_length=period, k=kd, d=kd)
        stochrsi.columns = ["k", "d"]
        condition1 = stochrsi["k"] > stochrsi["d"]
        condition2 = stochrsi["k"].shift(1) <= stochrsi["d"].shift(1)
        stochrsi["GXvalue"] = np.where(condition1 & condition2, stochrsi["d"], 0)
        condition3 = stochrsi["k"] < stochrsi["d"]
        condition4 = stochrsi["k"].shift(1) >= stochrsi["d"].shift(1)
        stochrsi["DXvalue"] = np.where(condition3 & condition4, stochrsi["d"], 0)
        return stochrsi

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


class AlpAdxStochrsiDematr(DematrCalculator):
    alpha_name = "alp_adx_stochrsi_dematr"
    symbol = "ETHUSDT"
    timeframe = "5m"
    sizer = 0.5

    adx_len = 24
    rsi_len = 12
    kd = 4
    dema_len = 14
    atr_f = 14
    atr_s = 29
    atr_profit = 8
    atr_loss = 5

    def __init__(self) -> None:
        super().__init__(self.alpha_name, self.sizer, self.atr_profit, self.atr_loss)

    def gen_index_signal(self, kdf) -> pd.DataFrame:
        try:
            adx = Indicators.adx(kdf, self.adx_len)
            stochrsi = Indicators.stochrsi(kdf, self.rsi_len, self.kd)
            kdf_sig = pd.concat([kdf[["high", "low", "close"]], adx, stochrsi], axis=1)
            kdf_sig["dema"] = ta.dema(kdf_sig["close"], length=self.dema_len)
            datr = Indicators.double_atr(kdf, self.atr_f, self.atr_s)
            kdf_sig["atr"] = datr["Xvalue"]
            kdf_sig["signal"] = 0
            kdf_sig.loc[
                (kdf_sig["adx"] >= 30)
                & (kdf_sig["GXvalue"] < 20)
                & (0 < kdf_sig["GXvalue"]),
                "signal",
            ] = 1
            kdf_sig.loc[
                (kdf_sig["adx"] >= 30) & (kdf_sig["DXvalue"] > 80), "signal"
            ] = -1

            return kdf_sig[["high", "low", "close", "atr", "signal", "dema"]]
        except Exception as e:
            self._log(e)


if __name__ == "__main__":
    alpha = AlpAdxStochrsiDematr()
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
            time.sleep(interval)
