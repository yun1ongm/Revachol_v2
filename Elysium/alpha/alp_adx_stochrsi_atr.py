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
        # 如果dematr["Xvalue"]为0，那么填充为前一行的值
        dematr["Xvalue"] = dematr["Xvalue"].replace(0, method="ffill")
        return dematr


class AlpAdxStochrsiAtr(AtrCalculator):
    alpha_name = "alp_adx_stochrsi_atr"
    symbol = "ETHUSDT"
    timeframe = "5m"
    sizer = 0.1

    adx_len = 30
    rsi_len = 25
    kd = 8
    atr_len = 20
    atr_k = 4
    wlr = 1.5

    def __init__(self) -> None:
        super().__init__(self.alpha_name, self.sizer, self.atr_k, self.wlr)

    def gen_index_signal(self, kdf) -> pd.DataFrame:
        try:
            adx = Indicators.adx(kdf, self.adx_len)
            stochrsi = Indicators.stochrsi(kdf, self.rsi_len, self.kd)
            dematr = Indicators.dematr(kdf, self.atr_len, self.atr_len)
            kdf_sig = pd.concat([kdf[["high", "low", "close"]], adx, stochrsi], axis=1)
            kdf_sig["atr"] = dematr["Xvalue"]
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

            return kdf_sig[["high", "low", "close", "signal", "atr"]]
        except Exception as e:
            self._log(e)


if __name__ == "__main__":
    alpha = AlpAdxStochrsiAtr()
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
