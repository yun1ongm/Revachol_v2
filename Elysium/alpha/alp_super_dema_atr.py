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
    def supertrend(kdf, period, k):
        sti = ta.supertrend(
            kdf["high"], kdf["low"], kdf["close"], length=period, multiplier=k
        )
        sti.columns = ["bound", "direction", "lower", "upper"]
        return sti

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


class AlpSuperDemaAtr(AtrCalculator):
    alpha_name = "alp_super_dema_atr"
    symbol = "ETHUSDT"
    timeframe = "5m"
    sizer = 0.1

    sptr_len = 29
    sptr_k = 3.5
    dema_len = 28
    atr_len = 17
    atr_k = 7
    wlr = 1.5

    def __init__(self) -> None:
        super().__init__(self.alpha_name, self.sizer, self.atr_k, self.wlr)

    def gen_index_signal(self, kdf) -> pd.DataFrame:
        try:
            supertrend = Indicators.supertrend(kdf, self.sptr_len, self.sptr_k)
            kdf_sig = pd.concat([kdf, supertrend], axis=1)
            kdf_sig["dema"] = ta.dema(kdf_sig["close"], length=self.dema_len)
            dematr = Indicators.dematr(kdf_sig, self.atr_len, self.dema_len)
            kdf_sig["atr"] = dematr["Xvalue"]
            kdf_sig["volume_ema"] = ta.ema(kdf_sig["volume"], length=self.atr_len)
            kdf_sig["signal"] = 0

            kdf_sig.loc[
                (kdf_sig["close"] > kdf_sig["dema"])
                & (kdf_sig["low"] <= kdf_sig["dema"])
                & (kdf_sig["direction"] == 1)
                & (kdf_sig["volume_USDT"] < kdf_sig["volume_ema"]),
                "signal",
            ] = 1

            kdf_sig.loc[
                (kdf_sig["close"] < kdf_sig["dema"])
                & (kdf_sig["high"] >= kdf_sig["dema"])
                & (kdf_sig["direction"] == -1)
                & (kdf_sig["volume_USDT"] < kdf_sig["volume_ema"]),
                "signal",
            ] = -1

            return kdf_sig[["high", "low", "close", "atr", "signal"]]
        except Exception as e:
            self._log(e)


if __name__ == "__main__":
    alpha = AlpSuperDemaAtr()
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
