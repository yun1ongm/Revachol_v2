import time
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
    def supertrend(kdf, sptr_len, sptr_k):
        supertrend = ta.supertrend(
            kdf["high"], kdf["low"], kdf["close"], sptr_len, sptr_k
        )
        supertrend.columns = ["bound", "direction", "lbound", "ubound"]

        return supertrend[["bound", "direction"]]

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


class AlpSuperDematr(DematrCalculator):
    alpha_name = "alp_super_dematr"
    symbol = "ETHUSDT"
    timeframe = "5m"
    sizer = 0.2

    sptr_len = 21
    sptr_k = 4
    dema_len = 24
    atr_f = 8
    atr_s = 21
    atr_profit = 6
    atr_loss = 5

    def __init__(self) -> None:
        super().__init__(self.alpha_name, self.sizer, self.atr_profit, self.atr_loss)

    def gen_index_signal(self, kdf) -> pd.DataFrame:
        try:
            supertrend = Indicators.supertrend(kdf, self.sptr_len, self.sptr_k)
            kdf_sig = pd.concat([kdf, supertrend], axis=1)
            kdf_sig["dema"] = ta.dema(kdf_sig["close"], length=self.dema_len)
            datr = Indicators.double_atr(kdf_sig, self.atr_f, self.atr_s)
            kdf_sig["atr"] = datr["Xvalue"]
            kdf_sig["volume_ema"] = ta.ema(kdf_sig["volume_USDT"], length=self.atr_f)
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

            return kdf_sig[["high", "low", "close", "atr", "signal", "dema"]]
        except Exception as e:
            self._log(e)


if __name__ == "__main__":
    alpha = AlpSuperDematr()
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
