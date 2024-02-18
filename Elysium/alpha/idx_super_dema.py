import pandas as pd
import pandas_ta as ta
import numpy as np

class IdxSuperDema:
    """
    Args:
        kdf (pd.DataFrame): dataframe with kline
        sptr_len (int): length of supertrend
        sptr_k (int): multiplier of supertrend
        dema_len (int): length of dema
        atr_len (int): length of atr

    """
    index_name = "idx_super_dema"
    
    def __init__(self, kdf, sptr_len, sptr_k, dema_len,  atr_f, atr_s) -> None:
        self.kdf = kdf
        self.sptr_len = sptr_len
        self.sptr_k = sptr_k
        self.dema_len = dema_len
        self.atr_f = atr_f
        self.atr_s = atr_s

    def _supertrend(self) -> pd.DataFrame:
        supertrend = ta.supertrend(
            self.kdf["high"], self.kdf["low"], self.kdf["close"], self.sptr_len, self.sptr_k
        )
        supertrend.columns = ["stop_price", "direction", "lbound", "ubound"]

        return supertrend[["stop_price", "direction"]]
    
    def _bodyatr(self) -> pd.DataFrame:
        kdf = self.kdf
        atr_fast = ta.atr(
            kdf["high"], kdf["low"], kdf["close"], length=self.atr_f, mamode="ema"
        )
        atr_slow = ta.atr(
            kdf["high"], kdf["low"], kdf["close"], length=self.atr_s, mamode="ema"
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
        datr["atr"] = datr["Xvalue"].replace(0, method="ffill")
        datr['body'] = kdf['high'] - kdf['low']
        datr['bodyatr'] = datr['body'] / datr['atr']
        return datr[['bodyatr', 'atr']]
    
    def _double_atr(self) -> pd.DataFrame:
        kdf = self.kdf
        atr_fast = ta.atr(
            kdf["high"], kdf["low"], kdf["close"], length= self.atr_f, mamode="ema"
        )
        atr_slow = ta.atr(
            kdf["high"], kdf["low"], kdf["close"], length= self.atr_s, mamode="ema"
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
    
    def generate_bodyatr_signal(self) -> pd.DataFrame:
        try:
            supertrend = self._supertrend()
            bodyatr = self._bodyatr()
            kdf_sig = pd.concat([self.kdf, supertrend, bodyatr], axis=1)
            kdf_sig["dema"] = ta.dema(kdf_sig["close"], length=self.dema_len)
            kdf_sig["volume_ema"] = ta.ema(kdf_sig["volume_USDT"], length=self.atr_s)
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

            return kdf_sig[["close", "stop_price", "bodyatr", "signal"]]
        except Exception as e:
            print(e)
            return None
    
    def generate_dematr_signal(self) -> pd.DataFrame:
        try:
            supertrend = self._supertrend()
            kdf_sig = pd.concat([self.kdf, supertrend], axis=1)
            kdf_sig["dema"] = ta.dema(kdf_sig["close"], length=self.dema_len)
            datr = self._double_atr()
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
            print(e)
            return None