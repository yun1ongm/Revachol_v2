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
    
    def __init__(self, kdf, sptr_len, sptr_k, dema_len, atr_len) -> None:
        self.kdf = kdf
        self.sptr_len = sptr_len
        self.sptr_k = sptr_k
        self.dema_len = dema_len
        self.atr_len = atr_len

    def _supertrend(self) -> pd.DataFrame:
        supertrend = ta.supertrend(
            self.kdf["high"], self.kdf["low"], self.kdf["close"], self.sptr_len, self.sptr_k
        )
        supertrend.columns = ["bound", "direction", "lbound", "ubound"]

        return supertrend[["bound", "direction"]]
    
    def _body2atr(self):
        body2atr = pd.DataFrame()
        body2atr["atr"] = ta.atr(
            self.kdf["high"], self.kdf["low"], self.kdf["close"], length= self.atr_len, mamode="ema"
        )
        body2atr['body'] = self.kdf['close'] - self.kdf['open']
        body2atr['bodyatr'] = body2atr['body'] / body2atr['atr']
        return  body2atr[['bodyatr', 'atr']]
    
    def _double_atr(self, atr_f, atr_s):
        atr_fast = ta.atr(
            self.kdf["high"], self.kdf["low"], self.kdf["close"], length=atr_f, mamode="ema"
        )
        atr_slow = ta.atr(
            self.kdf["high"], self.kdf["low"], self.kdf["close"], length=atr_s, mamode="ema"
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
        # 如果datr["Xvalue"]为0，那么填充为前一行的值
        datr["Xvalue"] = datr["Xvalue"].replace(0, method="ffill")
        return datr
    
    def generate_bodyatr_signal(self) -> pd.DataFrame:
        try:
            supertrend = self._supertrend()
            body2atr = self._body2atr()
            kdf_sig = pd.concat([self.kdf, supertrend, body2atr], axis=1)
            kdf_sig["dema"] = ta.dema(kdf_sig["close"], length=self.dema_len)
            kdf_sig["volume_ema"] = ta.ema(kdf_sig["volume_USDT"], length=self.atr_len)
            kdf_sig["stop_price"] = kdf_sig["bound"]
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

            return kdf_sig[["signal", "close", "stop_price", "bodyatr"]]
        except Exception as e:
            print(e)
            return None
    
    def generate_dematr_signal(self, atr_f, atr_s) -> pd.DataFrame:
        try:
            supertrend = self._supertrend()
            datr = self._double_atr(atr_f, atr_s)
            kdf_sig = pd.concat([self.kdf, supertrend], axis=1)
            kdf_sig["dema"] = ta.dema(kdf_sig["close"], length=self.dema_len)

            kdf_sig["atr"] = datr["Xvalue"]
            kdf_sig["volume_ema"] = ta.ema(kdf_sig["volume_USDT"], length=self.atr_len)
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