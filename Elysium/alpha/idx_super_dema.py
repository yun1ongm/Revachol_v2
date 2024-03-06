import pandas as pd
import pandas_ta as ta

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
            self.kdf["high"], self.kdf["low"], self.kdf["close"],  self.sptr_len,  self.sptr_k
        )
        supertrend.columns = ["stop_price", "direction", "lbound", "ubound"]

        return supertrend[["stop_price", "direction"]]

    def _bodyatr(self):
        batr = self.kdf[["high", "low", "close"]]
        batr["atr"] = ta.atr(batr["high"], batr["low"], batr["close"], self.atr_len)
        batr['body'] = batr['high'] - batr['low']
        batr['bodyatr'] = batr['body'] / batr['atr']
        return batr[['bodyatr', 'atr']]
    
    def generate_bodyatr_signal(self) -> pd.DataFrame:
        try:
            supertrend = self._supertrend()
            bodyatr = self._bodyatr()
            kdf_sig = pd.concat([self.kdf, supertrend, bodyatr], axis=1)
            kdf_sig["dema"] = ta.dema(kdf_sig["close"], length=self.dema_len)
            kdf_sig["volume_ema"] = ta.ema(kdf_sig["volume_U"], length=self.atr_len)
            kdf_sig["signal"] = 0

            kdf_sig.loc[
                (kdf_sig["close"] <= kdf_sig["dema"])
                & (kdf_sig["close"] > kdf_sig["open"])
                & (kdf_sig["direction"] == 1)
                & (kdf_sig["volume_U"] < kdf_sig["volume_ema"]),
                "signal",
            ] = 1

            kdf_sig.loc[
                (kdf_sig["close"] >= kdf_sig["dema"])
                & (kdf_sig["close"] < kdf_sig["open"])
                & (kdf_sig["direction"] == -1)
                & (kdf_sig["volume_U"] < kdf_sig["volume_ema"]),
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
            kdf_sig["atr"] = ta.atr(kdf_sig["high"], kdf_sig["low"], kdf_sig["close"], length=self.atr_len)
            kdf_sig["volume_ema"] = ta.ema(kdf_sig["volume_U"], length=self.atr_len)
            kdf_sig["signal"] = 0

            kdf_sig.loc[
                (kdf_sig["close"] > kdf_sig["dema"])
                & (kdf_sig["low"] <= kdf_sig["dema"])
                & (kdf_sig["direction"] == 1)
                & (kdf_sig["volume_U"] < kdf_sig["volume_ema"]),
                "signal",
            ] = 1

            kdf_sig.loc[
                (kdf_sig["close"] < kdf_sig["dema"])
                & (kdf_sig["high"] >= kdf_sig["dema"])
                & (kdf_sig["direction"] == -1)
                & (kdf_sig["volume_U"] < kdf_sig["volume_ema"]),
                "signal",
            ] = -1

            return kdf_sig[["high", "low", "close", "atr", "signal", "dema"]]
        except Exception as e:
            print(e)
            return None