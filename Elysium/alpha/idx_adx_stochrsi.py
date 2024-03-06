
import pandas as pd
import pandas_ta as ta
import numpy as np


class IdxAdxStochrsi:
    """
    Args:
        kdf (pd.DataFrame): dataframe with kline
        adx_len (int): length of adx
        rsi_len (int): length of rsi
        kd (int): length of kd
        dema_len (int): length of dema
        atr_len (int): length of atr    
    """
    index_name = "idx_adx_stochrsi"

    def __init__(self, kdf, adx_len, rsi_len, kd, dema_len, atr_len) -> None:
        self.kdf = kdf
        self.adx_len = adx_len
        self.rsi_len = rsi_len
        self.kd = kd
        self.dema_len = dema_len
        self.atr_len = atr_len

    def _adx(self) -> pd.DataFrame:
        adx = ta.adx(self.kdf["high"], self.kdf["low"], self.kdf["close"], length=self.adx_len)
        adx.columns = ["adx", "plus", "minus"]

        return adx

    def _stochrsi(self) -> pd.DataFrame:
        stochrsi = ta.stochrsi(self.kdf["close"], rsi_length=self.rsi_len, k=self.kd, d=self.kd)
        stochrsi.columns = ["k", "d"]
        condition1 = stochrsi["k"] > stochrsi["d"]
        condition2 = stochrsi["k"].shift(1) <= stochrsi["d"].shift(1)
        stochrsi["GXvalue"] = np.where(condition1 & condition2, stochrsi["d"], 0)
        condition3 = stochrsi["k"] < stochrsi["d"]
        condition4 = stochrsi["k"].shift(1) >= stochrsi["d"].shift(1)
        stochrsi["DXvalue"] = np.where(condition3 & condition4, stochrsi["d"], 0)
        return stochrsi

    def generate_dematr_signal(self) -> pd.DataFrame:
        try:
            adx = self._adx()
            stochrsi = self._stochrsi()
            kdf_sig = pd.concat([self.kdf, adx, stochrsi], axis=1)
            kdf_sig["dema"] = ta.dema(kdf_sig["close"], length=self.dema_len)
            kdf_sig["atr"] = ta.atr(kdf_sig["high"], kdf_sig["low"], kdf_sig["close"], length=self.atr_len)
            kdf_sig["signal"] = 0
            kdf_sig.loc[
                (kdf_sig["adx"] >= 25)
                & (kdf_sig["GXvalue"] < 25)
                & (0 < kdf_sig["GXvalue"]),
                "signal",
            ] = 1
            kdf_sig.loc[
                (kdf_sig["adx"] >= 25) & (kdf_sig["DXvalue"] > 75), "signal"
            ] = -1

            return kdf_sig[["high", "low", "close", "atr", "signal", "dema"]]
        except Exception as e:
            print(e)
            return None

