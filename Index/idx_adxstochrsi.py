import talib as ta
import pandas as pd
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

    def __init__(self, kdf, adx_len, rsi_len, k, d, dema_len) -> None:
        self.kdf = kdf
        self.adx_len = adx_len
        self.rsi_len = rsi_len
        self.k = k
        self.d = d
        self.dema_len = dema_len

    def _adx_stochrsi(self) -> pd.DataFrame:
        adx= pd.DataFrame(ta.ADX(self.kdf["high"], self.kdf["low"], self.kdf["close"], timeperiod=self.adx_len))
        adx.columns = ["adx"]
        stochrsi = ta.STOCHRSI(self.kdf["close"], timeperiod=self.rsi_len,fastk_period=self.k, fastd_period=self.d, fastd_matype=0)
        transposed_stochrsi = tuple(zip(*stochrsi))
        stochrsi_df = pd.DataFrame(transposed_stochrsi, index=adx.index,columns=["k","d"])
        adx_stochrsi = pd.concat([adx,stochrsi_df],axis=1)
        condition1 = adx_stochrsi["k"] > adx_stochrsi["d"]
        condition2 = adx_stochrsi["k"].shift(1) <= adx_stochrsi["d"].shift(1)
        adx_stochrsi["GXvalue"] = np.where(condition1 & condition2, adx_stochrsi["d"], 0)
        condition3 = adx_stochrsi["k"] < adx_stochrsi["d"]
        condition4 = adx_stochrsi["k"].shift(1) >= adx_stochrsi["d"].shift(1)
        adx_stochrsi["DXvalue"] = np.where(condition3 & condition4, adx_stochrsi["d"], 0)
        return adx_stochrsi

    def generate_dematr_signal(self) -> pd.DataFrame:
        adx_stochrsi = self._adx_stochrsi()
        kdf_sig = pd.concat([self.kdf, adx_stochrsi], axis=1)
        kdf_sig["dema"] = ta.DEMA(kdf_sig["close"], timeperiod=self.dema_len)
        kdf_sig["atr"] = ta.ATR(kdf_sig["high"], kdf_sig["low"], kdf_sig["close"], timeperiod=self.dema_len)
        kdf_sig["signal"] = 0
        kdf_sig.loc[
                (kdf_sig["adx"] >= 25)
                & (kdf_sig["GXvalue"] < 20)
                & (0 < kdf_sig["GXvalue"]),
                "signal",
            ] = 1
        kdf_sig.loc[
                (kdf_sig["adx"] >= 25) & (kdf_sig["DXvalue"] > 80), "signal"
            ] = -1

        return kdf_sig[["open", "volume_U", "high", "low", "close", "atr", "signal", "dema"]]


