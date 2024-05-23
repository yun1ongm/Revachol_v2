import pandas as pd
import pandas_ta as pta
import numpy as np


class IdxAdxStochrsi:
    """
    Args:
        kdf (pd.DataFrame): dataframe with kline
        adx_len (int): length of adx
        rsi_len (int): length of rsi
        kd (int): length of kd   
    """
    index_name = "idx_adx_stochrsi"

    def __init__(self, kdf, adx_len, stoch_len, rsi_len, kd) -> None:
        self.kdf = kdf
        self.adx_len = adx_len
        self.stoch_len = stoch_len
        self.rsi_len = rsi_len
        self.kd = kd

    def _adx_stochrsi(self) -> pd.DataFrame:
        adx= pd.DataFrame(pta.adx(self.kdf["high"], self.kdf["low"], self.kdf["close"], timeperiod=self.adx_len))
        adx.columns = ["adx","pdi","mdi"]
        stochrsi = pd.DataFrame(pta.stochrsi(self.kdf["close"], length=self.stoch_len , rsi_length = self.rsi_len, k=self.kd, d=self.kd))
        stochrsi.columns = ["k","d"]
        adx_stochrsi = pd.concat([adx,stochrsi],axis=1)
        condition1 = adx_stochrsi["k"] > adx_stochrsi["d"]
        condition2 = adx_stochrsi["k"].shift(1) <= adx_stochrsi["d"].shift(1)
        adx_stochrsi["GXvalue"] = np.where(condition1 & condition2, adx_stochrsi["d"], 0)
        condition3 = adx_stochrsi["k"] < adx_stochrsi["d"]
        condition4 = adx_stochrsi["k"].shift(1) >= adx_stochrsi["d"].shift(1)
        adx_stochrsi["DXvalue"] = np.where(condition3 & condition4, adx_stochrsi["d"], 0)
        return adx_stochrsi

    def generate_dematr_signal(self, dema_len) -> pd.DataFrame:
        adx_stochrsi = self._adx_stochrsi()
        kdf_sig = pd.concat([self.kdf, adx_stochrsi], axis=1)
        kdf_sig["dema"] = pta.dema(kdf_sig["close"], length= dema_len)
        kdf_sig["atr"] = pta.atr(kdf_sig["high"], kdf_sig["low"], kdf_sig["close"], length= dema_len, mamode = 'EMA')
        kdf_sig["signal"] = 0
        kdf_sig.loc[
                (kdf_sig["adx"] >= 25) & (kdf_sig["GXvalue"] < 15) & (0 < kdf_sig["GXvalue"]),
                "signal"
            ] = 1
        kdf_sig.loc[
                (kdf_sig["adx"] >= 25) & (kdf_sig["DXvalue"] > 85), 
                "signal"
            ] = -1

        return kdf_sig[["open", "volume_U", "high", "low", "close", "atr", "signal", "dema"]]
    
    def generate_atr_signal(self, vol_len) -> pd.DataFrame:
        adx_stochrsi = self._adx_stochrsi()
        kdf_sig = pd.concat([self.kdf, adx_stochrsi], axis=1)
        kdf_sig["atr"] = pta.atr(kdf_sig["high"], kdf_sig["low"], kdf_sig["close"], length= vol_len, mamode = 'EMA')
        kdf_sig["signal"] = 0

        kdf_sig.loc[
                (kdf_sig["adx"] >= 25) & (kdf_sig["GXvalue"] < 15) & (0 < kdf_sig["GXvalue"]),
                "signal"
            ] = 1
        kdf_sig.loc[
                (kdf_sig["adx"] >= 25) & (kdf_sig["DXvalue"] > 85), 
                "signal"
            ] = -1

        return kdf_sig[["open", "volume_U", "high", "low", "close", "atr", "signal"]]



