import pandas as pd
import pandas_ta as pta
import numpy as np

class IdxMacdTrend:
    """
    Args:
        kdf (pd.DataFrame): dataframe with kline
        fast (int): length of fast
        slow (int): length of slow
        signaling (int): length of signaling

    """
    index_name = "idx_macd_trend"

    def __init__(self, kdf, fast, slow, signaling) -> None:
        self.kdf = kdf
        self.fast = fast
        self.slow = slow
        self.signaling = signaling

    def _macd(self):
        macd_df = pta.macd(self.kdf["close"], fast=self.fast, slow=self.slow, signal=self.signaling)
        macd_df.columns = ["diff", "macd", "dea"]
        condition1 = macd_df["diff"] > macd_df["dea"]
        condition2 = macd_df["diff"].shift(1) <= macd_df["dea"].shift(1)
        macd_df["GXvalue"] = np.where(condition1 & condition2, macd_df["dea"], 0)
        condition3 = macd_df["diff"] < macd_df["dea"]
        condition4 = macd_df["diff"].shift(1) >= macd_df["dea"].shift(1)
        macd_df["DXvalue"] = np.where(condition3 & condition4, macd_df["dea"], 0)

        return macd_df
    
    def _bodyatr(self, atr_len):
        batr = self.kdf[["high", "low", "close"]]
        batr["atr"] = pta.atr(batr["high"], batr["low"], batr["close"], atr_len)
        batr['body'] = batr['high'] - batr['low']
        batr['bodyatr'] = batr['body'] / batr['atr']
        return batr[['bodyatr', 'atr']]
    
    def generate_dematr_signal(self, dema_len, threshold) -> pd.DataFrame:
        try:
            macd = self._macd()
            kdf_sig = pd.concat([self.kdf, macd], axis=1)
            kdf_sig["dema"] = pta.dema(kdf_sig["close"], length= dema_len)
            kdf_sig["atr"] = pta.atr(kdf_sig["high"], kdf_sig["low"], kdf_sig["close"], length= dema_len)
            kdf_sig["GX_atr"] = kdf_sig["GXvalue"] / kdf_sig["atr"]
            kdf_sig["DX_atr"] = kdf_sig["DXvalue"] / kdf_sig["atr"]
            kdf_sig["signal"] = 0

            # 零上金叉
            kdf_sig.loc[
                (kdf_sig["GXvalue"] < 0.5) & (kdf_sig["GXvalue"] > 0), "signal"
            ] = 1
            # 零下死叉
            kdf_sig.loc[
                (kdf_sig["DXvalue"] > -0.5) & (kdf_sig["DXvalue"] < 0), "signal"
            ] = -1
            # 零上死叉
            kdf_sig.loc[
                (kdf_sig["DXvalue"] > threshold), "signal"
            ] = -1
            # 零下金叉
            kdf_sig.loc[
                (kdf_sig["GXvalue"] < -threshold), "signal"
            ] = 1

            return kdf_sig[["open", "volume_U", "high", "low", "close", "atr", "signal", "dema"]]
        except Exception as e:
            print(e)