import pandas as pd
import pandas_ta as ta
import numpy as np

class IdxMacdRvs:
    """
    Args:
        kdf (pd.DataFrame): dataframe with kline
        fast (int): length of fast
        slow (int): length of slow
        signaling (int): length of signaling

    """
    index_name = "idx_macd_rvs"

    def __init__(self, kdf, fast, slow, signaling, threshold, dema_len) -> None:
        self.kdf = kdf
        self.fast = fast
        self.slow = slow
        self.signaling = signaling
        self.threshold = threshold
        self.dema_len = dema_len

    def _macd(self):
        macd_df = ta.macd(self.kdf["close"], fast=self.fast, slow=self.slow, signal=self.signaling)
        macd_df.columns = ["diff", "macd", "dea"]
        condition1 = macd_df["diff"] > macd_df["dea"]
        condition2 = macd_df["diff"].shift(1) <= macd_df["dea"].shift(1)
        macd_df["GXvalue"] = np.where(condition1 & condition2, macd_df["dea"], 0)
        condition3 = macd_df["diff"] < macd_df["dea"]
        condition4 = macd_df["diff"].shift(1) >= macd_df["dea"].shift(1)
        macd_df["DXvalue"] = np.where(condition3 & condition4, macd_df["dea"], 0)

        return macd_df
    
    def _bodyatr(self):
        batr = self.kdf[["high", "low", "close"]]
        batr["atr"] = ta.atr(batr["high"], batr["low"], batr["close"], self.dema_len)
        batr['body'] = batr['high'] - batr['low']
        batr['bodyatr'] = batr['body'] / batr['atr']
        return batr[['bodyatr', 'atr']]
    
    def generate_bodyatr_signal(self) -> pd.DataFrame:
            macd = self._macd()
            bodyatr = self._bodyatr()
            kdf_sig = pd.concat([self.kdf, macd, bodyatr], axis=1) 
            kdf_sig["signal"] = 0
            kdf_sig["stop_price"] = 0
            # 零下金叉背离
            last_gx = 0
            last_price = 0      
            for index, value in kdf_sig["GXvalue"].items():
                price = kdf_sig["low"].at[index]
                if value < last_gx and price > last_price:
                    kdf_sig.at[index, "signal"] = 1
                    kdf_sig["stop_price"] = kdf_sig["low"].at[index] - kdf_sig["atr"].at[index]
                if value < -self.threshold:
                    last_gx = value
                    last_price = kdf_sig["close"].at[index]
            # 零上死叉背离
            last_dx = 0
            last_price = 0
            for index, value in kdf_sig["DXvalue"].items():
                price = kdf_sig["high"].at[index]
                if value > last_dx and price < last_price:
                    kdf_sig.at[index, "signal"] = -1
                if value > self.threshold:
                    last_dx = value
                    last_price = kdf_sig["close"].at[index]
            return kdf_sig[["open", "volume_U", "high", "low", "close", "stop_price", "bodyatr", "signal"]]
