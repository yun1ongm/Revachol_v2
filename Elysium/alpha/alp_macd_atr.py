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
    def macd(kdf, fast, slow, signaling):
        macd = ta.macd(kdf["close"], fast=fast, slow=slow, signal=signaling)
        macd.columns = ["diff", "macd", "dea"]
        # 生成金叉死叉信号
        condition1 = macd["diff"] > macd["dea"]
        condition2 = macd["diff"].shift(1) <= macd["dea"].shift(1)
        macd["GXvalue"] = np.where(condition1 & condition2, macd["dea"], 0)
        condition3 = macd["diff"] < macd["dea"]
        condition4 = macd["diff"].shift(1) >= macd["dea"].shift(1)
        macd["DXvalue"] = np.where(condition3 & condition4, macd["dea"], 0)
        return macd


class AlpMacdAtr(AtrCalculator):
    alpha_name = "macd_atr"
    symbol = "ETHUSDT"
    timeframe = "15m"
    sizer = 0.1

    fast = 11
    slow = 32
    signaling = 8
    threshold = 1.5
    atr_len = 13
    atr_k = 2
    wlr = 1.5

    def __init__(self) -> None:
        super().__init__(self.alpha_name, self.sizer, self.atr_k, self.wlr)

    def gen_index_signal(self, kdf) -> pd.DataFrame:
        macd = Indicators.macd(kdf, self.fast, self.slow, self.signaling)
        kdf_sig = pd.concat([kdf, macd], axis=1)
        kdf_sig["signal"] = 0
        kdf_sig["atr"] = ta.atr(
            kdf_sig["high"], kdf_sig["low"], kdf_sig["close"], length=self.atr_len
        )
        # 零上金叉
        kdf_sig.loc[
            (kdf_sig["GXvalue"] < self.threshold) & (kdf_sig["GXvalue"] > 0), "signal"
        ] = 1
        # 零下死叉
        kdf_sig.loc[
            (kdf_sig["DXvalue"] > -self.threshold) & (kdf_sig["DXvalue"] < 0), "signal"
        ] = -1

        return kdf_sig[["high", "low", "close", "signal", "atr"]]


if __name__ == "__main__":
    alpha = AlpMacdAtr()
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
