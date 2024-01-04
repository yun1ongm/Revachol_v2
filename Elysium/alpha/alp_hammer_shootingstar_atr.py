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
    def hammer_shootingstar(kdf, hs_k):
        hs_df = kdf[["open", "high", "low", "close"]]
        hs_df["up_wick"] = np.where(
            hs_df["open"] > hs_df["close"],
            hs_df["high"] - hs_df["open"],
            hs_df["high"] - hs_df["close"],
        )
        hs_df["down_wick"] = np.where(
            hs_df["open"] > hs_df["close"],
            hs_df["close"] - hs_df["low"],
            hs_df["open"] - hs_df["low"],
        )
        hs_df["body"] = hs_df["close"] - hs_df["open"]
        hs_df["hs_signal"] = np.where(
            (hs_df["body"] > 0)
            & (hs_df["down_wick"] > hs_df["body"] * hs_k)
            & (hs_df["up_wick"] * hs_k < hs_df["body"]),
            1,
            np.where(
                (hs_df["body"] < 0)
                & (hs_df["up_wick"] > -hs_df["body"] * hs_k)
                & (hs_df["down_wick"] * hs_k < -hs_df["body"]),
                -1,
                0,
            ),
        )

        return hs_df


class AlpHammerShootingstarAtr(AtrCalculator):
    alpha_name = "alp_hammer_shootingstar_atr"
    symbol = "ETHUSDT"
    timeframe = "5m"
    sizer = 0.05

    hs_k = 4.5
    v_len = 20
    vol_k = 1.5
    atr_len = 31
    atr_k = 2
    wlr = 1

    def __init__(self) -> None:
        super().__init__(self.alpha_name, self.sizer, self.atr_k, self.wlr)

    def gen_index_signal(self, kdf) -> pd.DataFrame:
        try:
            hs_df = Indicators.hammer_shootingstar(kdf, self.hs_k)
            hs_df["volume"] = kdf["volume"]
            hs_df["close"] = kdf["close"]
            hs_df["atr"] = ta.atr(
                kdf["high"], kdf["low"], kdf["close"], length=self.atr_len
            )
            hs_df["volume_ma"] = ta.ema(kdf["volume"], length=self.v_len)
            hs_df["signal"] = 0

            hs_df.loc[
                (hs_df["volume"] > hs_df["volume_ma"] * self.vol_k)
                & (hs_df["hs_signal"] == 1),
                "signal",
            ] = 1
            hs_df.loc[
                (hs_df["volume"] > hs_df["volume_ma"] * self.vol_k)
                & (hs_df["hs_signal"] == -1),
                "signal",
            ] = -1

            return hs_df[["high", "low", "close", "signal", "atr"]]
        except Exception as e:
            self._log(e)


if __name__ == "__main__":
    alpha = AlpHammerShootingstarAtr()
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
