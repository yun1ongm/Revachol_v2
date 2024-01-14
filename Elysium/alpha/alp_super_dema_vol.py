import sys
import time
import warnings

import pandas as pd
import pandas_ta as ta

temp_path = "/Users/rivachol/Desktop/Rivachol_v2/Elysium"
sys.path.append(temp_path)
from market.market_bot import MarketEngine
from alpha.alpha_base import VolCalculator


warnings.filterwarnings("ignore")


class Indicators:
    def supertrend(kdf, sptr_len, sptr_k):
        supertrend = ta.supertrend(
            kdf["high"], kdf["low"], kdf["close"], sptr_len, sptr_k
        )
        supertrend.columns = ["stop_price", "direction", "lbound", "ubound"]

        return supertrend[["stop_price", "direction"]]


class AlpSuperDemaVol(VolCalculator):
    alpha_name = "alp_super_dema_vol"
    symbol = "ETHUSDT"
    timeframe = "5m"
    sizer = 0.1

    sptr_len = 21
    sptr_k = 4
    dema_len = 27
    vol_len = 19
    vol_k = 4.5

    def __init__(self) -> None:
        super().__init__(self.alpha_name, self.sizer, self.vol_k)

    def gen_index_signal(self, kdf) -> pd.DataFrame:
        try:
            supertrend = Indicators.supertrend(kdf, self.sptr_len, self.sptr_k)
            kdf_sig = pd.concat([kdf, supertrend], axis=1)
            kdf_sig["dema"] = ta.dema(kdf_sig["close"], length=self.dema_len)
            kdf_sig["volume_ema"] = ta.ema(kdf_sig["volume"], length=self.vol_len)
            kdf_sig["signal"] = 0

            kdf_sig.loc[
                (kdf_sig["close"] > kdf_sig["dema"])
                & (kdf_sig["low"] <= kdf_sig["dema"])
                & (kdf_sig["direction"] == 1)
                & (kdf_sig["volume"] < kdf_sig["volume_ema"]),
                "signal",
            ] = 1

            kdf_sig.loc[
                (kdf_sig["close"] < kdf_sig["dema"])
                & (kdf_sig["high"] >= kdf_sig["dema"])
                & (kdf_sig["direction"] == -1)
                & (kdf_sig["volume"] < kdf_sig["volume_ema"]),
                "signal",
            ] = -1

            return kdf_sig[["close", "volume", "volume_ema", "stop_price", "signal"]]
        except Exception as e:
            self._log(e)


if __name__ == "__main__":
    alpha = AlpSuperDemaVol()
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
