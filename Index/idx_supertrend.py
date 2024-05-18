import pandas as pd
import pandas_ta as pta
import numpy as np

class IdxSupertrend:
    """
    Args:
        kdf (pd.DataFrame): dataframe with kline
        sptr_len (int): length of supertrend
        sptr_k (int): multiplier of supertrend

    """
    index_name = "idx_super_dema"
    
    def __init__(self, kdf, sptr_len, sptr_k) -> None:
        self.kdf = kdf
        self.sptr_len = sptr_len
        self.sptr_k = sptr_k

    def _supertrend(self) -> pd.DataFrame:
        supertrend = pta.supertrend(
            self.kdf["high"], self.kdf["low"], self.kdf["close"],  self.sptr_len,  self.sptr_k
        )
        supertrend.columns = ["stop_price", "direction", "lbound", "ubound"]

        return supertrend[["stop_price", "direction"]]

    def _price_change(self):
        kdf_sig = self.kdf[["open", "high", "low", "close"]]
        kdf_sig['delta_down'] = kdf_sig['close'] - max(kdf_sig['high'],kdf_sig['high'].shift(1)) 
        kdf_sig['delta_up'] = kdf_sig['close'] - min(kdf_sig['low'],kdf_sig['low'].shift(1))
        kdf_sig['delta_close'] = kdf_sig['close'] - kdf_sig['close'].shift(1)
        kdf_sig['delta_price'] = np.where(kdf_sig['delta_close'] >= 0, kdf_sig['delta_up'], kdf_sig['delta_down'])
        return kdf_sig[['delta_close', 'delta_down', 'delta_up', 'delta_price']]
    
    def generate_popinjay_signal(self, atr_entry) -> pd.DataFrame:
        try:
            supertrend = self._supertrend()
            price_change = self._price_change()
            kdf_sig = pd.concat([self.kdf, supertrend, price_change], axis=1)
            kdf_sig['atr'] = pta.atr(kdf_sig["high"], kdf_sig["low"], kdf_sig["close"], length=self.sptr_len, mamode = 'EMA')
            kdf_sig['cum_delta'] = 0
            kdf_sig["signal"] = 0

            cumdelta = 0
            for index, row in kdf_sig.iterrows():
                delta_price = row['delta_price']
                if delta_price >= 0 and cumdelta >= 0:
                    cumdelta += delta_price
                elif delta_price >= 0 and cumdelta < 0:
                    cumdelta = delta_price
                elif delta_price < 0 and cumdelta >= 0:
                    cumdelta = delta_price
                elif delta_price < 0 and cumdelta < 0:
                    cumdelta += delta_price
                kdf_sig.loc[index, 'cum_delta'] = cumdelta
            
            kdf_sig.loc[
                (kdf_sig["direction"] == 1)
                & (kdf_sig["cum_delta"] < -kdf_sig["atr"] * atr_entry),
                "signal",
            ] = 1
            kdf_sig.loc[
                (kdf_sig["direction"] == -1)
                & (kdf_sig["cum_delta"] > kdf_sig["atr"] * atr_entry),
                "signal",
            ] = -1

            return kdf_sig[["open", "volume_U", "high", "low", "close", "stop_price", "atr", "signal"]]
        except Exception as e:
            print(e)
            return None
    
    def generate_dematr_signal(self, dema_len) -> pd.DataFrame:
        try:
            supertrend = self._supertrend()
            kdf_sig = pd.concat([self.kdf, supertrend], axis=1)
            kdf_sig["dema"] = pta.dema(kdf_sig["close"], length =dema_len)
            kdf_sig['sma'] = pta.sma(kdf_sig["close"], length = self.sptr_len)
            kdf_sig["atr"] = pta.atr(kdf_sig["high"], kdf_sig["low"], kdf_sig["close"], length= dema_len, mamode = 'EMA')
            kdf_sig["volume_ema"] = pta.ema(kdf_sig["volume_U"], length=self.sptr_len)
            kdf_sig["signal"] = 0

            kdf_sig.loc[
                (kdf_sig["close"] > kdf_sig["sma"])
                & (kdf_sig["low"] < kdf_sig["sma"])
                & (kdf_sig["direction"] == 1)
                & (kdf_sig["volume_U"] < kdf_sig["volume_ema"]),
                "signal",
            ] = 1

            kdf_sig.loc[
                (kdf_sig["close"] < kdf_sig["sma"])
                & (kdf_sig["high"] > kdf_sig["sma"])
                & (kdf_sig["direction"] == -1)
                & (kdf_sig["volume_U"] < kdf_sig["volume_ema"]),
                "signal",
            ] = -1

            return kdf_sig[["open", "high", "low", "close", "volume_U", "atr", "signal", "dema"]]
        except Exception as e:
            print(e)
            return None
    
    def generate_atr_signal(self) -> pd.DataFrame:
        try:
            supertrend = self._supertrend()
            kdf_sig = pd.concat([self.kdf, supertrend], axis=1)
            kdf_sig["atr"] = pta.atr(kdf_sig["high"], kdf_sig["low"], kdf_sig["close"], length=self.sptr_len, mamode = 'EMA')
            kdf_sig["volume_ema"] = pta.ema(kdf_sig["volume_U"], length=self.sptr_len)
            kdf_sig["signal"] = 0

            kdf_sig.loc[
                (kdf_sig["direction"] == 1)
                & (kdf_sig["direction"].shift(1) == -1)
                & (kdf_sig["volume_U"] > kdf_sig["volume_ema"]),
                "signal",
            ] = 1

            kdf_sig.loc[
                (kdf_sig["direction"] == -1)
                & (kdf_sig["direction"].shift(1) == 1)
                & (kdf_sig["volume_U"] > kdf_sig["volume_ema"]),
                "signal",
            ] = -1

            return kdf_sig[["open", "volume_U", "high", "low", "close", "atr", "signal"]]
        except Exception as e:
            print(e)
            return None