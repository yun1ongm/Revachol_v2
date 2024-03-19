import pandas as pd
import pandas_ta as pta

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
    
    def __init__(self, kdf, sptr_len, sptr_k, dema_len) -> None:
        self.kdf = kdf
        self.sptr_len = sptr_len
        self.sptr_k = sptr_k
        self.dema_len = dema_len

    def _supertrend(self) -> pd.DataFrame:
        supertrend = pta.supertrend(
            self.kdf["high"], self.kdf["low"], self.kdf["close"],  self.sptr_len,  self.sptr_k
        )
        supertrend.columns = ["stop_price", "direction", "lbound", "ubound"]

        return supertrend[["stop_price", "direction"]]

    def _candle_pressure(self):
        canp = self.kdf[["open", "high", "low", "close"]]
        canp['body'] = (canp['close'] - canp['open'])/ canp['close']
        canp['dump'] = (canp['high'] - canp['close']) / canp['close']
        canp['pump'] = (canp['close'] - canp['low']) / canp['close']

        return canp[["body", "dump", "pump"]]
    
    # def pivot_points(self, window) -> pd.DataFrame:
    #     pivot_points = self.kdf
    #     pivot_points["r1"] = self.kdf["high"].rolling(window=window).max()
    #     pivot_points["s1"] = self.kdf["low"].rolling(window=window).min()
    #     pivot_points["pivot"] = (pivot_points["r1"] + pivot_points["s1"]) / 2
    #     pivot_points["r2"] = pivot_points["pivot"] + (pivot_points["r1"] - pivot_points["s1"])
    #     pivot_points["s2"] = pivot_points["pivot"] - (pivot_points["r1"] - pivot_points["s1"])
    #     return pivot_points
    
    def generate_pressure_signal(self) -> pd.DataFrame:
        try:
            supertrend = self._supertrend()
            canp = self._candle_pressure()
            kdf_sig = pd.concat([self.kdf, supertrend, canp], axis=1)
            kdf_sig["dema"] = pta.dema(kdf_sig["close"], length=self.dema_len)
            kdf_sig["volume_ema"] = pta.ema(kdf_sig["volume_U"], length=self.sptr_len)
            kdf_sig["signal"] = 0
            # 上行区间内上穿均线开仓
            kdf_sig.loc[
                (kdf_sig["low"] <= kdf_sig["dema"])
                & (kdf_sig["close"] > kdf_sig["dema"])
                & (kdf_sig["direction"] == 1)
                & (kdf_sig["volume_U"] < kdf_sig["volume_ema"]),
                "signal",
            ] = 1
            # 下行区间内下穿均线开仓
            kdf_sig.loc[
                (kdf_sig["high"] >= kdf_sig["dema"])
                & (kdf_sig["close"] < kdf_sig["dema"])
                & (kdf_sig["direction"] == -1)
                & (kdf_sig["volume_U"] < kdf_sig["volume_ema"]),
                "signal",
            ] = -1

            return kdf_sig[["open", "volume_U", "high", "low", "close", "stop_price", "body", "dump", "pump", "signal"]]
        except Exception as e:
            print(e)
            return None
    
    def generate_dematr_signal(self) -> pd.DataFrame:
        try:
            supertrend = self._supertrend()
            kdf_sig = pd.concat([self.kdf, supertrend], axis=1)
            kdf_sig["dema"] = pta.dema(kdf_sig["close"], length=self.dema_len)
            kdf_sig["atr"] = pta.atr(kdf_sig["high"], kdf_sig["low"], kdf_sig["close"], length=self.dema_len, fillna=True)
            kdf_sig["volume_ema"] = pta.ema(kdf_sig["volume_U"], length=self.dema_len)
            kdf_sig["signal"] = 0

            kdf_sig.loc[
                (kdf_sig["direction"].shift(1) == -1)
                & (kdf_sig["direction"] == 1)
                & (kdf_sig["volume_U"] < kdf_sig["volume_ema"]),
                "signal",
            ] = 1

            kdf_sig.loc[
                (kdf_sig["direction"].shift(1) == 1)
                & (kdf_sig["direction"] == -1)
                & (kdf_sig["volume_U"] < kdf_sig["volume_ema"]),
                "signal",
            ] = -1

            return kdf_sig[["open", "volume_U", "high", "low", "close", "atr", "signal", "dema"]]
        except Exception as e:
            print(e)
            return None
    
    def generate_pct_signal(self) -> pd.DataFrame:
        try:
            supertrend = self._supertrend()
            kdf_sig = pd.concat([self.kdf, supertrend], axis=1)
            kdf_sig["dema"] = pta.dema(kdf_sig["close"], length=self.dema_len)
            kdf_sig["volume_ema"] = pta.ema(kdf_sig["volume_U"], length=self.dema_len)
            kdf_sig["signal"] = 0
            # 上行区间内上穿均线开仓
            kdf_sig.loc[
                (kdf_sig["low"] <= kdf_sig["dema"])
                & (kdf_sig["close"] > kdf_sig["dema"])
                & (kdf_sig["direction"] == 1)
                & (kdf_sig["volume_U"] < kdf_sig["volume_ema"]),
                "signal",
            ] = 1
            # 下行区间内下穿均线开仓
            kdf_sig.loc[
                (kdf_sig["high"] >= kdf_sig["dema"])
                & (kdf_sig["close"] < kdf_sig["dema"])
                & (kdf_sig["direction"] == -1)
                & (kdf_sig["volume_U"] < kdf_sig["volume_ema"]),
                "signal",
            ] = -1

            return kdf_sig[["open", "volume_U", "high", "low", "close", "dema", "signal"]]
        except Exception as e:
            print(e)
            return None