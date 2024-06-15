import pandas as pd
import pandas_ta as pta
import numpy as np


class Adx:
    indicator_name = "adx"

    def __init__(self, kdf, adx_len: int):
        self.kdf = kdf
        self.adx_len = adx_len

    def get_indicator(self) -> pd.DataFrame:
        adx = pd.DataFrame(
            pta.adx(
                self.kdf["high"],
                self.kdf["low"],
                self.kdf["close"],
                timeperiod=self.adx_len,
            )
        )
        adx.columns = ["adx", "pdi", "mdi"]
        return adx


class StochRsi:
    indicator_name = "stochrsi"

    def __init__(self, kdf, stoch_len: int, rsi_len: int, kd: int):
        self.kdf = kdf
        self.stoch_len = stoch_len
        self.rsi_len = rsi_len
        self.kd = kd

    def get_indicator(self) -> pd.DataFrame:
        stochrsi = pd.DataFrame(
            pta.stochrsi(
                self.kdf["close"],
                length=self.stoch_len,
                rsi_length=self.rsi_len,
                k=self.kd,
                d=self.kd,
            )
        )
        stochrsi.columns = ["k", "d"]
        stochrsi["upcross"] = np.where(
            (stochrsi["k"] > stochrsi["d"]) & (stochrsi["k"].shift(1) <= stochrsi["d"]),
            stochrsi["d"],
            0,
        )
        stochrsi["downcross"] = np.where(
            (stochrsi["k"] < stochrsi["d"]) & (stochrsi["k"].shift(1) >= stochrsi["d"]),
            stochrsi["d"],
            0,
        )

        return stochrsi


class Macd:
    indicator_name = "macd"

    def __init__(self, kdf, fast, slow, signal):
        self.kdf = kdf
        self.fast = fast
        self.slow = slow
        self.signal = signal

    def get_indicator(self) -> pd.DataFrame:
        macd = pta.macd(
            self.kdf["close"], fast=self.fast, slow=self.slow, signal=self.signal
        )
        macd.columns = ["diff", "macd", "dea"]
        condition1 = macd["diff"] > macd["dea"]
        condition2 = macd["diff"].shift(1) <= macd["dea"].shift(1)
        macd["GXvalue"] = np.where(condition1 & condition2, macd["dea"], 0)
        condition3 = macd["diff"] < macd["dea"]
        condition4 = macd["diff"].shift(1) >= macd["dea"].shift(1)
        macd["DXvalue"] = np.where(condition3 & condition4, macd["dea"], 0)
        return macd


class Supertrend:
    indicator_name = "supertrend"

    def __init__(self, kdf, sptr_len, sptr_k):
        self.kdf = kdf
        self.sptr_len = sptr_len
        self.sptr_k = sptr_k

    def get_indicator(self) -> pd.DataFrame:
        supertrend = pta.supertrend(
            self.kdf["high"],
            self.kdf["low"],
            self.kdf["close"],
            self.sptr_len,
            self.sptr_k,
        )
        supertrend.columns = ["stop_price", "direction", "lbound", "ubound"]

        return supertrend


class Trendline:
    indicator_name = "trendline"

    def __init__(self, kdf, swing, reset, slope, calcMethod="Atr"):
        self.kdf_signal = kdf
        self.timedelta = kdf.index[-1] - kdf.index[-2]
        self.swing = swing
        self.reset = reset
        self.slope = slope
        self.calcMethod = calcMethod  # Atr, Stdev, Linreg
        # Placeholder variables
        self.rh = 0.0
        self.rh_count = 0
        self.rh_index = np.nan
        self.rl = 0.0
        self.rl_count = 0
        self.rl_index = np.nan

    def _calculate_ralative_hl(self, delta_price: float):
        self.kdf_signal["upper"] = np.nan
        self.kdf_signal["lower"] = np.nan
        self.kdf_signal["rh"] = np.nan
        self.kdf_signal["rl"] = np.nan
        # Calculate relative highs and lows
        for index, row in self.kdf_signal.iterrows():
            # 更新最高点
            if row["high"] > self.rh:
                self.rh = row["high"]
                self.rh_index = index
                self.rh_count = 0
            # 未出现高点，且周期在swing和reset之间，更新上轨，周期数加1
            elif self.rh_count >= self.swing and self.rh_count < self.reset:
                self.kdf_signal.loc[index, "rh"] = self.rh
                self.rh_count += 1
                self.kdf_signal.loc[index, "upper"] = (
                    self.rh - self.rh_count * delta_price
                )
            # 未出现高点的周期超过reset，更新最高点并重新计算swing周期内上轨
            elif self.rh_count >= self.reset:
                self.rh = (
                    self.kdf_signal["high"]
                    .loc[(index - self.timedelta * self.swing) : index]
                    .max()
                )
                self.kdf_signal.loc[index, "rh"] = self.rh
                self.rh_count = self.swing / 2
                self.kdf_signal.loc[index, "upper"] = (
                    self.rh - self.rh_count * delta_price
                )
            # 未出现高点且周期数小于swing,当周期数加1
            else:
                self.rh_count += 1

            if row["low"] < self.rl:
                self.rl = row["low"]
                self.rl_index = index
                self.rl_count = 0
            elif self.rl_count >= self.swing and self.rl_count < self.reset:
                self.kdf_signal.loc[index, "rl"] = self.rl
                self.rl_count += 1
                self.kdf_signal.loc[index, "lower"] = (
                    self.rl + self.rl_count * delta_price
                )
            elif self.rl_count >= self.reset:
                self.rl = (
                    self.kdf_signal["low"]
                    .loc[(index - self.timedelta * self.swing) : index]
                    .min()
                )
                self.kdf_signal.loc[index, "rl"] = self.rl
                self.rl_count = self.swing / 2
                self.kdf_signal.loc[index, "lower"] = (
                    self.rl + self.rl_count * delta_price
                )
            else:
                self.rl_count += 1

    def _calculate_delta_price(self) -> float:
        kdf = self.kdf_signal
        if self.calcMethod == "Atr":
            delta_price = (
                pta.atr(
                    kdf["high"],
                    kdf["low"],
                    kdf["close"],
                    length=self.swing,
                    mamode="EMA",
                ).mean()
                / self.swing
                * self.slope
            )
        elif self.calcMethod == "Stdev":
            delta_price = (
                pta.stdev(kdf["close"], length=self.swing).mean()
                / self.swing
                * self.slope
            )
        # elif self.calcMethod == 'Linreg':
        #     slope = math.fabs(ta.SMA(self.src * self.n, self.length) - ta.SMA(self.src, self.length) * ta.SMA(self.n, self.length)) / ta.VAR(self.n, self.length) / 2 * self.mult
        return delta_price

    def get_indicator(self) -> pd.DataFrame:
        delta_price = self._calculate_delta_price()
        self._calculate_ralative_hl(delta_price)
        return self.kdf_signal[
            ["open", "high", "low", "close", "volume_U", "upper", "lower", "rh", "rl"]
        ]


class Vwap:
    indicator_name = "vwap"

    def __init__(self, kdf, vwap_len: int):
        self.kdf = kdf
        self.vwap_len = vwap_len

    def get_indicator(self) -> pd.DataFrame:
        vwap = pd.DataFrame(
            pta.vwap(
                self.kdf["high"],
                self.kdf["low"],
                self.kdf["close"],
                self.kdf["volume_U"],
                fillna=True,
            )
        )
        vwap.columns = ["vwap"]
        vwap["stdev"] = (
            self.kdf[["close", "high", "low"]]
            .rolling(window=self.vwap_len)
            .std()
            .mean(axis=1)
        )
        vwap["stdev"] = vwap["stdev"].fillna(method="bfill")
        vwap["vwap_upper"] = vwap["vwap"] + vwap["stdev"]
        vwap["vwap_lower"] = vwap["vwap"] - vwap["stdev"]
        return vwap


if __name__ == "__main__":
    import sys

    sys.path.append("/Users/rivachol/Desktop/Rivachol_v2")
    from test_scripts import get_klines

    symbol = "BTCUSDT"
    timeframe = "1m"
    kdf = get_klines(symbol, timeframe, 1500)
    test = StochRsi(kdf, 14, 14, 3)
    test_df = test.get_indicator()
    # export to csv
    test_df.to_csv(
        f"/Users/rivachol/Desktop/Rivachol_v2/test_data/{symbol}_{timeframe}_{test.indicator_name}.csv"
    )
