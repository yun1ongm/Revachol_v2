import pandas as pd
import pandas_ta as pta
import numpy as np

class IdxTrendline:

    index_name = "idx_trendline"

    def __init__(self, kdf, swing=14, reset = 50, slope=1.0, calcMethod='Atr'):
        self.kdf_signal = kdf
        self.timedelta = kdf.index[-1] - kdf.index[-2]
        self.swing = swing
        self.reset = reset
        self.slope = slope
        self.calcMethod = calcMethod # Atr, Stdev, Linreg
        # Placeholder variables
        self.rh = 0.0
        self.rh_count = 0
        self.rh_index = np.nan
        self.rl = 0.0
        self.rl_count = 0
        self.rl_index = np.nan
    
    def _calculate_ralative_hl(self, delta_price:float):
        self.kdf_signal["upper"] = np.nan
        self.kdf_signal["lower"] = np.nan
        self.kdf_signal["rh"] = np.nan
        self.kdf_signal["rl"] = np.nan
        # Calculate relative highs and lows
        for index, row in self.kdf_signal.iterrows():
            #更新最高点
            if row['high'] > self.rh:
                self.rh = row['high']
                self.rh_index = index
                self.rh_count = 0
            #未出现高点，且周期在swing和reset之间，更新上轨，周期数加1
            elif self.rh_count >= self.swing and self.rh_count < self.reset:
                self.kdf_signal.loc[index,"rh"] = self.rh
                self.rh_count += 1
                self.kdf_signal.loc[index, "upper"] = self.rh - self.rh_count * delta_price
            #未出现高点的周期超过reset，更新最高点并重新计算swing周期内上轨
            elif self.rh_count >= self.reset:
                self.rh = self.kdf_signal['high'].loc[(index-self.timedelta*self.swing):index].max()
                self.kdf_signal.loc[index,"rh"] = self.rh
                self.rh_count = self.swing
                self.kdf_signal.loc[index, "upper"] = self.rh - self.rh_count * delta_price
            #未出现高点且周期数小于swing,当周期数加1
            else:
                self.rh_count += 1

            if row['low'] < self.rl:
                self.rl = row['low']
                self.rl_index = index
                self.rl_count = 0
            elif self.rl_count >= self.swing and self.rl_count < self.reset:
                self.kdf_signal.loc[index,"rl"] = self.rl
                self.rl_count += 1
                self.kdf_signal.loc[index, "lower"] = self.rl + self.rl_count * delta_price
            elif self.rl_count >= self.reset:
                self.rl = self.kdf_signal['low'].loc[(index-self.timedelta*self.swing):index].min()
                self.kdf_signal.loc[index,"rl"] = self.rl
                self.rl_count = self.swing
                self.kdf_signal.loc[index, "lower"] = self.rl + self.rl_count * delta_price
            else:
                self.rl_count += 1

    def _calculate_delta_price(self) -> float:
        kdf = self.kdf_signal
        if self.calcMethod == 'Atr':
            delta_price = pta.atr(kdf["high"], kdf["low"], kdf["close"], length=self.swing, mamode = 'EMA').mean()/self.swing * self.slope
        elif self.calcMethod == 'Stdev':
            delta_price = pta.stdev(kdf["close"], length=self.swing).mean()/self.swing * self.slope
        # elif self.calcMethod == 'Linreg':
        #     slope = math.fabs(ta.SMA(self.src * self.n, self.length) - ta.SMA(self.src, self.length) * ta.SMA(self.n, self.length)) / ta.VAR(self.n, self.length) / 2 * self.mult
        return delta_price
    
    def trendline(self):
        delta_price = self._calculate_delta_price()
        self._calculate_ralative_hl(delta_price)
        return self.kdf_signal[["upper", "lower", "rh", "rl"]]

