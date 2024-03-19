import pandas as pd
import pandas_ta as ta
import math

class IdxTrendline:
    """
    Args:
    """
    index_name = "idx_trendline"

    def __init__(self, length=14, mult=1.0, calcMethod='Atr', backpaint=True, upCss='teal', dnCss='red', showExt=True):
        self.length = length
        self.mult = mult
        self.calcMethod = calcMethod
        self.backpaint = backpaint
        self.upCss = upCss
        self.dnCss = dnCss
        self.showExt = showExt

        self.n = 0
        self.src = []

        self.ph = []
        self.pl = []

        self.slope_ph = 0.0
        self.slope_pl = 0.0

        self.offset = length if backpaint else 0

        # Placeholder variables
        self.upper = 0.0
        self.lower = 0.0
        self.upos = 0
        self.dnos = 0

    def calculate_trendlines(self):
        while self.n < len(self.src):
            self.calculate_pivot()
            self.calculate_slope()
            self.calculate_trendline_values()
            self.calculate_extended_lines()
            self.calculate_plots()
            self.calculate_breakouts()
            self.n += 1

        self.check_alerts()

    def calculate_pivot(self):
        self.ph.append(ta.pivothigh(self.src, self.length, self.length))
        self.pl.append(ta.pivotlow(self.src, self.length, self.length))

    def calculate_slope(self):
        if self.calcMethod == 'Atr':
            slope = ta.ATR(self.length) / self.length * self.mult
        elif self.calcMethod == 'Stdev':
            slope = ta.STDDEV(self.src, self.length) / self.length * self.mult
        elif self.calcMethod == 'Linreg':
            slope = math.fabs(ta.SMA(self.src * self.n, self.length) - ta.SMA(self.src, self.length) * ta.SMA(self.n, self.length)) / ta.VAR(self.n, self.length) / 2 * self.mult

        self.slope_ph = slope if self.ph[self.n] else self.slope_ph
        self.slope_pl = slope if self.pl[self.n] else self.slope_pl

    def calculate_trendline_values(self):
        self.upper = self.ph[self.n] if self.ph[self.n] else self.upper - self.slope_ph
        self.lower = self.pl[self.n] if self.pl[self.n] else self.lower + self.slope_pl

        self.upos = 0 if self.ph[self.n] else 1 if self.src[self.n] > self.upper - self.slope_ph * self.length else self.upos
        self.dnos = 0 if self.pl[self.n] else 1 if self.src[self.n] < self.lower + self.slope_pl * self.length else self.dnos

    def calculate_extended_lines(self):
        if self.ph[self.n] and self.showExt:
            self.uptl = [(self.n - self.offset, self.ph[self.n] if self.backpaint else self.upper - self.slope_ph * self.length), (self.n - self.offset + 1, self.ph[self.n] - self.slope)]
        if self.pl[self.n] and self.showExt:
            self.dntl = [(self.n - self.offset, self.pl[self.n] if self.backpaint else self.lower + self.slope_pl * self.length), (self.n - self.offset + 1, self.pl[self.n] + self.slope)]

    def calculate_plots(self):
        self.upper_plot = self.upper if self.backpaint else self.upper - self.slope_ph * self.length
        self.lower_plot = self.lower if self.backpaint else self.lower + self.slope_pl * self.length

    def calculate_breakouts(self):
        if self.upos > self.upos[-1]:
            self.upper_breakout = self.low
        if self.dnos > self.dnos[-1]:
            self.lower_breakout = self.high

    def check_alerts(self):
        if self.upos > self.upos[-1]:
            print("Upward Breakout: Price broke the down-trendline upward")
        if self.dnos > self.dnos[-1]:
            print("Downward Breakout: Price broke the up-trendline downward")


