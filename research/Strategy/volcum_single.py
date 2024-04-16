import sys
sys.path.append("/Users/rivachol/Desktop/Rivachol_v2/")

from research.strategy.cta_base import BacktestEngine
import pandas as pd

class StgyVolcumSing(BacktestEngine):
    """this strategy contains technicals of vol and ema, which aims to change position with candle's volume based on ema of volume"""
    def __init__(self, alpha_name, symbol, timeframe, start, window_days) -> None: 
        super().__init__(alpha_name, symbol, timeframe, start, window_days)
        self.performance_df = self.initialize_portfolio_variables(self.kdf)

    def run(self, kdf_signal: pd.DataFrame, harvest_ratio: float) -> pd.DataFrame:
        """
        Args:
            kdf_signal (pd.DataFrame): signal dataframe
            harvest_ratio (float): harvest ratio
            Returns:
            pd.DataFrame: performance dataframe
        """
        value = self.initial_money
        position = 0

        for index, row in kdf_signal.iterrows():
            signal = row.signal
            close = row.close
            stop_price = row.stop_price
            volcum = row.volcum

            realized_pnl = 0
            commission = 0

            if position > 0:
                unrealized_pnl = (close - entry_price) * position
                if close < stop_price or volcum > harvest_ratio:
                    realized_pnl = unrealized_pnl
                    entry_price = 0
                    position = 0
                    commission = self.comm * position * close
                    value += unrealized_pnl - commission
                elif signal == -1:
                    realized_pnl = unrealized_pnl
                    entry_price = close
                    position = -self.sizer
                    commission = 2 * self.comm * self.sizer * close
                    value -= commission

            elif position < 0:
                unrealized_pnl = (close - entry_price) * position
                if close > stop_price or volcum > retreat_ratio:
                    realized_pnl = unrealized_pnl
                    entry_price = 0
                    position = 0
                    commission = self.comm * -position * close
                    value += unrealized_pnl - commission
                elif signal == 1:
                    realized_pnl = unrealized_pnl
                    entry_price = close
                    position = self.sizer
                    commission = 2 * self.comm * self.sizer * close
                    value -= commission

            else:
                unrealized_pnl = 0
                entry_price = 0

                if signal == 1:
                    entry_price = close
                    position += self.sizer
                    commission = self.comm * self.sizer * close
                    value -= commission

                elif signal == -1:
                    entry_price = close
                    position += -self.sizer
                    commission = self.comm * self.sizer * close
                    value -= commission

            self.performance_df["value"].at[index] = value
            self.performance_df["signal"].at[index] = signal
            self.performance_df["position"].at[index] = position
            self.performance_df["entry_price"].at[index] = entry_price
            self.performance_df["stop_price"].at[index] = stop_price
            self.performance_df["unrealized_pnl"].at[index] = unrealized_pnl
            self.performance_df["realized_pnl"].at[index] = realized_pnl
            self.performance_df["commission"].at[index] = commission

        return self.performance_df

