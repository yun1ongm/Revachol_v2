import sys
sys.path.append("/Users/rivachol/Desktop/Rivachol_v2/")

from research.strategy.cta_base import BacktestEngine
import pandas as pd

class StgyDematrSing(BacktestEngine):
    """this strategy contains technicals of Dema and Atr, which aims to change position with candle's too far away from dema based on atr"""
    def __init__(self, alpha_name, symbol, timeframe, start, window_days) -> None: 
        super().__init__(alpha_name, symbol, timeframe, start, window_days)
        self.performance_df = self.initialize_portfolio_variables(self.kdf)

    def run(self, kdf_signal: pd.DataFrame, atr_profit: float, atr_loss: float) -> pd.DataFrame:
        """this strategy aims to change position with candle's close based on ema and atr"""
        value = self.initial_money
        position = 0

        for index, row in kdf_signal.iterrows():
            signal = row.signal
            close = row.close
            high = row.high
            low = row.low
            atr = row.atr
            dema = row.dema

            realized_pnl = 0
            commission = 0

            if position > 0:
                unrealized_pnl = (close - entry_price) * position
                stop_loss = dema - atr * atr_loss
                stop_profit = dema + atr * atr_profit
                if low < stop_loss or high > stop_profit:
                    realized_pnl = unrealized_pnl
                    commission = self.comm * position * close
                    value += unrealized_pnl - commission
                    position = 0
                elif signal == -1:
                    realized_pnl = unrealized_pnl
                    entry_price = close
                    position = -self.sizer
                    commission = 2 * self.comm * self.sizer * close
                    value -= commission

            elif position < 0:
                unrealized_pnl = (close - entry_price) * position
                stop_loss = dema + atr * atr_loss
                stop_profit = dema - atr * atr_profit
                if high > stop_loss or low < stop_profit:
                    realized_pnl = unrealized_pnl
                    commission = self.comm * -position * close
                    value += unrealized_pnl - commission
                    position = 0
                elif signal == 1:
                    realized_pnl = unrealized_pnl
                    entry_price = close
                    position = self.sizer
                    commission = 2 * self.comm * self.sizer * close
                    value -= commission

            else:
                entry_price = 0
                stop_loss = 0
                stop_profit = 0
                unrealized_pnl = 0

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
            self.performance_df["stop_loss"].at[index] = stop_loss
            self.performance_df["stop_profit"].at[index] = stop_profit
            self.performance_df["unrealized_pnl"].at[index] = unrealized_pnl
            self.performance_df["realized_pnl"].at[index] = realized_pnl
            self.performance_df["commission"].at[index] = commission

        return self.performance_df

