import sys
sys.path.append("/Users/rivachol/Desktop/Rivachol_v2/")

from research.strategy.cta_base import BacktestEngine
import pandas as pd

class StgyBodyatrMulti(BacktestEngine):
    """this strategy contains technicals of candle's body and atr, which aims to change position with candle's body/atr exceeding threshold"""
    def __init__(self, alpha_name, symbol, timeframe, start, window_days) -> None: 
        super().__init__(alpha_name, symbol, timeframe, start, window_days)
        self.performance_df = self.initialize_portfolio_variables(self.kdf)

    def run(self, kdf_signal: pd.DataFrame, harvest_ratio: float, retreat_ratio: float) -> pd.DataFrame:
        """

        Args:
            kdf_signal (pd.DataFrame): dataframe with signal
            upbody_ratio (float): upbody ratio
            downbody_ratio (float): downbody ratio

        """
        value = self.initial_money
        position = 0

        for index, row in kdf_signal.iterrows():
            signal = row.signal
            close = row.close
            stop_price = row.stop_price
            bodyatr = row.bodyatr

            realized_pnl = 0
            commission = 0

            if position > 0:
                unrealized_pnl = (close - entry_price) * position
                money_thresh = (entry_price * position < self.initial_money * self.leverage)
                if close < stop_price or bodyatr > harvest_ratio:
                    realized_pnl = unrealized_pnl
                    entry_price = 0
                    position = 0
                    commission = self.comm * position * close
                    value += unrealized_pnl - commission

                elif signal == 1 and money_thresh:
                    entry_price =(entry_price * position + close * self.sizer)/(position + self.sizer)
                    position += self.sizer
                    commission = self.comm * self.sizer * close
                    value -= commission
                elif signal == -1:
                    if position > self.sizer:
                        entry_price =(entry_price * position - close * self.sizer)/(position - self.sizer)
                        position += -self.sizer
                        commission = self.comm * self.sizer * close
                        value -= commission
                    else:
                        realized_pnl = unrealized_pnl
                        entry_price = 0
                        position = 0
                        commission = self.comm * position * close
                        value += unrealized_pnl - commission

            elif position < 0:
                unrealized_pnl = (close - entry_price) * position
                money_thresh = (entry_price * -position < self.initial_money * self.leverage)
                if close > stop_price or bodyatr > retreat_ratio:
                    realized_pnl = unrealized_pnl
                    entry_price = 0
                    position = 0
                    commission = self.comm * -position * close
                    value += unrealized_pnl - commission

                elif signal == -1 and money_thresh:
                    entry_price = (entry_price * position - close * self.sizer) / (position - self.sizer)
                    position += -self.sizer
                    commission = self.comm * self.sizer * close
                    value -= commission
                elif signal == 1:
                    if position < -self.sizer:
                        entry_price = (entry_price * position + close * self.sizer) / (position + self.sizer)
                        position += self.sizer
                        commission = self.comm * self.sizer * close
                        value -= commission
                    else:
                        realized_pnl = unrealized_pnl
                        entry_price = 0
                        position = 0
                        commission = self.comm * -position * close
                        value += unrealized_pnl - commission


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