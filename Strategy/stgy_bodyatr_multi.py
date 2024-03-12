import pandas as pd
import sys
sys.path.append("/Users/rivachol/Desktop/Rivachol_v2/")
from backtest import BacktestFramework

class StgyBodyatrMulti(BacktestFramework):
    """
        Args:
            kdf_signal (pd.DataFrame): dataframe with klines and signal
            upbody_ratio (float): upbody ratio
            downbody_ratio (float): downbody ratio
            money (float): initial money
            leverage (float): leverage
            sizer (float): sizer

    """
    strategy_name = "stgy_bodyatr_multi"

    def __init__(self, harvest_ratio, retreat_ratio, money, leverage, sizer) -> None:
        self.harvest_ratio = harvest_ratio
        self.retreat_ratio = retreat_ratio
        self.money = money
        self.leverage = leverage
        self.sizer = sizer
        self.comm = 0.0004

    def _strategy_run(self, value, signal, position, close, high, low, stop_price, bodyatr, entry_price) -> tuple:
        realized_pnl = 0
        commission = 0

        if position > 0:
            unrealized_pnl = (close - entry_price) * position
            money_thresh = (entry_price * position < self.money * self.leverage)
            if close < stop_price or bodyatr > self.harvest_ratio:
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
            money_thresh = (entry_price * -position < self.money * self.leverage)
            if close > stop_price or bodyatr > self.retreat_ratio:
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

        return value, signal, position, entry_price, stop_price, unrealized_pnl, realized_pnl, commission

    def generate_portfolio(self, index_signal: pd.DataFrame) -> pd.DataFrame:
        """
        Args:
            index_signal (pd.DataFrame): dataframe with columns 
        """
        portfolio = self.initialize_portfolio_variables(index_signal)
        value = self.money
        position = 0
        entry_price = 0
        stop_price = 0

        for index, row in index_signal.iterrows():
            signal = row.signal
            close = row.close
            high = row.high
            low = row.low
            stop_price = row.stop_price
            bodyatr = row.bodyatr

            value, signal, position, entry_price, stop_price, unrealized_pnl, realized_pnl, commission= self._strategy_run(
                value, signal, position, close, high, low, stop_price, bodyatr, entry_price
            )
            portfolio = self.record_values_sp(portfolio, index, value, signal, position, entry_price, stop_price, unrealized_pnl, realized_pnl, commission)
        return portfolio