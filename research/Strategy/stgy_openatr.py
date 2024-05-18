import pandas as pd
import sys
sys.path.append("/Users/rivachol/Desktop/Rivachol_v2/")
from research.backtest import BacktestFramework

class StgyOpenAtr(BacktestFramework):
    """
        Args:
            atr_profit (float): atr profit
            atr_loss (float): atr loss
            money (float): initial money
            leverage (float): leverage

    """
    strategy_name = "stgy_open_atr"

    def __init__(self, atr_profit, atr_loss, money, leverage) -> None:
        self.atr_profit = atr_profit
        self.atr_loss = atr_loss
        self.money = money
        self.leverage = leverage
        self.comm = 0.0004

    def _strategy_run(self, value, signal, position, close, high, low, atr, entry_price) -> tuple:
        realized_pnl = 0
        commission = 0
        sizer = round(self.money/close, 3)

        if position > 0:
            unrealized_pnl = (close - entry_price) * position
            stop_loss = entry_price - atr * self.atr_loss
            stop_profit = entry_price + atr * self.atr_profit

            if low < stop_loss or high > stop_profit or signal == -1:
                realized_pnl = unrealized_pnl
                commission = self.comm * position * close
                value += unrealized_pnl - commission
                entry_price = 0
                position = 0

        elif position < 0:
            unrealized_pnl = (close - entry_price) * position
            stop_loss = entry_price + atr * self.atr_loss
            stop_profit = entry_price - atr * self.atr_profit

            if low < stop_profit or high > stop_loss or signal == 1:
                realized_pnl = unrealized_pnl
                commission = self.comm * -position * close
                value += unrealized_pnl - commission
                entry_price = 0
                position = 0

        else:
            entry_price = 0
            stop_loss = 0
            stop_profit = 0
            unrealized_pnl = 0

            if signal == 1:
                entry_price = close
                position += sizer
                commission = self.comm * sizer * close
                value -= commission

            elif signal == -1:
                entry_price = close
                position += -sizer
                commission = self.comm * sizer * close
                value -= commission

        return value, signal, position, entry_price, stop_profit, stop_loss, unrealized_pnl, realized_pnl, commission

    def generate_portfolio(self, index_signal: pd.DataFrame) -> pd.DataFrame:
        """
        Args:
            index_signal (pd.DataFrame): dataframe with columns 
        """
        portfolio = self.initialize_portfolio_variables(index_signal)
        value = self.money
        position = 0
        entry_price = 0

        for index, row in index_signal.iterrows():
            signal = row.signal
            close = row.close
            high = row.high
            low = row.low
            atr = row.atr

            value, signal, position, entry_price, stop_profit, stop_loss, unrealized_pnl, realized_pnl, commission= self._strategy_run(
                value, signal, position, close, high, low, atr, entry_price
            )
            
            portfolio = self.record_values_slsp(portfolio, index, value, signal, position, entry_price, stop_loss, stop_profit, unrealized_pnl, realized_pnl, commission)
        return portfolio