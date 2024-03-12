import pandas as pd
import numpy as np
import sys
sys.path.append("/Users/rivachol/Desktop/Rivachol_v2/")
from backtest import BacktestFramework

class StgyDematrSing(BacktestFramework):
    """
        Args:
            kdf_signal (pd.DataFrame): dataframe with klines and signal
            atr_profit (float): atr profit
            atr_loss (float): atr loss
            money (float): initial money
            leverage (float): leverage
            sizer (float): sizer

        Return:
            position (float)
    """
    strategy_name = "stgy_dematr_sing"


    def __init__(self, atr_profit, atr_loss, money, leverage, sizer) -> None:
        self.atr_profit = atr_profit
        self.atr_loss = atr_loss
        self.money = money
        self.leverage = leverage
        self.sizer = sizer
        self.comm = 0.0004

    def _strategy_run(self, value, signal, position, close, high, low, atr, dema, entry_price) -> tuple:
        realized_pnl = 0
        commission = 0
        if position > 0:
            unrealized_pnl = (close - entry_price) * position
            stop_loss = dema - atr * self.atr_loss
            stop_profit = dema + atr * self.atr_profit
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
            stop_loss = dema + atr * self.atr_loss
            stop_profit = dema - atr * self.atr_profit
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

        return value, signal, position, entry_price, stop_profit, stop_loss, unrealized_pnl, realized_pnl, commission

    def generate_portfolio(self, index_signal: pd.DataFrame) -> pd.DataFrame:
        """
        Args:
            index_signal (pd.DataFrame): dataframe with columns high, low, close, signal, atr, dema
        """
        portfolio = self.initialize_portfolio_variables(index_signal)
        value = self.money
        position = 0
        entry_price = 0
        stop_profit = 0
        stop_loss = 0

        for index, row in index_signal.iterrows():
            signal = row.signal
            close = row.close
            high = row.high
            low = row.low
            atr = row.atr
            dema = row.dema

            value, signal, position, entry_price, stop_profit, stop_loss, unrealized_pnl, realized_pnl, commission= self._strategy_run(
                value, signal, position, close, high, low, atr, dema, entry_price
            )

            portfolio = self.record_values(portfolio, index, value, signal, position, entry_price, stop_loss, stop_profit, unrealized_pnl, realized_pnl, commission)
        return portfolio
        

