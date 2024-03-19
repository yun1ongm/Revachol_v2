import pandas as pd
import numpy as np
import sys
sys.path.append("/Users/rivachol/Desktop/Rivachol_v2/")
from backtest import BacktestFramework

class StgyDempactSing(BacktestFramework):
    """
        Args:
            kdf_signal (pd.DataFrame): dataframe with klines and signal
            pct_profit (float): percentage profit
            pct_loss (float): percentage loss
            money (float): initial money
            leverage (float): leverage
            sizer (float): sizer

        Return:
            position (float)
    """
    strategy_name = "stgy_dempact_sing"


    def __init__(self, pct_profit, pct_loss, money, leverage, sizer) -> None:
        self.pct_profit =  pct_profit
        self.pct_loss = pct_loss
        self.money = money
        self.leverage = leverage
        self.sizer = sizer
        self.comm = 0.0004

    def _strategy_run(self, value, signal, position, close, dema, entry_price) -> tuple:
        realized_pnl = 0
        commission = 0
        
        if position > 0:
            unrealized_pnl = (close - entry_price) * position
            stop_loss = dema * (1-self.pct_loss)
            stop_profit = dema * (1+self.pct_profit)
            if close <= stop_loss or close >= stop_profit:
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
            stop_loss = dema * (1+self.pct_loss)
            stop_profit = dema * (1-self.pct_profit)
            if close >= stop_loss or close <= stop_profit:
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
            dema = row.dema

            value, signal, position, entry_price, stop_profit, stop_loss, unrealized_pnl, realized_pnl, commission= self._strategy_run(
               value, signal, position, close, dema, entry_price
            )

            portfolio = self.record_values(portfolio, index, value, signal, position, entry_price, stop_loss, stop_profit, unrealized_pnl, realized_pnl, commission)
        return portfolio
        

