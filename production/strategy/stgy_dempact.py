import pandas as pd
import sys
sys.path.append("/Users/rivachol/Desktop/Rivachol_v2/")
from research.backtest import BacktestFramework

class StgyDempact(BacktestFramework):

    """
        Args:
            kdf_signal (pd.DataFrame): dataframe with klines and signal
            profit_pct (float): profit percentage
            loss_pct (float): loss percentage
            money (float): initial money
            leverage (float): leverage
            sizer (float): sizer
    """
    strategy_name = "stgy_dempact"

    def __init__(self, profit_pct, loss_pct, money, leverage) -> None:
        self.profit_pct = profit_pct
        self.loss_pct = loss_pct
        self.money = money
        self.leverage = leverage
        self.comm = 0.0004

    def _strategy_run(self, value, signal, position, close, dema, entry_price) -> tuple:
        realized_pnl = 0
        commission = 0
        sizer = round(self.money/close, 3)

        if position > 0:
            unrealized_pnl = (close - entry_price) * position
            dema_pct = (close - dema)/ dema
            if dema_pct <= -self.loss_pct or signal == -1 or dema_pct >= self.profit_pct:
                realized_pnl = unrealized_pnl
                commission = self.comm * position * close
                value += unrealized_pnl - commission
                position = 0

        elif position < 0:
            unrealized_pnl = (close - entry_price) * position
            dema_pct = (close - dema)/ dema
            if dema_pct <= -self.profit_pct or signal == 1 or dema_pct >= self.loss_pct:
                realized_pnl = unrealized_pnl
                commission = self.comm * -position * close
                value += unrealized_pnl - commission
                position = 0

        else:
            entry_price = 0
            unrealized_pnl = 0

            if signal == 1:
                entry_price = close
                position += sizer
                commission = self.comm * sizer * close
                value -= commission

            elif signal == -1:
                entry_price = close
                position += - sizer
                commission = self.comm * sizer * close
                value -= commission

        return value, signal, position, entry_price, unrealized_pnl, realized_pnl, commission


    def generate_portfolio(self, index_signal: pd.DataFrame) -> pd.DataFrame:
        """
        Args:
            index_signal (pd.DataFrame): dataframe with columns high, low, close, signal, atr, dema
        """
        portfolio = self.initialize_portfolio_variables(index_signal)
        value = self.money
        position = 0
        entry_price = 0

        for index, row in index_signal.iterrows():
            signal = row.signal
            close = row.close
            dema = row.dema

            value, signal, position, entry_price, unrealized_pnl, realized_pnl, commission= self._strategy_run(
                value, signal, position, close, dema, entry_price
            )

            portfolio = self.record_values(portfolio, index, value, signal, position, entry_price, unrealized_pnl, realized_pnl, commission)
        return portfolio
