import pandas as pd
import sys
sys.path.append("/Users/rivachol/Desktop/Rivachol_v2/")
from research.backtest import BacktestFramework

class StgyDematrMulti(BacktestFramework):
    """
        Args:
            atr_profit (float): atr profit
            atr_loss (float): atr loss
            money (float): initial money
            leverage (float): leverage
    """
    strategy_name = "stgy_dematr_multi"

    def __init__(self, atr_profit, atr_loss, money, leverage) -> None:
        self.atr_profit = atr_profit
        self.atr_loss = atr_loss
        self.money = money
        self.leverage = leverage
        self.comm = 0.0002

    def _strategy_run(self, value, signal, position, close, atr, dema, entry_price) -> tuple:
        realized_pnl = 0
        commission = 0
        sizer = round(self.money/close, 3)

        if position > 0:
            unrealized_pnl = (close - entry_price) * position
            money_thresh = (entry_price * position < self.money * self.leverage)

            if close < dema - self.stop_loss or close > dema + self.stop_profit or signal == -1:
                realized_pnl = unrealized_pnl
                commission = self.comm * position * close
                value += unrealized_pnl - commission
                entry_price = 0
                position = 0
                self.stop_loss = 0
                self.stop_profit = 0

            elif signal == 1 and money_thresh:
                entry_price =(entry_price * position + close * sizer)/(position + sizer)
                position += sizer
                commission = self.comm * sizer * close
                value -= commission
                self.stop_loss = atr * self.atr_loss
                self.stop_profit = atr * self.atr_profit

        elif position < 0:
            unrealized_pnl = (close - entry_price) * position
            money_thresh = (entry_price * -position < self.money * self.leverage)

            if close < dema - self.stop_profit or close > dema + self.stop_loss or signal == 1:
                realized_pnl = unrealized_pnl
                commission = self.comm * -position * close
                value += unrealized_pnl - commission
                entry_price = 0
                position = 0
                self.stop_loss = 0
                self.stop_profit = 0

            elif signal == -1 and money_thresh:
                entry_price = (entry_price * position - close * sizer) / (position - sizer)
                position += -sizer
                commission = self.comm * sizer * close
                value -= commission
                self.stop_loss = atr * self.atr_loss
                self.stop_profit = atr * self.atr_profit

        else:
            entry_price = 0
            unrealized_pnl = 0
            self.stop_loss = 0
            self.stop_profit = 0

            if signal == 1:
                entry_price = close
                position += sizer
                commission = self.comm * sizer * close
                value -= commission
                self.stop_loss = atr * self.atr_loss
                self.stop_profit = atr * self.atr_profit

            elif signal == -1:
                entry_price = close
                position += -sizer
                commission = self.comm * sizer * close
                value -= commission
                self.stop_loss = atr * self.atr_loss
                self.stop_profit = atr * self.atr_profit

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
            atr = row.atr
            dema = row.dema

            value, signal, position, entry_price, unrealized_pnl, realized_pnl, commission= self._strategy_run(
                value, signal, position, close, atr, dema, entry_price
            )

            portfolio = self.record_values(portfolio, index, value, signal, position, entry_price, unrealized_pnl, realized_pnl, commission)
        return portfolio
