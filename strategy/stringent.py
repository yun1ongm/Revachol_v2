import pandas as pd
import sys

sys.path.append("/Users/rivachol/Desktop/Rivachol_v2/")
from research.backtest import BacktestFramework


class AtrOpen(BacktestFramework):
    """
    Args:
        tp_atr (float): take profit when price is deviated in favor from open price by tp_atr * atr
        sl_atr(float): stop loss when price is deviated against from open price by sl_atr * atr
        money (float): initial money
        leverage (float): leverage

    """

    strategy_name = "atr_open"
    comm = 0.0004

    def __init__(self, tp_atr, sl_atr, money, leverage) -> None:
        self.tp_atr = tp_atr
        self.sl_atr = sl_atr
        self.money = money
        self.leverage = leverage

    def _standarize_sizer(self, close: float) -> float:
        return round(self.money / close, 3)

    def _strategy_run(self, value, signal, position, close, atr, entry_price) -> tuple:
        realized_pnl = 0
        commission = 0
        sizer = round(self.money / close, 3)

        if position > 0:
            unrealized_pnl = (close - entry_price) * position
            stop_loss = entry_price - atr * self.sl_atr
            take_profit = entry_price + atr * self.tp_atr

            if close < stop_loss or close > take_profit:
                realized_pnl = unrealized_pnl
                commission = self.comm * position * close
                value += unrealized_pnl - commission
                entry_price = 0
                position = 0

        elif position < 0:
            unrealized_pnl = (close - entry_price) * position
            stop_loss = entry_price + atr * self.sl_atr
            take_profit = entry_price - atr * self.tp_atr

            if close < take_profit or close > stop_loss:
                realized_pnl = unrealized_pnl
                commission = self.comm * -position * close
                value += unrealized_pnl - commission
                entry_price = 0
                position = 0

        else:
            unrealized_pnl = 0

            if signal == 1:
                entry_price = close
                take_profit = entry_price + atr * self.tp_atr
                stop_loss = entry_price - atr * self.sl_atr
                position += sizer
                commission = self.comm * sizer * close
                value -= commission

            elif signal == -1:
                entry_price = close
                take_profit = entry_price - atr * self.tp_atr
                stop_loss = entry_price + atr * self.sl_atr
                position += -sizer
                commission = self.comm * sizer * close
                value -= commission

            else:
                entry_price = 0
                take_profit = 0
                stop_loss = 0

        return (
            value,
            signal,
            position,
            entry_price,
            take_profit,
            stop_loss,
            unrealized_pnl,
            realized_pnl,
            commission,
        )

    def get_result(self, signal: pd.DataFrame) -> pd.DataFrame:
        portfolio = self.initialize_portfolio_variables(signal)
        self.sizer = self._standarize_sizer(signal.close[0])
        value = self.money
        position = 0
        entry_price = 0

        for index, row in signal.iterrows():
            signal = row.signal
            close = row.close
            atr = row.atr
            (
                value,
                signal,
                position,
                entry_price,
                take_profit,
                stop_loss,
                unrealized_pnl,
                realized_pnl,
                commission,
            ) = self._strategy_run(value, signal, position, close, atr, entry_price)

            portfolio = self.record_values_sltp(
                portfolio,
                index,
                value,
                signal,
                position,
                entry_price,
                stop_loss,
                take_profit,
                unrealized_pnl,
                realized_pnl,
                commission,
            )
        return portfolio
