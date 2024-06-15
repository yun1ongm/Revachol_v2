import pandas as pd
import sys

sys.path.append("/Users/rivachol/Desktop/Rivachol_v2/")
from research.backtest import BacktestFramework


class DemaTrailing(BacktestFramework):
    """
    Args:
        tp_percent (float): when price is deviated in favor from dema by tp_percent, take profit
        sl_percent (float): when price is deviated against from dema by sl_percent, stop loss
        money (float): initial money
        leverage (float): leverage
    """

    strategy_name = "dema_trailing"
    comm = 0.0004

    def __init__(self, tp_percent, sl_percent, money, leverage):
        self.tp_percent = tp_percent
        self.sl_percent = sl_percent
        self.money = money
        self.leverage = leverage

    def _strategy_run(self, value, signal, position, close, dema, entry_price) -> tuple:
        realized_pnl = 0
        commission = 0
        sizer = round(self.money / close, 3)

        if position > 0:
            # 持有多头头寸
            unrealized_pnl = (close - entry_price) * position
            take_profit = dema * (1 + self.tp_percent)
            stop_loss = dema * (1 - self.sl_percent)
            if close < stop_loss or signal == -1 or close > take_profit:
                # 超过止损止盈点全部平仓
                realized_pnl = unrealized_pnl
                commission = self.comm * position * close
                value += unrealized_pnl - commission
                position = 0

        elif position < 0:
            unrealized_pnl = (close - entry_price) * position
            take_profit = dema * (1 - self.tp_percent)
            stop_loss = dema * (1 + self.sl_percent)
            if close > stop_loss or signal == 1 or close < take_profit:
                realized_pnl = unrealized_pnl
                commission = self.comm * -position * close
                value += unrealized_pnl - commission
                position = 0

        else:
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

            else:
                entry_price = 0

        return (
            value,
            signal,
            position,
            entry_price,
            unrealized_pnl,
            realized_pnl,
            commission,
        )

    def get_result(self, signal: pd.DataFrame) -> pd.DataFrame:
        portfolio = self.initialize_portfolio_variables(signal)
        value = self.money
        position = 0
        entry_price = 0

        for index, row in signal.iterrows():
            signal = row.signal
            close = row.close
            dema = row.dema
            (
                value,
                signal,
                position,
                entry_price,
                unrealized_pnl,
                realized_pnl,
                commission,
            ) = self._strategy_run(value, signal, position, close, dema, entry_price)

            portfolio = self.record_values(
                portfolio,
                index,
                value,
                signal,
                position,
                entry_price,
                unrealized_pnl,
                realized_pnl,
                commission,
            )
        return portfolio
