import pandas as pd
import sys

sys.path.append("/Users/rivachol/Desktop/Rivachol_v2/")
from research.backtest import BacktestFramework


class DemaStd(BacktestFramework):
    """
    Args:
        tp_std (float): take profit when price is deviated in favor from dema by multiple of std
        sl_std (float): stop loss when price is deviated against from dema by multiple of std
        money (float): initial money
        leverage (float): leverage
    """

    strategy_name = "dema_std"
    comm = 0.0004

    def __init__(self, tp_std: float, sl_std: float, money: float, leverage: int):
        self.tp_std = tp_std
        self.sl_std = sl_std
        self.money = money
        self.leverage = leverage

    def _standarize_sizer(self, close: float) -> float:
        return round(self.money / close, 3)

    def _strategy_run(
        self, value, signal, position, close, dema, std, entry_price
    ) -> tuple:
        realized_pnl = 0
        commission = 0

        if position > 0:
            # 持有多头头寸
            unrealized_pnl = (close - entry_price) * position
            stop_loss = dema - std * self.sl_std
            take_profit = dema + std * self.tp_std

            if close < stop_loss or close > take_profit:
                # 超过止损止盈点全部平仓
                realized_pnl = unrealized_pnl
                commission = self.comm * position * close
                value += unrealized_pnl - commission
                entry_price = 0
                position = 0

            elif (
                signal == 1 and entry_price * abs(position) < self.money * self.leverage
            ):
                # 买入信号且资金充足
                entry_price = (entry_price * position + close * self.sizer) / (
                    position + self.sizer
                )
                position += self.sizer
                commission = self.comm * self.sizer * close
                value -= commission

            elif signal == -1:
                if position > self.sizer:
                    # 卖出信号且持仓量大于self.sizer，减仓
                    realized_pnl = (close - entry_price) * self.sizer
                    position -= self.sizer
                    commission = self.comm * self.sizer * close
                    value += realized_pnl - commission
                else:
                    # 卖出信号且持仓量小于self.sizer，全部平仓
                    realized_pnl = unrealized_pnl
                    commission = self.comm * position * close
                    value += unrealized_pnl - commission
                    entry_price = 0
                    position = 0

        elif position < 0:
            # 持有空头头寸
            unrealized_pnl = (close - entry_price) * position
            stop_loss = dema + std * self.sl_std
            take_profit = dema - std * self.tp_std

            if close < take_profit or close > stop_loss:
                # 超过止损止盈点全部平仓
                realized_pnl = unrealized_pnl
                commission = self.comm * position * close
                value += unrealized_pnl - commission
                entry_price = 0
                position = 0

            elif (
                signal == -1
                and entry_price * abs(position) < self.money * self.leverage
            ):
                # 卖出信号且资金充足
                entry_price = (entry_price * position - close * self.sizer) / (
                    position - self.sizer
                )
                position -= self.sizer
                commission = self.comm * self.sizer * close
                value -= commission

            elif signal == 1:
                if position < -self.sizer:
                    # 买入信号且持仓量大于self.sizer，减仓
                    realized_pnl = (close - entry_price) * self.sizer
                    position += self.sizer
                    commission = self.comm * self.sizer * close
                    value += realized_pnl - commission
                else:
                    # 买入信号且持仓量小于self.sizer，全部平仓
                    realized_pnl = unrealized_pnl
                    commission = self.comm * position * close
                    value += unrealized_pnl - commission
                    entry_price = 0
                    position = 0

        else:
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
        self.sizer = self._standarize_sizer(signal.close[0])
        value = self.money
        position = 0
        entry_price = 0

        for index, row in signal.iterrows():
            signal = row.signal
            close = row.close
            std = row["std"]
            dema = row.dema
            (
                value,
                signal,
                position,
                entry_price,
                unrealized_pnl,
                realized_pnl,
                commission,
            ) = self._strategy_run(
                value, signal, position, close, std, dema, entry_price
            )

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
