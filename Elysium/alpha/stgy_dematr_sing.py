import pandas as pd
import numpy as np


class StgyDematrSing:
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

    def _initialize_portfolio_variables(self, index_signal: pd.DataFrame) -> pd.DataFrame:
        portfolio = index_signal[["high", "low", "close"]]
        portfolio[f"position_{self.strategy_name}"] = np.zeros(len(portfolio))
        portfolio[f"signal_{self.strategy_name}"] = np.zeros(len(portfolio))
        portfolio["entry_price"] = np.zeros(len(portfolio))
        portfolio["stop_loss"] = np.zeros(len(portfolio))
        portfolio["stop_profit"] = np.zeros(len(portfolio))

        return portfolio

    def _calculate_position(
        self,
        signal,
        close,
        high,
        low,
        atr,
        dema,
        entry_price,
        position,
    ) -> tuple:
        if position > 0:
            stop_loss = dema - atr * self.atr_loss
            stop_profit = dema + atr * self.atr_profit
            if low < stop_loss or high > stop_profit:
                entry_price = 0
                position = 0
            elif signal == -1:
                entry_price = close
                position = -self.sizer

        elif position < 0:
            stop_loss = dema + atr * self.atr_loss
            stop_profit = dema - atr * self.atr_profit
            if high > stop_loss or low < stop_profit:
                entry_price = 0
                position = 0
            elif signal == 1:
                entry_price = close
                position = self.sizer

        else:
            entry_price = 0
            stop_loss = 0
            stop_profit = 0

            if signal == 1:
                entry_price = close
                position = self.sizer

            elif signal == -1:
                entry_price = close
                position = -self.sizer

        return entry_price, stop_profit, stop_loss, position

    def _record_current_values(
        self,
        port_info,
        index,
        position,
        signal,
        entry_price,
        stop_loss,
        stop_profit,
    ) -> pd.DataFrame:
        port_info.loc[index, f"position_{self.strategy_name}"] = round(position,2)
        port_info.loc[index, f"signal_{self.strategy_name}"] = signal
        port_info.loc[index, "entry_price"] = entry_price
        port_info.loc[index, "stop_loss"] = stop_loss
        port_info.loc[index, "stop_profit"] = stop_profit

        return port_info

    def generate_signal_position(self, index_signal: pd.DataFrame) -> pd.DataFrame:
        port_info = self._initialize_portfolio_variables(index_signal)
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
            entry_price, stop_profit, stop_loss, position = self._calculate_position(
                signal,
                close,
                high,
                low,
                atr,
                dema,
                entry_price,
                position,
            )
            port_info = self._record_current_values(
                port_info,
                index,
                position,
                signal,
                entry_price,
                stop_loss,
                stop_profit,
            )
        return port_info[[f"position_{self.strategy_name}", f"signal_{self.strategy_name}", "entry_price", "stop_loss", "stop_profit"]]
        

