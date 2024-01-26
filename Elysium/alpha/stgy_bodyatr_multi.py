import pandas as pd
import numpy as np

class StgyBodyatrMulti:
    """
        Args:
            kdf_signal (pd.DataFrame): dataframe with klines and signal
            upbody_ratio (float): upbody ratio
            downbody_ratio (float): downbody ratio
            money (float): initial money
            leverage (float): leverage
            sizer (float): sizer

        Return:

    """
    strategy_name = "stgy_bodyatr_multi"

    def __init__(self, upbody_ratio, downbody_ratio,money,leverage,sizer) -> None:
        self.upbody_ratio = upbody_ratio
        self.downbody_ratio = downbody_ratio
        self.money = money
        self.leverage = leverage
        self.sizer = sizer


    def _initialize_portfolio_variables(self, index_signal: pd.DataFrame) -> pd.DataFrame:
        portfolio = index_signal[["close", "stop_price", "bodyatr"]]
        portfolio[f"position_{self.strategy_name}"] = np.zeros(len(portfolio))
        portfolio[f"signal_{self.strategy_name}"] = np.zeros(len(portfolio))
        portfolio["entry_price"] = np.zeros(len(portfolio))
        
        return portfolio

    def _calculate_position(
        self,
        signal,
        close,
        stop_price,
        bodyatr,
        entry_price,
        position,
    ) -> tuple:
        if position > 0:
            money_thresh = (entry_price * position < self.money * self.leverage)
            if close < stop_price or abs(bodyatr) > self.upbody_ratio:
                entry_price = 0
                stop_price = None  
                position = 0
            elif signal == 1 and money_thresh:
                entry_price = (entry_price * position + close * self.sizer)/(position + self.sizer)
                position += self.sizer
            elif signal == -1:
                if position > self.sizer:
                    position += -self.sizer
                else:
                    entry_price = 0
                    stop_price = None
                    position = 0
        elif position < 0:
            money_thresh = (entry_price * -position < self.money * self.leverage)
            if close > stop_price or abs(bodyatr) > self.downbody_ratio:
                entry_price = 0
                stop_price = None
                position = 0
            elif signal == -1 and money_thresh:
                entry_price = (entry_price * position - close * self.sizer) / (position - self.sizer)
                position += -self.sizer
            elif signal == 1:
                if position < -self.sizer:
                    position += self.sizer
                else:
                    entry_price = 0
                    stop_price = None
                    position = 0

        else:
            stop_price = None
            if signal == 1:
                entry_price = close
                position += self.sizer

            elif signal == -1:
                entry_price = close
                position += -self.sizer

        return position, entry_price, stop_price

    def _record_current_values(
        self, port_info, index, signal, position, entry_price, stop_price
    ) -> pd.DataFrame:
        port_info.loc[index, f"signal_{self.strategy_name}"] = round(signal,2)
        port_info.loc[index, f"position_{self.strategy_name}"] = round(position,2)
        port_info.loc[index, "entry_price"] = round(entry_price,2)
        port_info.loc[index, "stop_price"] = stop_price

        return port_info

    def generate_signal_position(self, index_signal: pd.DataFrame) -> pd.DataFrame:
        port_info = self._initialize_portfolio_variables(index_signal)
        position = 0
        entry_price = 0

        for index, row in index_signal.iterrows():
            signal = row.signal
            close = row.close
            stop_price = row.stop_price
            bodyatr = row.bodyatr

            position, entry_price, stop_price = self._calculate_position(signal, close, stop_price, bodyatr, entry_price, position)

            port_info = self._record_current_values(
                port_info,
                index,
                signal,
                position,
                entry_price,
                stop_price,
            )

        return port_info[[f"position_{self.strategy_name}", f"signal_{self.strategy_name}", "entry_price", "stop_price"]]
