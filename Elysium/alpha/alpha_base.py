import pandas as pd
import numpy as np
import logging
import os


class DematrCalculator:
    """
    alpha_name: str
    sizer: int
    atr_profit: float
    atr_loss: float
    """

    def __init__(self, alpha_name, sizer, atr_profit, atr_loss):
        self.alpha_name = alpha_name
        self.sizer = sizer
        self.atr_profit = atr_profit
        self.atr_loss = atr_loss
        self._init_logger()

    def _init_logger(self) -> None:
        self.logger = logging.getLogger(self.alpha_name)
        self.logger.setLevel(logging.INFO)
        log_file = f"log_book/{self.alpha_name}.log"
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s, %(message)s")
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

    def _log(self, string) -> None:
        self.logger.info(string)

    def _initialize_portfolio_variables(self, kdf: pd.DataFrame) -> pd.DataFrame:
        portfolio = kdf[["high", "low", "close"]]
        portfolio[f"position_{self.alpha_name}"] = np.zeros(len(portfolio))
        portfolio[f"signal_{self.alpha_name}"] = np.zeros(len(portfolio))
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
                position = 0
            elif signal == -1:
                entry_price = close
                position = -self.sizer

        elif position < 0:
            stop_loss = dema + atr * self.atr_loss
            stop_profit = dema - atr * self.atr_profit
            if high > stop_loss or low < stop_profit:
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
        port_info.loc[index, f"position_{self.alpha_name}"] = position
        port_info.loc[index, f"signal_{self.alpha_name}"] = signal
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
        self._log(f'position {port_info[f"position_{self.alpha_name}"][-1]}')
        self._log(f'signal {port_info[f"signal_{self.alpha_name}"][-1]}')
        self._log(f'entry_price {round(port_info["entry_price"][-1],2)}')
        self._log(f'stop_profit {round(port_info["stop_profit"][-1],2)}')
        self._log(f'stop_loss {round(port_info["stop_loss"][-1],2)}')
        return port_info[[f"position_{self.alpha_name}", f"signal_{self.alpha_name}"]]


class VolCalculator:
    """
    alpha_name: str
    sizer: int
    vol_k: float
    """

    def __init__(self, alpha_name, sizer, vol_k):
        self.alpha_name = alpha_name
        self.sizer = sizer
        self.vol_k = vol_k
        self._init_logger()

    def _init_logger(self) -> None:
        self.logger = logging.getLogger(self.alpha_name)
        self.logger.setLevel(logging.INFO)
        log_file = f"log_book/{self.alpha_name}.log"
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s, %(message)s")
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

    def _log(self, string) -> None:
        self.logger.info(string)

    def _initialize_portfolio_variables(self, kdf: pd.DataFrame) -> pd.DataFrame:
        portfolio = kdf[["close", "volume"]]
        portfolio[f"position_{self.alpha_name}"] = np.zeros(len(portfolio))
        portfolio[f"signal_{self.alpha_name}"] = np.zeros(len(portfolio))
        portfolio["entry_price"] = np.zeros(len(portfolio))
        portfolio["stop_price"] = np.zeros(len(portfolio))

        return portfolio

    def _calculate_position(
        self,
        signal,
        close,
        volume,
        volume_ema,
        entry_price,
        stop_price,
        position,
    ) -> tuple:
        if position > 0:
            if close <= stop_price or (volume > self.vol_k * volume_ema):
                position = 0
                entry_price = 0
                stop_price = 0
            elif signal == -1:
                position = -self.sizer
                entry_price = close
                stop_price = stop_price

        elif position < 0:
            if close >= stop_price or (volume > self.vol_k * volume_ema):
                position = 0
                entry_price = 0
                stop_price = 0
            elif signal == 1:
                position = self.sizer
                entry_price = close
                stop_price = stop_price
        else:
            entry_price = 0
            stop_price = 0
            position = 0

            if signal == 1:
                position = self.sizer
                entry_price = close
                stop_price = stop_price

            elif signal == -1:
                position = -self.sizer
                entry_price = close
                stop_price = stop_price

        return entry_price, stop_price, position

    def _record_current_values(
        self, port_info, index, signal, entry_price, stop_price, position
    ) -> pd.DataFrame:
        port_info.loc[index, f"position_{self.alpha_name}"] = position
        port_info.loc[index, f"signal_{self.alpha_name}"] = signal
        port_info.loc[index, "entry_price"] = entry_price
        port_info.loc[index, "stop_price"] = stop_price

        return port_info

    def generate_signal_position(self, index_signal: pd.DataFrame) -> pd.DataFrame:
        port_info = self._initialize_portfolio_variables(index_signal)
        position = 0
        entry_price = 0

        for index, row in index_signal.iterrows():
            signal = row.signal
            close = row.close
            volume = row.volume
            volume_ema = row.volume_ema
            stop_price = row.stop_price
            entry_price, stop_price, position = self._calculate_position(
                signal,
                close,
                volume,
                volume_ema,
                entry_price,
                stop_price,
                position,
            )

            port_info = self._record_current_values(
                port_info,
                index,
                signal,
                entry_price,
                stop_price,
                position,
            )
        self._log(f'position {port_info[f"position_{self.alpha_name}"][-1]}')
        self._log(f'signal {port_info[f"signal_{self.alpha_name}"][-1]}')
        self._log(f'entry_price {round(port_info["entry_price"][-1],2)}')
        self._log(f'stop_price {round(port_info["stop_price"][-1],2)}')
        return port_info[[f"position_{self.alpha_name}", f"signal_{self.alpha_name}"]]
