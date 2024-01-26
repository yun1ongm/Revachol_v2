import logging
import os
import time
import pandas as pd
import sys
temp_path = "/Users/rivachol/Desktop/Rivachol_v2/Elysium"
sys.path.append(temp_path)
from market.market_bot import MarketEngine
from alpha.idx_adx_stochrsi import IdxAdxStochrsi
from alpha.stgy_dematr_multi import StgyDematrMulti

import warnings
warnings.filterwarnings("ignore")


class AlpAdxStochrsiDematr:
    """
        Args:
            money (float): initial money
            leverage (float): leverage
            sizer (float): sizer

        Return:
            position and signal in portfolio: pd.DataFrame
    """
    alpha_name = "alp_adx_stochrsi_dematr"
    index_name = "idx_adx_stochrsi"
    strategy_name = "stgy_dematr_multi"
    symbol = "ETHUSDT"
    timeframe = "5m"


    adx_len = 30
    rsi_len = 9
    kd = 7
    dema_len = 14
    atr_f = 8
    atr_s = 16
    atr_profit = 3
    atr_loss = 4

    def __init__(self, money, leverage, sizer) -> None:
        self.money = money
        self.leverage = leverage
        self.sizer = sizer
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

    def _log_stgy_res(self, port_info) -> None:
        self._log(f'position {port_info[f"position_{self.strategy_name}"][-1]}')
        self._log(f'signal {port_info[f"signal_{self.strategy_name}"][-1]}')
        self._log(f'entry_price {round(port_info["entry_price"][-1],2)}')
        self._log(f'stop_profit {round(port_info["stop_profit"][-1],2)}')
        self._log(f'stop_loss {round(port_info["stop_loss"][-1],2)}')

    def generate_signal_position(self, kdf:pd.DataFrame) -> float:
        try:
            index = IdxAdxStochrsi(kdf, self.adx_len, self.rsi_len, self.kd, self.dema_len, self.atr_f, self.atr_s)
            strategy = StgyDematrMulti(self.atr_profit, self.atr_loss, self.money, self.leverage, self.sizer)
            idx_signal = index.generate_dematr_signal()
            stgy_signal = strategy.generate_signal_position(idx_signal)
            position = stgy_signal[f"position_{self.strategy_name}"][-1]
            self._log_stgy_res(stgy_signal)
            return position

        except Exception as e:
            self._log(e)
            return None

if __name__ == "__main__":
    alp = AlpAdxStochrsiDematr(money = 500, leverage = 5, sizer = 0.1)
    market = MarketEngine(alp.symbol, alp.timeframe)
    while True:
        market.update_CKlines()
        alp.generate_signal_position(market.kdf)
        time.sleep(15)

