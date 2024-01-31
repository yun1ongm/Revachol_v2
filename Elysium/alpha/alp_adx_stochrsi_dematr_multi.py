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
    logger = logging.getLogger(alpha_name)


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

    def _info_stgy_res(self, port_info) -> None:
        self.logger.info(f'position {port_info[f"position_{self.strategy_name}"][-1]}')
        self.logger.info(f'signal {port_info[f"signal_{self.strategy_name}"][-1]}')
        self.logger.info(f'entry_price {round(port_info["entry_price"][-1],2)}')
        self.logger.info(f'stop_profit {round(port_info["stop_profit"][-1],2)}')
        self.logger.info(f'stop_loss {round(port_info["stop_loss"][-1],2)}\n------------------')

    def generate_signal_position(self, kdf:pd.DataFrame) -> float:
        try:
            index = IdxAdxStochrsi(kdf, self.adx_len, self.rsi_len, self.kd, self.dema_len, self.atr_f, self.atr_s)
            strategy = StgyDematrMulti(self.atr_profit, self.atr_loss, self.money, self.leverage, self.sizer)
            idx_signal = index.generate_dematr_signal()
            stgy_signal = strategy.generate_signal_position(idx_signal)
            position = stgy_signal[f"position_{self.strategy_name}"][-1]
            self._info_stgy_res(stgy_signal)
            return position

        except Exception as e:
            self.logger.exception(e)
            return None

if __name__ == "__main__":
    alp = AlpAdxStochrsiDematr(money = 500, leverage = 5, sizer = 0.1)
    market = MarketEngine(alp.symbol, alp.timeframe)
    while True:
        market.update_CKlines()
        alp.generate_signal_position(market.kdf)
        time.sleep(15)

