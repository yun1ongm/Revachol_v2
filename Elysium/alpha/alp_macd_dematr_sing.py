import logging
import os
import time
import pandas as pd
import sys
temp_path = "/Users/rivachol/Desktop/Rivachol_v2/Elysium"
sys.path.append(temp_path)
from market.market_bot import MarketEngine
from alpha.idx_macd_trend import IdxMacdTrend
from alpha.stgy_dematr_sing import StgyDematrSing

import warnings
warnings.filterwarnings("ignore")

class AlpMacdDematr:
    """
        Args:
            money (float): initial money
            leverage (float): leverage
            sizer (float): sizer

        Return:
            position and signal in portfolio: pd.DataFrame
        
    """
    alpha_name = "alp_macd_dematr"
    index_name = "idx_macd_trend"
    strategy_name = "stgy_dematr_sing"
    symbol = "ETHUSDT"
    timeframe = "5m"

    fast = 12
    slow = 40
    signaling = 12
    threshold = 0.4
    dema_len = 29
    atr_profit = 5
    atr_loss = 4

    logger = logging.getLogger(alpha_name)

    def __init__(self, money, leverage, sizer) -> None:
        self.money = money
        self.leverage = leverage
        self.sizer = sizer

    def _log_stgy_res(self, port_info) -> None:
        self.logger.info(f'position {port_info[f"position_{self.strategy_name}"][-1]}')
        self.logger.info(f'signal {port_info[f"signal_{self.strategy_name}"][-1]}')
        self.logger.info(f'entry_price {round(port_info["entry_price"][-1],2)}')
        self.logger.info(f'stop_profit {round(port_info["stop_profit"][-1],2)}')
        self.logger.info(f'stop_loss {round(port_info["stop_loss"][-1],2)}\n------------------')

    def generate_signal_position(self, kdf:pd.DataFrame) -> float:
        try:
            index = IdxMacdTrend(kdf, self.fast, self.slow, self.signaling, self.threshold, self.dema_len)
            strategy = StgyDematrSing(self.atr_profit, self.atr_loss, self.money, self.leverage, self.sizer)
            idx_signal = index.generate_dematr_signal()
            stgy_signal = strategy.generate_signal_position(idx_signal)
            position = stgy_signal[f"position_{self.strategy_name}"][-1]
            self._log_stgy_res(stgy_signal)
            return position
        except Exception as e:
            self.logger.exception(e)
            return None

if __name__ == "__main__":
    alp = AlpMacdDematr(money = 500, leverage = 5, sizer = 0.1)
    market = MarketEngine(alp.symbol, alp.timeframe)
    while True:
        market.update_CKlines()
        alp.generate_signal_position(market.kdf)
        time.sleep(15)
