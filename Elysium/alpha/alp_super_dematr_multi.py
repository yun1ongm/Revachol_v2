import logging
import os
import time

import sys
temp_path = "/Users/rivachol/Desktop/Rivachol_v2/Elysium"
sys.path.append(temp_path)
from market.market_bot import MarketEngine
from alpha.idx_super_dema import IdxSuperDema
from alpha.stgy_dematr_multi import StgyDematrMulti

import warnings
warnings.filterwarnings("ignore")


class AlpSuperDematr:
    """
        Args:
            money (float): initial money
            leverage (float): leverage
            sizer (float): sizer
        
        Return:
            position and signal in portfolio: pd.DataFrame
    """
    alpha_name = "alp_super_dematr_multi"
    index_name = "idx_super_dema"
    strategy_name = "stgy_dematr_multi"
    symbol = "ETHUSDT"
    timeframe = "5m"

    sptr_len = 21
    sptr_k = 4
    dema_len = 24
    atr_f = 8
    atr_s = 21
    atr_profit = 6
    atr_loss = 5

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

    def main(self,interval = 15) -> None:
        market = MarketEngine(self.symbol, self.timeframe)
        while True:
            try:
                market.update_CKlines()
                index = IdxSuperDema(market.kdf, self.sptr_len, self.sptr_k, self.dema_len, self.atr_f)
                strategy = StgyDematrMulti(self.atr_profit, self.atr_loss, self.money, self.leverage, self.sizer)
                idx_signal = index.generate_dematr_signal(self.atr_f, self.atr_s)
                stgy_signal = strategy.generate_signal_position(idx_signal)
                self.position = stgy_signal[f"position_{self.strategy_name}"][-1]
                self._log_stgy_res(stgy_signal)
                time.sleep(interval)
                
            except Exception as e:
                self._log(e)
                time.sleep(interval / 2)

if __name__ == "__main__":
    alp = AlpSuperDematr(money = 500, leverage = 5, sizer = 0.1)
    alp.main()
    
