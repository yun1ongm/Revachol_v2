import logging
import time
import pandas as pd
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

    sptr_len = 14
    sptr_k = 2.5
    dema_len = 57
    atr_f = 14
    atr_s = 29
    atr_profit = 2
    atr_loss = 3

    logger = logging.getLogger(alpha_name)

    def __init__(self, money, leverage, sizer) -> None:
        self.money = money
        self.leverage = leverage
        self.sizer = sizer


    def generate_signal_position(self, kdf:pd.DataFrame) -> dict:
        try:
            index = IdxSuperDema(kdf, self.sptr_len, self.sptr_k, self.dema_len, self.atr_f, self.atr_s)
            strategy = StgyDematrMulti(self.atr_profit, self.atr_loss, self.money, self.leverage, self.sizer)
            idx_signal = index.generate_dematr_signal()
            update_time = idx_signal.index[-1]
            stgy_signal = strategy.generate_signal_position(idx_signal)
            position = stgy_signal[f"position_{self.strategy_name}"][-1]
            signal = stgy_signal[f"signal_{self.strategy_name}"][-1]
            entry_price = stgy_signal["entry_price"][-1]
            stop_loss = stgy_signal["stop_loss"][-1]
            stop_profit = stgy_signal["stop_profit"][-1]
            signal_position ={
                "position": position,
                "signal": signal,
                "entry_price": entry_price,
                "stop_loss": stop_loss,
                "stop_profit": stop_profit,
                "update_time": update_time
            }
            self.logger.info(f"{signal_position}")
                
            return signal_position
        except Exception as e:
            self.logger.exception(e)


if __name__ == "__main__":
    import contek_timbersaw as timbersaw
    timbersaw.setup()
    alp = AlpSuperDematr(money = 1000, leverage = 5, sizer = 0.2)
    market = MarketEngine('BTCUSDT', '5m')
    while True:
        market.update_CKlines()
        position = alp.generate_signal_position(market.kdf)
        time.sleep(10)
    
