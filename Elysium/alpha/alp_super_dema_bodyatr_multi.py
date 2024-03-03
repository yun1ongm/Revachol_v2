import logging
import time
import pandas as pd
import sys
temp_path = "/Users/rivachol/Desktop/Rivachol_v2/Elysium"
sys.path.append(temp_path)
from market.market_bot import MarketEngine
from alpha.idx_super_dema import IdxSuperDema
from alpha.stgy_bodyatr_multi import StgyBodyatrMulti

import warnings
warnings.filterwarnings("ignore")

class AlpSuperDemaBodyatr:
    """
        Args:
            money (float): initial money
            leverage (float): leverage
            sizer (float): sizer

        Return:
            position and signal in portfolio: pd.DataFrame
        
    """
    alpha_name = "alp_super_dema_bodyatr"
    index_name = "idx_super_dema"
    strategy_name = "stgy_bodyatr_multi"


    sptr_len = 18
    sptr_k = 2.5
    dema_len = 60
    atr_f = 9
    atr_s = 26
    harvest_ratio = 2.4
    retreat_ratio = 1

    logger = logging.getLogger(alpha_name)

    def __init__(self, money, leverage, sizer) -> None:
        self.money = money
        self.leverage = leverage
        self.sizer = sizer

    def generate_signal_position(self, kdf:pd.DataFrame) -> dict:
        try:
            index = IdxSuperDema(kdf, self.sptr_len, self.sptr_k, self.dema_len, self.atr_f, self.atr_s)
            strategy = StgyBodyatrMulti(self.harvest_ratio, self.retreat_ratio, self.money, self.leverage, self.sizer)
            idx_signal = index.generate_bodyatr_signal()
            update_time = idx_signal.index[-1]
            stgy_signal = strategy.generate_signal_position(idx_signal)
            position = stgy_signal[f"position_{self.strategy_name}"][-1]
            signal = stgy_signal[f"signal_{self.strategy_name}"][-1]
            entry_price = stgy_signal["entry_price"][-1]
            stop_price = stgy_signal["stop_price"][-1]
            signal_position ={
                "position": position,
                "signal": signal,
                "entry_price": entry_price,
                "stop_price": stop_price,
                "update_time": update_time
            }
            self.logger.info(f"{signal_position}")

            return signal_position
        except Exception as e:
            self.logger.exception(e)

if __name__ == "__main__":
    import contek_timbersaw as timbersaw
    timbersaw.setup()
    alp = AlpSuperDemaBodyatr(money = 500, leverage = 5, sizer = 0.1)
    market = MarketEngine('BTCUSDT', '5m')
    while True:
        market.update_CKlines()
        position = alp.generate_signal_position(market.kdf)
        time.sleep(10)