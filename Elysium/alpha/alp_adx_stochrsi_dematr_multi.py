import logging
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

    logger = logging.getLogger(alpha_name)

    def __init__(self, money, leverage, sizer) -> None:
        self.money = money
        self.leverage = leverage
        self.sizer = sizer

    def generate_signal_position(self, kdf:pd.DataFrame) -> float:
        try:
            index = IdxAdxStochrsi(kdf, self.adx_len, self.rsi_len, self.kd, self.dema_len, self.atr_f, self.atr_s)
            strategy = StgyDematrMulti(self.atr_profit, self.atr_loss, self.money, self.leverage, self.sizer)
            idx_signal = index.generate_dematr_signal()
            update_time = idx_signal.index[-1]
            stgy_signal = strategy.generate_signal_position(idx_signal)
            position = stgy_signal[f"position_{self.strategy_name}"][-1]
            signal = stgy_signal[f"signal_{self.strategy_name}"][-1]
            entry_price = stgy_signal["entry_price"][-1]
            stop_profit = stgy_signal["stop_profit"][-1]
            stop_loss = stgy_signal["stop_loss"][-1]
            signal_position ={
                "position": position,
                "signal": signal,
                "entry_price": entry_price,
                "stop_profit": stop_profit,
                "stop_loss": stop_loss,
                "update_time": update_time
            }
            self.logger.info(f"{signal_position}")

            return signal_position
        except Exception as e:
            self.logger.exception(e)

if __name__ == "__main__":
    import contek_timbersaw as timbersaw
    timbersaw.setup()
    alp = AlpAdxStochrsiDematr(money = 500, leverage = 5, sizer = 0.1)
    market = MarketEngine(alp.symbol, alp.timeframe)
    while True:
        market.update_CKlines()
        alp.generate_signal_position(market.kdf)
        time.sleep(10)

