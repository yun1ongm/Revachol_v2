import logging
import time
import pandas as pd
import sys
temp_path = "/Users/rivachol/Desktop/Rivachol_v2/"
sys.path.append(temp_path)
from Elysium.market.market_bot import MarketEngine
from Elysium.alpha.idx_adx_stochrsi import IdxAdxStochrsi
from Elysium.alpha.stgy_dematr_multi import StgyDematrMulti
import contek_timbersaw as timbersaw
import warnings
warnings.filterwarnings("ignore")

class AlpAdxStochrsiDematr:
    """
        Args:
            money (float): initial money
            leverage (float): leverage
            sizer (float): sizer
            params (dict): parameters for the alpha

        Return:
            position and signal in portfolio: pd.DataFrame
    """
    alpha_name = "alp_adx_stochrsi_dematr"
    index_name = "idx_adx_stochrsi"
    strategy_name = "stgy_dematr_multi"

    logger = logging.getLogger(alpha_name)

    def __init__(self, money, leverage, sizer, params:dict) -> None:
        self.money = money
        self.leverage = leverage
        self.sizer = sizer
        self._set_params(params)
    
    def _set_params(self, params:dict):
        self.adx_len = params["adx_len"]
        self.rsi_len = params["rsi_len"]
        self.kd = params["kd"]
        self.dema_len = params["dema_len"]
        self.atr_len = params["atr_len"]
        self.atr_profit = params["atr_profit"]
        self.atr_loss = params["atr_loss"]

    def generate_signal_position(self, kdf:pd.DataFrame) -> float:
        try:
            index = IdxAdxStochrsi(kdf, self.adx_len, self.rsi_len, self.kd, self.dema_len, self.atr_len)
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
    timbersaw.setup()
    params = {'adx_len': 24, 'rsi_len': 21, 'kd': 5, 'dema_len': 33, 'atr_len': 27, 'atr_profit': 3, 'atr_loss': 4}
    alp = AlpAdxStochrsiDematr(money = 500, leverage = 5, sizer = 0.1, params = params)
    market = MarketEngine('BTCUSDT', '5m')
    while True:
        market.update_CKlines()
        alp.generate_signal_position(market.kdf)
        time.sleep(10)

