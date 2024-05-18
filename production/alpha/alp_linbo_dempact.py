import logging
import time
import pandas as pd

import sys
temp_path = "/Users/rivachol/Desktop/Rivachol_v2/"
sys.path.append(temp_path)
from research.backtest import BacktestFramework
from Index.idx_trendline import IdxTrendline
from research.Strategy.stgy_dempact import StgyDempact
from production.kline import KlineGenerator
import contek_timbersaw as timbersaw
import warnings
warnings.filterwarnings("ignore")

class AlpLinboDempact(BacktestFramework):
    """
        Args:
            money (float): initial money
            leverage (float): leverage
            params (dict): parameters for the alpha

        Return:
            position and signal in portfolio: pd.DataFrame
        
    """
    alpha_name = "alp_linbo_dempact"  
    index_name = "idx_trendline"
    strategy_name = "stgy_dempact"
    symbol = "BTCUSDT"
    timeframe = "5m"
    logger = logging.getLogger(alpha_name)

    def __init__(self, money, leverage, params:dict) -> None:
        self._set_params(params)
        self.money = money
        self.leverage = leverage
    
    def _set_params(self, params:dict):
        self.swing = params["swing"]
        self.reset = params["reset"]
        self.slope = params["slope"]    
        self.profit_pct = params["profit_pct"]
        self.loss_pct = params['loss_pct']

    def generate_signal_position(self, kdf:pd.DataFrame) -> dict:
        try:
            index = IdxTrendline(kdf, self.swing, self.reset, self.slope)
            strategy = StgyDempact(self.profit_pct, self.loss_pct, self.money, self.leverage)
            idx_signal = index.generate_dema_signal()
            update_time = idx_signal.index[-1]
            stgy_signal = strategy.generate_portfolio(idx_signal)
            position = stgy_signal[f"position"][-1]
            signal =  stgy_signal[f"signal"][-1]
            entry_price =  stgy_signal["entry_price"][-1]
            signal_position ={
                "position": position,
                "signal": signal,
                "entry_price": entry_price,
                "update_time": update_time
            }
            self.logger.info(f"{signal_position}")

            return signal_position
        except Exception as e:
            self.logger.exception(e)

if __name__ == "__main__":
    params = {'swing': 90, 'reset': 281, 'slope': 1.0, 'profit_pct': 0.038, 'loss_pct': 0.002}

    def live_trading(params):
        timbersaw.setup()
        alp = AlpLinboDempact(money = 1000, leverage = 5, params = params)
        market = KlineGenerator('BTCUSDT', '5m')
        while True:
            market.update_klines()
            alp.generate_signal_position(market.kdf)
            time.sleep(10)

    live_trading(params)
