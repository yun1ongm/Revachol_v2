import logging
import time
import pandas as pd
import sys
temp_path = "/Users/rivachol/Desktop/Rivachol_v2/"
sys.path.append(temp_path)
from Research.backtest import BacktestFramework
from Market.kline import KlineGenerator
from Index.idx_macd_trend import IdxMacdTrend
from Strategy.stgy_dema import StgyDema
import contek_timbersaw as timbersaw
import warnings
warnings.filterwarnings("ignore")

class AlpMacdDema(BacktestFramework):
    """
        Args:
            money (float): initial money
            leverage (float): leverage
            sizer (float): sizer

        Return:
            position and signal in portfolio: pd.DataFrame
        
    """
    alpha_name = "alp_macd_dema"
    index_name = "idx_macd_trend"
    strategy_name = "stgy_dema"
    symbol = "BTCUSDT"
    timeframe = "5m"
    logger = logging.getLogger(alpha_name)

    def __init__(self, money, leverage, sizer, params:dict) -> None:
        '''initialize the parameters
        Args:
        money: float
        leverage: float
        sizer: float
        params: dict
        mode: int (0 for backtest, 1 for live trading)
        '''
        self._set_params(params)

        self.money = money
        self.leverage = leverage
        self.sizer = sizer

    def _set_params(self, params:dict) -> None:
        '''set the parameters
        Args:
        params: dict
        '''
        self.fast = params["fast"]
        self.slow = params["slow"]
        self.signaling = params["signaling"]
        self.threshold = params["threshold"]
        self.dema_len = params["dema_len"]

    def generate_signal_position(self, kdf:pd.DataFrame) -> dict:
        try:
            index = IdxMacdTrend(kdf, self.fast, self.slow, self.signaling, self.threshold, self.dema_len)
            strategy = StgyDema(self.money, self.leverage, self.sizer)
            idx_signal = index.generate_dematr_signal()
            update_time = idx_signal.index[-1]
            portfolio = strategy.generate_portfolio(idx_signal)
            position = portfolio[f"position"][-1]
            signal = portfolio[f"signal"][-1]
            entry_price = portfolio["entry_price"][-1]
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
            return None

if __name__ == "__main__":
    params = {'fast': 11, 'slow': 21, 'signaling': 9, 'threshold': 0.3, 'dema_len': 100}
    def live_trading(params):
        timbersaw.setup()
        alp = AlpMacdDema(money = 1000, leverage = 5, sizer = 0.1, params = params)
        market = KlineGenerator('BTCUSDT', '5m')
        while True:
            market.update_klines()
            alp.generate_signal_position(market.kdf)
            time.sleep(10)

    live_trading(params)


