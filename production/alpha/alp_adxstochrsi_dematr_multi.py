import logging
import time
import pandas as pd

import sys
temp_path = "/Users/rivachol/Desktop/Rivachol_v2/"
sys.path.append(temp_path)
from research.backtest import BacktestFramework
from production.kline import KlineGenerator
from Index.idx_adxstochrsi import IdxAdxStochrsi
from production.strategy.stgy_dematr_multi import StgyDematrMulti
import contek_timbersaw as timbersaw
import warnings
warnings.filterwarnings("ignore")

class AlpAdxStochrsiDematrMulti(BacktestFramework):
    """
        Args:
            money (float): initial money
            leverage (float): leverage
            sizer (float): sizer
            params (dict): parameters for the alpha

        Return:
            position and signal in portfolio: pd.DataFrame
    """
    alpha_name = "alp_adxstochrsi_dematr_multi"
    index_name = "idx_adxstochrsi"
    strategy_name = "stgy_dematr_multi"
    symbol = "BTCUSDT"
    timeframe = "5m"
    logger = logging.getLogger(alpha_name)

    def __init__(self, money, leverage, params:dict) -> None:
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
    
    def _set_params(self, params:dict):
        self.adx_len = params["adx_len"]
        self.stoch_len = params["stoch_len"]
        self.rsi_len = params["rsi_len"]
        self.kd = params["kd"]
        self.dema_len = params["dema_len"]
        self.atr_profit = params["atr_profit"]
        self.atr_loss = params["atr_loss"]

    def generate_signal_position(self, kdf:pd.DataFrame) -> float:
        try:
            index = IdxAdxStochrsi(kdf, self.adx_len, self.stoch_len, self.rsi_len, self.kd, self.dema_len)
            strategy = StgyDematrMulti(self.atr_profit, self.atr_loss, self.money, self.leverage)
            idx_signal = index.generate_dematr_signal()
            update_time = idx_signal.index[-1]
            portfolio = strategy.generate_portfolio(idx_signal)
            position = portfolio[f"position"][-1]
            signal = portfolio[f"signal"][-1]
            entry_price = portfolio["entry_price"][-1]
            stop_profit = portfolio["stop_profit"][-1]
            stop_loss = portfolio["stop_loss"][-1]
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
    params = {'adx_len': 75, 'rsi_len': 51, 'stoch_len': 21, 'kd': 4, 'dema_len': 27, 'atr_profit': 2, 'atr_loss': 5}
    def live_trading(params):
        timbersaw.setup()
        alp = AlpAdxStochrsiDematrMulti(money = 1000, leverage = 5, params = params)
        market = KlineGenerator('BTCUSDT', '5m')
        while True:
            market.update_klines()
            alp.generate_signal_position(market.kdf)
            time.sleep(10)

    live_trading(params)
