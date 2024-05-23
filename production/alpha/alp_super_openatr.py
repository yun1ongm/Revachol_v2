import sys
main_path = "/Users/rivachol/Desktop/Rivachol_v2/"
sys.path.append(main_path)
import warnings
warnings.filterwarnings("ignore")

import logging
import time
import pandas as pd
import yaml
from research.backtest import BacktestFramework
from production.kline import KlineGenerator
from Index.idx_supertrend import IdxSupertrend
from production.strategy.stgy_openatr import StgyOpenAtr
import contek_timbersaw as timbersaw


class AlpSuperOpenatr(BacktestFramework):
    """
        Args:
            money (float): initial money
            leverage (float): leverage
            params (dict): parameters for the alpha
        
        Return:
            position and signal in portfolio: pd.DataFrame
    """
    alpha_name = "alp_super_openatr"
    index_name = "idx_supertrend"
    strategy_name = "stgy_openatr"
    symbol = "BTCUSDT"
    timeframe = "1m"
    logger = logging.getLogger(alpha_name)

    def __init__(self, money, leverage, params:dict) -> None:
        self._set_params(params)
        self.money = money
        self.leverage = leverage

    def _set_params(self, params:dict):
        self.sptr_len = params["sptr_len"]
        self.sptr_k = params["sptr_k"]
        self.vol_len = params['vol_len']
        self.atr_profit = params["atr_profit"]
        self.atr_loss = params["atr_loss"]

    def generate_signal_position(self, kdf:pd.DataFrame) -> float:
        try:
            index = IdxSupertrend(kdf, self.sptr_len, self.sptr_k)
            strategy = StgyOpenAtr(self.atr_profit, self.atr_loss, self.money, self.leverage)
            idx_signal = index.generate_atr_signal(self.vol_len)
            update_time = idx_signal.index[-1]
            stgy_signal = strategy.generate_portfolio(idx_signal)
            position = stgy_signal["position"][-1]
            signal = stgy_signal["signal"][-1]
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
    timbersaw.setup()
    rel_path = "/production/config.yaml"
    with open(main_path + rel_path, 'r') as stream:
        config = yaml.safe_load(stream)
        params = config["alpha_params"]["alp_super_openatr"]
    alp = AlpSuperOpenatr(money = 1000, leverage = 5, params = params)
    market = KlineGenerator('BTCUSDT', '1m')
    while True:
        market.update_klines()
        alp.generate_signal_position(market.kdf)
        time.sleep(10)

    
