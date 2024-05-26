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
from Index.idx_adxstochrsi import IdxAdxStochrsi
from production.strategy.stgy_openatr import StgyOpenAtr
import contek_timbersaw as timbersaw

class AlpAdxStochrsiOpenatr(BacktestFramework):
    """
        Args:
            money (float): initial money
            leverage (float): leverage
            params (dict): parameters for the alpha

        Return:
            position and signal in portfolio: pd.DataFrame
    """
    alpha_name = "alp_adx_stochrsi_openatr"
    index_name = "idx_adx_stochrsi"
    strategy_name = "stgy_openatr"
    symbol = "BTCUSDT"
    timeframe = "1m"
    logger = logging.getLogger(alpha_name)

    def __init__(self, money, leverage, params:dict) -> None:
        self._set_params(params)
        self.money = money
        self.leverage = leverage

    def _set_params(self, params:dict):
        self.adx_len = params["adx_len"]
        self.stoch_len = params["stoch_len"]
        self.rsi_len = params["rsi_len"]
        self.kd = params["kd"]
        self.atr_profit = params["atr_profit"]
        self.atr_loss = params["atr_loss"]
        self.vol_len = params['vol_len']

    def generate_signal_position(self, kdf:pd.DataFrame) -> float:
        try:
            index = IdxAdxStochrsi(kdf, self.adx_len, self.stoch_len, self.rsi_len, self.kd)
            strategy = StgyOpenAtr(self.atr_profit, self.atr_loss, self.money, self.leverage)
            idx_signal = index.generate_atr_signal(self.vol_len)
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
            if signal_position["signal"] != 0:
                self.logger.info(f"trigger condition: {idx_signal.iloc[-3:]}")
            if signal_position["position"] != 0:
                self.logger.info(f"{signal_position}")

            return signal_position
        except Exception as e:
            self.logger.exception(e)

if __name__ == "__main__":
    timbersaw.setup()
    rel_path = "/production/config.yaml"
    with open(main_path + rel_path, 'r') as stream:
        config = yaml.safe_load(stream)
        params = config["alpha_params"]["alp_adx_stochrsi_openatr"]
    alp = AlpAdxStochrsiOpenatr(money = 1000, leverage = 5, params = params)
    market = KlineGenerator('BTCUSDT', '1m')
    while True:
        market.update_klines()
        alp.generate_signal_position(market.kdf)
        time.sleep(10)