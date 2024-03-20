# -*- coding: utf-8 -*-
import yaml
import logging
import warnings
warnings.filterwarnings("ignore")
import time
import sys
main_path = "/Users/rivachol/Desktop/Rivachol_v2/"
sys.path.append(main_path)
import contek_timbersaw as timbersaw
from Market.kline import KlineGenerator
from Alpha.alp_super_dematr_sing import AlpSuperDematrSing
from Alpha.alp_adxstochrsi_dematr_multi import AlpAdxStochrsiDematrMulti

class ModelUrban:
    """
    Model Urban is a model that combines multiple alphas to generate a single signal position
    Args:
        symbol: str: symbol of the trading pair
        timeframe: str: timeframe of the trading pair
    
    Attributes:
        alphas: list: list of alpha classes
        market: MarketEngine: market engine class
        signal_position: float: combined signal position from all alphas
    """
    model_name = "model_urban"
    logger = logging.getLogger(model_name)

    def __init__(self, symbol, timeframe) -> None:
        config = self._read_config()
        self.alphas = [
            AlpSuperDematrSing(money = 1000, leverage = 5, sizer = 0.02,
                           params = config["alpha_params"]["alp_super_dematr_sing"], mode = 1),
            AlpAdxStochrsiDematrMulti(money = 1000, leverage = 5, sizer = 0.01,
                                 params = config["alpha_params"]["alp_adx_stochrsi_dematr_multi"], mode = 1),
        ]
        self.market = KlineGenerator(symbol, timeframe)
        self.signal_position = None

    def _read_config(self, rel_path = "config.yaml") -> dict:
        try:
            with open(main_path + rel_path, 'r') as stream:
                config = yaml.safe_load(stream)
        except FileNotFoundError:
            self.logger.error('Config file not found')
            sys.exit(1)
        return config

    def merging_signal(self) -> None:
        """generate signals from each alpha and merge them into a single dataframe"""
        previous_signal_position = self.signal_position if self.signal_position is not None else 0
        merged_position = 0
        for alpha in self.alphas:
            signal_position = alpha.generate_signal_position(self.market.kdf)
            if signal_position:
                merged_position += signal_position["position"]
            else:
                self.logger.info(f"Alpha:{alpha.alpha_name} did not generate a signal")
        self.signal_position = round(merged_position,3)
        self._export_signal_position()
        if self.signal_position != previous_signal_position:
            change = self.signal_position - previous_signal_position
            model.logger.warning(f"Signal Position Change:{change}\n-- -- -- -- -- -- -- -- --")
        self.logger.info(f"Combined Signal Position:{self.signal_position}\n-- -- -- -- -- -- -- -- --")

    def _export_signal_position(self):
        """export signal position to a yaml file"""
        self.update_time = self.market.kdf.index[-1]
        with open(main_path + "signal_position.yaml", "w") as file:
            yaml.dump({"signal_position": str(self.signal_position),
                        "update_time":str(self.update_time)}, file)
            
    def run(self) -> None:
        while True:
            try:
                self.market.update_klines()
                self.merging_signal()
                time.sleep(10)
            except Exception as e:
                self.logger.exception(e)
                time.sleep(5)

if __name__ == "__main__":
    timbersaw.setup()
    model = ModelUrban(symbol="BTCUSDT", timeframe="5m")
    model.run()
