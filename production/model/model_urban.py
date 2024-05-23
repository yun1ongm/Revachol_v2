import sys
main_path = "/Users/rivachol/Desktop/Rivachol_v2"
sys.path.append(main_path)
import warnings
warnings.filterwarnings("ignore")

# -*- coding: utf-8 -*-
import yaml
import logging
import time
from datetime import datetime, timedelta
import requests
import json
import contek_timbersaw as timbersaw
from production.kline import KlineGenerator
from production.alpha.alp_adx_stochrsi_openatr import AlpAdxStochrsiOpenatr
from production.alpha.alp_super_openatr  import AlpSuperOpenatr
from production.alpha.alp_linbo_dempact import AlpLinboDempact

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

    def __init__(self, symbol:list, timeframe) -> None:
        self._read_config()
        self.alphas = [
            AlpAdxStochrsiOpenatr(money = 1000, leverage = 5,
                                 params = self.config["alpha_params"]["alp_adx_stochrsi_openatr"]),
            AlpSuperOpenatr(money = 1000, leverage = 5,
                            params = self.config["alpha_params"]["alp_super_openatr"]),
            AlpLinboDempact(money = 1000, leverage = 5,
                            params = self.config["alpha_params"]["alp_linbo_dempact"])       
        ]
        self.market = KlineGenerator(symbol, timeframe)
        self.previous_model_position = 0
        self.model_position = 0
        self._export_signal_position()
        self.interval = 10

    def _read_config(self, rel_path = "/production/config.yaml") -> None:
        try:
            with open(main_path + rel_path, 'r') as stream:
                self.config = yaml.safe_load(stream)
        except FileNotFoundError:
            self.logger.error('Config file not found')
            sys.exit(1)
    
    def calculate_alpha(self) -> float:
        merged_position = 0
        for alpha in self.alphas:
            signal_position = alpha.generate_signal_position(self.market.kdf)
            if signal_position:
                merged_position += signal_position["position"]
            else:
                self.logger.info(f"Alpha:{alpha.alpha_name} did not generate a signal")
                self.market.push_discord({"content": f"Alpha:{alpha.alpha_name} did not generate a signal"})
        return round(merged_position,3)

    def merging_signal(self) -> None:
        self.previous_model_position = self.model_position
        self.model_position = self.calculate_alpha()
        if self.model_position != self.previous_model_position:
            change = self.model_position - self.previous_model_position
            model.logger.warning(f"Signal Position Change:{change}\n-- -- -- -- -- -- -- -- --")
            self.market.push_discord({"content": f"Signal Position Change:{change}\n-- -- -- -- -- -- -- -- --"})
            self._export_signal_position()
        self.logger.info(f"{self.model_name} Position:{self.model_position}\n-- -- -- -- -- -- -- -- --")

    def _export_signal_position(self):
        """export signal position to a yaml file"""
        export_path = "/production/signal_position.yaml"
        with open(main_path + export_path, "w") as file:
            model_signal = {
                self.model_name:
                    {"update_time":str(self.market.kdf.index[-1]),
                    "model_position": str(self.model_position)},
                    }
            self.market.push_discord({"content": f"Model signal position: {self.model_position}, update_time: {self.market.kdf.index[-1]}\n-- -- -- -- -- -- -- -- --"})
            yaml.dump(model_signal, file)
            
    def _countdown_update(self) -> bool:
        """check if the time of the candle matches the current time"""
        now = datetime.utcnow().replace(second=0, microsecond=0)
        candle_time = self.market.kdf.index[-1].replace(second=0, microsecond=0)
        if now - timedelta(minutes=self.market.timeframe_int) < candle_time:
            return True
        else:
            return False
            
    def run(self) -> None:
        while True:
            if self._countdown_update():
                time.sleep(self.interval)
            else:
                flag = self.market.update_klines()
                if flag:
                    self.merging_signal()
                    time.sleep(self.interval)
                else:
                    time.sleep(self.interval)


if __name__ == "__main__":
    timbersaw.setup()
    model = ModelUrban(symbol="BTCUSDT", timeframe="1m")
    model.run()
