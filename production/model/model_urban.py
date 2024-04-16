# -*- coding: utf-8 -*-
import yaml
import logging
import warnings
warnings.filterwarnings("ignore")
import time
from datetime import datetime, timedelta
import sys
main_path = "/Users/rivachol/Desktop/Rivachol_v2"
sys.path.append(main_path)
import contek_timbersaw as timbersaw
from production.kline import KlineGenerator
from production.alpha.alp_macd_dema import AlpMacdDema
from production.alpha.alp_adxstochrsi_dematr_multi import AlpAdxStochrsiDematrMulti

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
        config = self._read_config()
        self.alphas = [
            AlpSuperDematrSing(money = 500, leverage = 5,
                           params = config["alpha_params"]["alp_super_dematr_sing"], mode = 1),
            AlpAdxStochrsiDematrMulti(money = 500, leverage = 5,
                                 params = config["alpha_params"]["alp_adx_stochrsi_dematr_multi"], mode = 1),
        ]
        self.market = KlineGenerator(symbol, timeframe, mode = 1)
        self.signal_position = self.calculate_alpha()
        self._export_signal_position()
        self.interval = 10

    def _read_config(self, rel_path = "/config.yaml") -> dict:
        try:
            with open(main_path + rel_path, 'r') as stream:
                config = yaml.safe_load(stream)
        except FileNotFoundError:
            self.logger.error('Config file not found')
            sys.exit(1)
        return config
    
    def calculate_alpha(self) -> float:
        merged_position = 0
        for alpha in self.alphas:
            signal_position = alpha.generate_signal_position(self.market.kdf)
            if signal_position:
                merged_position += signal_position["position"]
            else:
                self.logger.info(f"Alpha:{alpha.alpha_name} did not generate a signal")
        return round(merged_position,3)

    def merging_signal(self) -> None:
        """generate signals from each alpha and merge them into a single dataframe"""
        previous_signal_position = self.signal_position if self.signal_position is not None else 0
        self.signal_position = self.calculate_alpha()
        if self.signal_position != previous_signal_position:
            change = self.signal_position - previous_signal_position
            model.logger.warning(f"Signal Position Change:{change}\n-- -- -- -- -- -- -- -- --")
            self._export_signal_position()
        self.logger.info(f"Combined Signal Position:{self.signal_position}\n-- -- -- -- -- -- -- -- --")

    def _export_signal_position(self):
        """export signal position to a yaml file"""
        self.update_time = self.market.kdf.index[-1]
        export_path = "/production/signal_position.yaml"
        with open(main_path + export_path, "w") as file:
            yaml.dump({"signal_position": str(self.signal_position),
                        "update_time":str(self.update_time)}, file)
            
    def _check_time_match(self,timeframe = 5) -> bool:
        """check if the time of the candle matches the current time"""
        now = datetime.utcnow().replace(second=0, microsecond=0)
        candle_time = self.market.kdf.index[-1].replace(second=0, microsecond=0)
        if now - timedelta(minutes=timeframe) < candle_time:
            return True
        else:
            return False
            
    def run(self) -> None:
        while True:
            if self._check_time_match():
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
    model = ModelUrban(symbol="BTCUSDT", timeframe="5m")
    model.run()
