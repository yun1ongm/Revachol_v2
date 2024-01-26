# -*- coding: utf-8 -*-

import logging
import os
import warnings
warnings.filterwarnings("ignore")
import pandas as pd
import sys
temp_path = "/Users/rivachol/Desktop/Rivachol_v2/Elysium"
sys.path.append(temp_path)
from market.market_bot import MarketEngine
from alpha.alp_super_dema_bodyatr_multi import AlpSuperDemaBodyatr
from alpha.alp_adx_stochrsi_dematr_multi import AlpAdxStochrsiDematr
from alpha.alp_macd_dematr_sing import AlpMacdDematr


warnings.filterwarnings("ignore")


class ModelUrban:
    model_name = "model_urban"

    def __init__(self, interval) -> None:
        self._init_logger()
        self.interval = interval
        self.alphas = [
            AlpSuperDemaBodyatr(money = 500, leverage = 5, sizer = 0.1),
            AlpAdxStochrsiDematr(money = 500, leverage = 5, sizer = 0.2),
            AlpMacdDematr(money = 500, leverage = 5, sizer = 0.3),
        ]
        self.market5m = MarketEngine("ETHUSDT", "5m")

    def _init_logger(self) -> None:
        self.logger = logging.getLogger(self.model_name)
        self.logger.setLevel(logging.INFO)
        log_file = f"log_book/{self.model_name}.log"
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s, %(message)s")
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

    def _log(self, string) -> None:
        self.logger.info(string)

    def merging_signal(self) -> float:
        """generate signals from each alpha and merge them into a single dataframe"""
        merged_position = 0
        self.market5m.update_CKlines()
        for alpha in self.alphas:
            position = alpha.generate_signal_position(self.market5m.kdf)
            merged_position += position
            self._log(
                f"{alpha.alpha_name} Position:{position}"
            )

        self._log(
            f"Combined Signal Position:{merged_position}\n-- -- -- -- -- -- -- -- --"
        )
        return merged_position

if __name__ == "__main__":
    test = ModelUrban(10)
    res = test.merging_signal()
