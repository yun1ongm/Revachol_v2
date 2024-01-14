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
from alpha.alp_super_dematr import AlpSuperDematr
from alpha.alp_adx_stochrsi_dematr import AlpAdxStochrsiDematr
from alpha.alp_macd_dematr import AlpMacdDematr


warnings.filterwarnings("ignore")


class ModelUrban:
    model_name = "model_urban"

    def __init__(self, interval) -> None:
        self._init_logger()
        self.interval = interval
        self.alphas = [
            AlpSuperDematr(),
            AlpAdxStochrsiDematr(),
            AlpMacdDematr(),
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
        merged = pd.DataFrame()
        for alpha in self.alphas:
            index = alpha.gen_index_signal(self.market5m.kdf)
            signal = alpha.generate_signal_position(index)
            merged[alpha.alpha_name] = signal[f"position_{alpha.alpha_name}"]
            self._log(
                f"{alpha.alpha_name} Position:{signal[f'position_{alpha.alpha_name}'][-1]}"
            )
        merged["total_position"] = merged.sum(axis=1)
        total_position = round(merged["total_position"][-1], 2)
        self._log(
            f"Combined Signal Position:{total_position}\n-- -- -- -- -- -- -- -- --"
        )
        return total_position


if __name__ == "__main__":
    test = ModelUrban(10)
    res = test.merging_signal()
