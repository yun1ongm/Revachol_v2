# -*- coding: utf-8 -*-

import logging
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
    logger = logging.getLogger(model_name)

    def __init__(self, interval) -> None:
        self.interval = interval
        self.alphas = [
            AlpSuperDemaBodyatr(money = 500, leverage = 5, sizer = 0.1),
            AlpAdxStochrsiDematr(money = 500, leverage = 5, sizer = 0.2),
            AlpMacdDematr(money = 500, leverage = 5, sizer = 0.3),
        ]
        self.market5m = MarketEngine("ETHUSDT", "5m")

    def merging_signal(self) -> float:
        """generate signals from each alpha and merge them into a single dataframe"""
        merged_position = 0
        self.market5m.update_CKlines()
        for alpha in self.alphas:
            position = alpha.generate_signal_position(self.market5m.kdf)
            merged_position += position
        self.logger.info(
            f"Combined Signal Position:{merged_position}\n-- -- -- -- -- -- -- -- --"
        )
        return round(merged_position,2)

if __name__ == "__main__":
    test = ModelUrban(10)
    res = test.merging_signal()
