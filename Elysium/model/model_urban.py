# -*- coding: utf-8 -*-

import logging
import warnings
warnings.filterwarnings("ignore")
import time
import sys
temp_path = "/Users/rivachol/Desktop/Rivachol_v2/Elysium"
sys.path.append(temp_path)
from market.market_bot import MarketEngine
from alpha.alp_super_dema_bodyatr_multi import AlpSuperDemaBodyatr
from alpha.alp_adx_stochrsi_dematr_sing import AlpAdxStochrsiDematr
from alpha.alp_macd_dematr_multi import AlpMacdDematr


class ModelUrban:
    model_name = "model_urban"
    logger = logging.getLogger(model_name)

    def __init__(self) -> None:
        self.alphas = [
            AlpSuperDemaBodyatr(money = 1000, leverage = 5, sizer = 0.2),
            AlpAdxStochrsiDematr(money = 500, leverage = 5, sizer = 0.2),
            AlpMacdDematr(money = 500, leverage = 5, sizer = 0.1),
        ]
        self.market5m = MarketEngine("ETHUSDT", "5m")
        self.signal_position = None

    def merging_signal(self) -> None:
        """generate signals from each alpha and merge them into a single dataframe"""
        merged_position = 0
        for alpha in self.alphas:
            signal_position = alpha.generate_signal_position(self.market5m.kdf)
            if signal_position:
                merged_position += signal_position["position"]
            else:
                self.logger.info(f"Alpha:{alpha.alpha_name} did not generate a signal")
        self.signal_position = round(merged_position,2)
        self.logger.info(
            f"Combined Signal Position:{self.signal_position}\n-- -- -- -- -- -- -- -- --"
        )


if __name__ == "__main__":
    import contek_timbersaw as timbersaw
    timbersaw.setup()
    model = ModelUrban()
    def calc_signal_position(model):
        while True:
            try:
                previous_signal_position = model.signal_position if model.signal_position is not None else 0
                model.market5m.update_CKlines()
                model.merging_signal()
                if model.signal_position != previous_signal_position:
                    change = model.signal_position - previous_signal_position
                    model.logger.warning(
                        f"Signal Position Change:{change}\n-- -- -- -- -- -- -- -- --"
                    )
                time.sleep(10)
            except Exception as e:
                model.logger.exception(e)
                time.sleep(5)
    calc_signal_position(model)