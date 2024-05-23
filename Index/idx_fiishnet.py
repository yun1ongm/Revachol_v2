import pandas as pd
import pandas_ta as pta
import numpy as np

class IdxFishnet:
    index_name = "idx_fishnet"
    params = {'atr_len': 9, 'thd': 0.1}
    logger = logging.getLogger(executor)

    def __init__(self, symbol, timeframe) -> None:
        super().__init__(symbol)
        self.timeframe = timeframe
        self.interval = 3
        if self.symbol == "BTCUSDC":
            self.digit = 1
            min_sizer = 0.002
        if self.symbol == "ETHUSDC":
            self.digit = 2
            min_sizer = 0.007
        else:
            self.digit = 3
            min_sizer = 0.005

        self.lot = min_sizer
        self.max_position = self.params['thd'] * self.equity * self.leverage
        self.levels = self.calc_levels()

    def calc_levels(self) -> dict:
        kdf = self.get_candle()
        atr = pta.atr(kdf["high"], kdf["low"], kdf["close"], length = self.params['atr_len'], mamode = "EMA")
        highest = kdf["high"].max()
        lowest = kdf["low"].min()
        self.grid = 2 * atr.mean()
        mid_pivot = (highest + lowest) / 2
        current_price = kdf["close"][-1]
        if current_price > mid_pivot:
            self.curr_level = mid_pivot + ((current_price - mid_pivot) // self.grid + 1) * self.grid
            upper_level1 = self.curr_level + self.grid
            lower_level1 = self.curr_level - self.grid
            upper_level2 = upper_level1 + self.grid
            lower_level2 = lower_level1 - self.grid
        
        else:
            self.curr_level = mid_pivot - ((mid_pivot - current_price) // self.grid + 1) * self.grid
            upper_level1 = self.curr_level + self.grid
            lower_level1 = self.curr_level - self.grid
            upper_level2 = upper_level1 + self.grid
            lower_level2 = lower_level1 - self.grid
        return {
            "curr_level": self.curr_level,
            "upper_level1": upper_level1,
            "lower_level1": lower_level1,
            "upper_level2": upper_level2,
            "lower_level2": lower_level2
        }

    def update_levels(self) -> bool:
        kdf = self.get_newest_candle()
        high = kdf.high[-1]
        low = kdf.low[-1]
        close = kdf.close[-1]
        if high >= self.levels["upper_level1"]:
            if high >= self.levels["upper_level2"]:
                # if the upper level2 is penetrated, refresh the curr_level based on the current price
                self.levels["curr_level"] = close
                self.levels['upper_level1'] = self.levels["curr_level"] + self.grid
                self.levels['lower_level1'] = self.levels["curr_level"] - self.grid
                self.levels['upper_level2'] = self.levels["curr_level"] + 2 * self.grid
                self.levels['lower_level2'] = self.levels["curr_level"] - 2 * self.grid
                self.logger.warning(f"upper_level2 is penetrated. Levels are updated.")
            else:
                self.levels['curr_level'] = self.levels["upper_level1"]
                self.levels['upper_level1'] = self.levels['curr_level'] + self.grid
                self.levels['lower_level1'] = self.levels['curr_level'] - self.grid
                self.levels['upper_level2'] = self.levels['curr_level'] + 2 * self.grid
                self.levels['lower_level2'] = self.levels['curr_level'] - 2 * self.grid
                self.logger.info(f"Levels are updated.")
            cancle_batch_orders()
            send_batch_order()
            return True
            
        elif low <= self.levels["lower_level1"]:
            if low <= self.levels["lower_level2"]:
                self.levels["curr_level"] = close
                self.levels['upper_level1'] = self.levels["curr_level"] + self.grid
                self.levels['lower_level1'] = self.levels["curr_level"] - self.grid
                self.levels['upper_level2'] = self.levels["curr_level"] + 2 * self.grid
                self.levels['lower_level2'] = self.levels["curr_level"] - 2 * self.grid
                self.logger.warning(f"level2 is penetrated. Levels are updated.")
            else:
                self.levels['curr_level'] = self.levels["lower_level1"]
                self.levels['upper_level1'] = self.levels['curr_level'] + self.grid
                self.levels['lower_level1'] = self.levels['curr_level'] - self.grid
                self.levels['upper_level2'] = self.levels['curr_level'] + 2 * self.grid
                self.levels['lower_level2'] = self.levels['curr_level'] - 2 * self.grid  
                self.logger.info(f"Levels are updated.")
            cancle_batch_orders()
            send_batch_order()
            return True
        else:
            self.logger.info(f"Levels are not updated.")
            return False    
    