import os
import logging
import operator
import warnings
from datetime import datetime

import pandas as pd
import pandas_ta as ta
import numpy as np
import optuna
import sys
sys.path.append("/Users/rivachol/Desktop/Rivachol_v2")
from research.strategy.bodyatr_multi import StgyBodyatrMulti

warnings.filterwarnings("ignore")


class Indicators:
    def triple_ma(kdf, short, mid, long) -> pd.DataFrame:
        '''this function aims to generate the signal of triple ma
        Args:
        kdf: pd.DataFrame
        short: int
        mid: int
        long: int
        Returns:
        pd.DataFrame
        '''
        tma = kdf[["high", "low", "close"]]
        tma['short_ema'] = ta.ema(kdf["close"], length=short)
        tma['mid_ema'] = ta.ema(kdf["close"], length=mid)
        tma['long_ema'] = ta.ema(kdf["close"], length=long)
        #均线多头排列
        ema_up_strong = (tma['short_ema'] > tma['mid_ema']) & (tma['mid_ema'] > tma['long_ema'])
        #均线空头排列
        ema_down_strong = (tma['short_ema'] < tma['mid_ema']) & (tma['mid_ema'] < tma['long_ema'])
        #均线回升排列
        ema_up_weak = (tma['short_ema'] > tma['mid_ema']) & (tma['mid_ema'] < tma['long_ema'])
        #均线回踩排列
        ema_down_weak = (tma['short_ema'] < tma['mid_ema']) & (tma['mid_ema'] > tma['long_ema'])
        #K线上穿两根均线
        condition1 = (tma["low"] < tma['mid_ema']) & (tma['low'] < tma['short_ema']) & (tma['close'] > tma['mid_ema']) & (tma['close'] > tma['short_ema'])
        #K线下穿两根均线
        condition2 = (tma["high"] > tma['mid_ema']) & (tma['high'] > tma['short_ema']) & (tma['close'] < tma['mid_ema']) & (tma['close'] < tma['short_ema'])
        tma["open_pos"] = np.where(condition1 & ema_up_strong, 1, 
                                     np.where(condition2 & ema_down_strong, -1, 
                                              np.where(condition1 & ema_up_weak, 0.5, 
                                                       np.where(condition2 & ema_down_weak, -0.5, 0))))
        tma['stop_price'] = np.where(condition1 & ema_up_strong, tma['long_ema'], 
                                     np.where(condition2 & ema_down_strong, tma['long_ema'], 
                                              np.where(condition1 & ema_up_weak, tma['low'].rolling(8).min(), 
                                                       np.where(condition2 & ema_down_weak,tma['high'].rolling(8).max(), 0))))
        tma['stop_price'] = tma['stop_price'].replace(0, method='ffill')
        return tma[['open_pos', 'stop_price']]
    
    def bodyatr(kdf, atr_len):
        batr = kdf[["high", "low", "close"]]
        batr["atr"] = ta.atr(batr["high"], batr["low"], batr["close"], atr_len)
        batr['body'] = batr['high'] - batr['low']
        batr['bodyatr'] = batr['body'] / batr['atr']
        return batr[['bodyatr', 'atr']]

class AlpTriplemaBodyatrMulti:
    alpha_name = "alp_triplema_bodyatr_multi"
    index_name = "triplema"
    strategy_name = "bodyatr_multi"
    symbol = "BTCUSDT"
    timeframe = "5m"
    start = datetime(2023, 11, 22, 0, 0, 0)
    window_days = 100

    logger = logging.getLogger(alpha_name)

    def __init__(self, money, leverage, sizer, params:dict, mode = 0) -> None:
        '''initialize the parameters
        Args:
        money: float
        leverage: float
        sizer: float
        params: dict
        mode: int (0 for backtest, 1 for live trading)
        '''

        self._set_params(params)
        if mode == 0:
            self.strategy = StgyBodyatrMulti(
                self.alpha_name, self.symbol, self.timeframe, self.start, self.window_days
            )
            
        else:
            self.money = money
            self.leverage = leverage
            self.sizer = sizer
    
    def _set_params(self, params:dict):
        self.short = params["short"]
        self.mid = params["mid"]
        self.long = params["long"]
        self.atr_len = params["atr_len"]
        self.harvest_ratio = params["harvest_ratio"]
        self.retreat_ratio = params["retreat_ratio"]

    def generate_signal_position(self, kdf:pd.DataFrame) -> pd.DataFrame:
        tma = Indicators.triple_ma(kdf, self.short, self.mid, self.long)
        bodyatr = Indicators.bodyatr(kdf, self.atr_len)
        kdf_sig = pd.concat([kdf, tma, bodyatr], axis=1)
        kdf_sig['volume_ema'] = ta.ema(kdf_sig["volume"], length=self.atr_len)
        kdf_sig["signal"] = kdf_sig["open_pos"]
        kdf_sig.loc[
            (kdf_sig["volume"] > kdf_sig["volume_ema"]),
            "signal",
        ] = 0

        return kdf_sig[["close", "stop_price", "bodyatr", "signal"]]

    def get_backtest_result(self, short, mid, long, atr_len, harvest_ratio, retreat_ratio) -> pd.DataFrame:
        self.short = short
        self.mid = mid
        self.long = long
        self.atr_len = atr_len
        self.harvest_ratio = harvest_ratio
        self.retreat_ratio = retreat_ratio

        kdf_sig = self.generate_signal_position(self.strategy.kdf)
        result = self.strategy.run(kdf_sig, harvest_ratio, retreat_ratio)
        self.output_result(result)
        return result
    
    def output_result(self, result:pd.DataFrame) -> None:
        os.makedirs("result_book", exist_ok=True)
        start_date = self.strategy.kdf.index[0].strftime("%Y-%m-%d")
        end_date = self.strategy.kdf.index[-1].strftime("%Y-%m-%d")
        result.to_csv(f"result_book/{self.alpha_name}_{start_date}to{end_date}.csv")

    def evaluate_performance(self, result):
        perf = self.strategy.calc_performance(result)

        return perf

    def objective(self, trial):
        kwargs = {
            "short": trial.suggest_int("short", 10, 20, step=2),
            "mid": trial.suggest_int("mid", 30, 90, step=5),
            "long": trial.suggest_int("long", 100, 200, step=10),
            "atr_len": trial.suggest_int("atr_len", 10, 60, step=2),
            "harvest_ratio": trial.suggest_float("harvest_ratio", 1, 2.4, step=0.2),
            "retreat_ratio": trial.suggest_float("retreat_ratio", 1, 2.4, step=0.2),
        }

        result = self.get_backtest_result(**kwargs)
        performance = self.evaluate_performance(result)

        return performance[self.target]


class Optimizer(AlpTriplemaBodyatrMulti):
    num_evals = 100
    target = "t_sharpe"
    print_log = True

    def __init__(self, money, leverage, sizer, params:dict):
        super().__init__(money, leverage, sizer, params)
        self.start_date = self.strategy.kdf.index[0].strftime("%Y-%m-%d")
        self.end_date = self.strategy.kdf.index[-1].strftime("%Y-%m-%d")
        self._init_logger()
        result = self.get_backtest_result(**params)
        performance = self.evaluate_performance(result)
        self._log(performance)
        self._log(
            f"Start optimizing {self.alpha_name} for goal {self.target} on {self.symbol} {self.timeframe} from {self.start_date} to {self.end_date}"
        )

    def _init_logger(self) -> None:
        self.logger = logging.getLogger(self.alpha_name)
        self.logger.setLevel(logging.INFO)
        log_file = f"study_log/{self.alpha_name}_{self.start_date}to{self.end_date}.log"
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s, %(message)s")
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

    def _log(self, string) -> None:
        if not self.print_log:
            print("this will not print log!")
        else:
            self.logger.info(string)

    def optimize_params(self):
        study = optuna.create_study(direction="maximize")
        study.optimize(self.objective, n_trials=self.num_evals)
        sorted_trials = sorted(
            [trial for trial in study.trials if trial.value is not None],
            key=operator.attrgetter("value"),
            reverse=True,
        )
        top_10_trials = sorted_trials[:10]
        self._write_to_log(top_10_trials)

        return study.best_params, study.best_value

    def _write_to_log(self, trials):
        log_message = "Top 10 results:\n"
        for i, trial in enumerate(trials):
            log_message += f"Rank {i+1}:\n"
            log_message += f"  Params: {trial.params}\n"
            log_message += f"  Value: {trial.value}\n\n"
            result = self.get_backtest_result(**trial.params)
            performance = self.evaluate_performance(result)
            log_message += f"  Performance: {performance}\n\n"

        self._log(log_message)

if __name__ == "__main__":
    optimizer = Optimizer(money = 1000, leverage = 5, sizer = 0.01, params = {
        "short": 20,
        "mid": 60,
        "long": 200,
        "atr_len": 30,
        "harvest_ratio": 2,
        "retreat_ratio": 2,
    
    })
    best_params, best_value = optimizer.optimize_params()
    print("Best parameters:")
    print(best_params)
    print("Best value:")
    print(best_value)
