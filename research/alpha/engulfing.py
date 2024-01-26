import os
import sys
import logging
import operator
import warnings
from datetime import datetime

import pandas as pd
import pandas_ta as ta
import numpy as np
import optuna

sys.path.append("/Users/rivachol/Desktop/Elysium/research")
from Elysium.research.strategy.cta_backtest import BacktestEngine

warnings.filterwarnings("ignore")


class Indicators:
    def engulfing(kdf, body_k):
        kdf_sig = kdf[["open", "high", "low", "close"]]
        kdf_sig["body"] = kdf_sig["close"] - kdf_sig["open"]
        kdf_sig["wick"] = kdf_sig["high"] - kdf_sig["low"]
        condition = abs(kdf_sig["body"]) > kdf_sig["wick"] * body_k
        kdf_sig["engulf_signal"] = np.where(
            condition
            & (kdf_sig["low"] < kdf_sig["low"].shift(1))
            & (kdf_sig["close"] > kdf_sig["high"].shift(1)),
            1,
            np.where(
                condition
                & (kdf_sig["high"] > kdf_sig["high"].shift(1))
                & (kdf_sig["close"] < kdf_sig["low"].shift(1)),
                -1,
                0,
            ),
        )

        return kdf_sig


class AlpEngulfingVol:
    alpha_name = "engulfing_vol"
    symbol = "ETHUSDT"
    timeframe = "5m"
    start = datetime(2023, 9, 24, 0, 0, 0)
    window_days = 100

    vol_len = 16
    vol_k = 3
    body_k = 0.8

    def __init__(self) -> None:
        self.backtest = BacktestEngine(
            self.alpha_name, self.symbol, self.timeframe, self.start, self.window_days
        )

    def _gen_index_signal(self) -> pd.DataFrame:
        kdf = self.backtest.kdf
        kdf_sig = Indicators.engulfing(kdf, self.body_k)
        kdf_sig["volume_USDT"] = kdf["volume_USDT"]
        kdf_sig["volume_ema"] = ta.ema(kdf_sig["volume_USDT"], length=self.vol_len)
        kdf_sig["stop_price"] = np.where(
            kdf_sig["engulf_signal"] == 1,
            kdf_sig["low"],
            np.where(kdf_sig["engulf_signal"] == -1, kdf_sig["high"], 0),
        )
        # 将stop_price为0的值填充为前一行的值
        kdf_sig["stop_price"] = kdf_sig["stop_price"].replace(0, method="ffill")
        kdf_sig["signal"] = 0

        kdf_sig.loc[
            (kdf_sig["volume_USDT"] > kdf_sig["volume_ema"] * self.vol_k)
            & (kdf_sig["engulf_signal"] == 1),
            "signal",
        ] = 1

        kdf_sig.loc[
            (kdf_sig["volume_USDT"] > kdf_sig["volume_ema"] * self.vol_k)
            & (kdf_sig["engulf_signal"] == -1),
            "signal",
        ] = -1

        return kdf_sig[["close", "volume_USDT", "volume_ema", "stop_price", "signal"]]

    def get_backtest_result(self, vol_len, vol_k, body_k) -> pd.DataFrame:
        self.vol_len = vol_len
        self.vol_k = vol_k
        self.body_k = body_k

        kdf_sig = self._gen_index_signal()
        result = self.backtest.run_stretegy_volume(kdf_sig, vol_k)
        return result

    def evaluate_performance(self, result):
        perf = self.backtest.calc_performance(result)

        return perf

    def objective(self, trial):
        kwargs = {
            "vol_len": trial.suggest_int("vol_len", 8, 30),
            "vol_k": trial.suggest_float("vol_k", 1, 5, step=0.5),
            "body_k": trial.suggest_float("body_k", 0.5, 0.95, step=0.05),
        }

        result = self.get_backtest_result(**kwargs)
        performance = self.evaluate_performance(result)

        return performance[self.target]


class Optimizer(AlpEngulfingVol):
    num_evals = 100
    target = "diysharpe"
    print_log = True

    def __init__(self):
        super().__init__()
        self.end_date = self.backtest.kdf.index[-1].strftime("%Y-%m-%d")
        self._init_logger()
        self._log(
            f"Start optimizing {self.alpha_name} on {self.symbol} {self.timeframe} from {self.start} to {self.end_date}"
        )

    def _init_logger(self) -> None:
        self.logger = logging.getLogger(self.alpha_name)
        self.logger.setLevel(logging.INFO)
        log_file = f"study_log/{self.end_date}_{self.alpha_name}.log"
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

        self._log(log_message)


if __name__ == "__main__":
    test = AlpEngulfingVol()
    result = test.get_backtest_result(test.vol_len, test.vol_k, test.body_k)
    performance = test.evaluate_performance(result)
    print(performance)
    optimizer = Optimizer()
    best_params, best_value = optimizer.optimize_params()
    print("Best parameters:")
    print(best_params)
    print("Best value:")
    print(best_value)
