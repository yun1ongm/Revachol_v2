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
from cta_backtest import BacktestEngine

warnings.filterwarnings("ignore")


class Indicators:
    def supertrend(kdf, sptr_len, sptr_k):
        supertrend = ta.supertrend(
            kdf["high"], kdf["low"], kdf["close"], sptr_len, sptr_k
        )
        supertrend.columns = ["stop_price", "direction", "lbound", "ubound"]

        return supertrend[["stop_price", "direction"]]

    def wmatr(kdf, atr_len, mean_len):
        atr = ta.atr(
            kdf["high"], kdf["low"], kdf["close"], length=atr_len, mamode="ema"
        )
        mean = ta.wma(atr, length=mean_len)
        wmatr = pd.concat([atr, mean], axis=1)
        wmatr.columns = ["atr_mean", "atr"]
        wmatr["status"] = np.where(wmatr["atr"] > wmatr["atr_mean"], 1, 0)
        return wmatr


class AlpSuperDemaVol:
    alpha_name = "superdemavol"
    symbol = "ETHUSDT"
    timeframe = "5m"
    start = datetime(2023, 9, 20, 0, 0, 0)
    window_days = 100

    sptr_len = 21
    sptr_k = 4
    dema_len = 27
    vol_len = 19
    vol_k = 4.5

    def __init__(self) -> None:
        self.backtest = BacktestEngine(
            self.alpha_name, self.symbol, self.timeframe, self.start, self.window_days
        )

    def _gen_index_signal(self) -> pd.DataFrame:
        kdf = self.backtest.kdf
        supertrend = Indicators.supertrend(kdf, self.sptr_len, self.sptr_k)
        kdf_sig = pd.concat([kdf, supertrend], axis=1)
        kdf_sig["dema"] = ta.dema(kdf_sig["close"], length=self.dema_len)
        kdf_sig["volume_ema"] = ta.ema(kdf_sig["volume"], length=self.vol_len)
        kdf_sig["signal"] = 0

        kdf_sig.loc[
            (kdf_sig["close"] > kdf_sig["dema"])
            & (kdf_sig["low"] <= kdf_sig["dema"])
            & (kdf_sig["direction"] == 1)
            & (kdf_sig["volume"] < kdf_sig["volume_ema"]),
            "signal",
        ] = 1

        kdf_sig.loc[
            (kdf_sig["close"] < kdf_sig["dema"])
            & (kdf_sig["high"] >= kdf_sig["dema"])
            & (kdf_sig["direction"] == -1)
            & (kdf_sig["volume"] < kdf_sig["volume_ema"]),
            "signal",
        ] = -1

        return kdf_sig[["close", "volume", "volume_ema", "stop_price", "signal"]]

    def get_backtest_result(
        self, sptr_len, sptr_k, dema_len, vol_len, vol_k
    ) -> pd.DataFrame:
        self.sptr_len = sptr_len
        self.sptr_k = sptr_k
        self.dema_len = dema_len
        self.vol_len = vol_len
        self.vol_k = vol_k

        kdf_sig = self._gen_index_signal()
        result = self.backtest.run_stretegy_volume(kdf_sig, vol_k)
        return result

    def evaluate_performance(self, result):
        perf = self.backtest.calc_performance(result)

        return perf

    def objective(self, trial):
        kwargs = {
            "sptr_len": trial.suggest_int("sptr_len", 6, 30),
            "sptr_k": trial.suggest_float("sptr_k", 1, 5, step=0.5),
            "dema_len": trial.suggest_int("dema_len", 6, 30),
            "vol_len": trial.suggest_int("vol_len", 6, 30),
            "vol_k": trial.suggest_float("vol_k", 1, 5, step=0.5),
        }
        result = self.get_backtest_result(**kwargs)
        performance = self.evaluate_performance(result)

        return performance[self.target]


class Optimizer(AlpSuperDemaVol):
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
    test = AlpSuperDemaVol()
    result = test.get_backtest_result(
        test.sptr_len,
        test.sptr_k,
        test.dema_len,
        test.vol_len,
        test.vol_k,
    )
    performance = test.evaluate_performance(result)
    print(performance)
    optimizer = Optimizer()
    best_params, best_value = optimizer.optimize_params()
    print("Best parameters:")
    print(best_params)
    print("Best value:")
    print(best_value)
