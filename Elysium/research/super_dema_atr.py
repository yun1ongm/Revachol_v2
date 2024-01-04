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

warnings.filterwarnings("ignore")
sys.path.append("/Users/rivachol/Desktop/Elysium/research")
from cta_backtest import BacktestEngine


class Indicators:
    def supertrend(kdf, sptr_len, sptr_k):
        supertrend = ta.supertrend(
            kdf["high"], kdf["low"], kdf["close"], sptr_len, sptr_k
        )
        supertrend.columns = ["bound", "direction", "lbound", "ubound"]

        return supertrend[["bound", "direction"]]

    def dematr(kdf, atr_len, dema_len):
        atr = ta.atr(
            kdf["high"], kdf["low"], kdf["close"], length=atr_len, mamode="ema"
        )
        ema = ta.ema(atr, length=dema_len)
        dematr = pd.concat([atr, ema], axis=1)
        dematr.columns = ["atr_ema", "atr_dema"]
        dematr["Xvalue"] = np.where(
            (dematr["atr_ema"] > dematr["atr_dema"])
            & (dematr["atr_ema"].shift(1) < dematr["atr_dema"].shift(1)),
            dematr["atr_dema"],
            0,
        )
        dematr["Xvalue"] = np.where(
            (dematr["atr_ema"] < dematr["atr_dema"])
            & (dematr["atr_ema"].shift(1) > dematr["atr_dema"].shift(1)),
            dematr["atr_dema"],
            0,
        )
        dematr["Xvalue"] = dematr["Xvalue"].replace(0, method="ffill")
        return dematr


class AlpSuperDemAtr:
    alpha_name = "super_dema_atr"
    symbol = "ETHUSDT"
    timeframe = "5m"
    start = datetime(2023, 9, 24, 0, 0, 0)
    window_days = 100

    sptr_len = 29
    sptr_k = 3.5
    dema_len = 28
    atr_len = 17
    atr_k = 7
    wlr = 1.5

    def __init__(self) -> None:
        self.backtest = BacktestEngine(
            self.alpha_name, self.symbol, self.timeframe, self.start, self.window_days
        )

    def _gen_index_signal(self) -> pd.DataFrame:
        kdf = self.backtest.kdf
        supertrend = Indicators.supertrend(kdf, self.sptr_len, self.sptr_k)
        kdf_sig = pd.concat([kdf, supertrend], axis=1)
        kdf_sig["dema"] = ta.dema(kdf_sig["close"], length=self.dema_len)
        dematr = Indicators.dematr(kdf_sig, self.atr_len, self.dema_len)
        kdf_sig["atr"] = dematr["Xvalue"]
        kdf_sig["volume_ema"] = ta.ema(kdf_sig["volume_USDT"], length=self.atr_len)
        kdf_sig["signal"] = 0

        kdf_sig.loc[
            (kdf_sig["close"] > kdf_sig["dema"])
            & (kdf_sig["low"] <= kdf_sig["dema"])
            & (kdf_sig["direction"] == 1)
            & (kdf_sig["volume_USDT"] < kdf_sig["volume_ema"]),
            "signal",
        ] = 1

        kdf_sig.loc[
            (kdf_sig["close"] < kdf_sig["dema"])
            & (kdf_sig["high"] >= kdf_sig["dema"])
            & (kdf_sig["direction"] == -1)
            & (kdf_sig["volume_USDT"] < kdf_sig["volume_ema"]),
            "signal",
        ] = -1

        return kdf_sig[["high", "low", "close", "atr", "signal"]]

    def get_backtest_result(
        self, sptr_len, sptr_k, dema_len, atr_len, atr_k, wlr
    ) -> pd.DataFrame:
        self.sptr_len = sptr_len
        self.sptr_k = sptr_k
        self.dema_len = dema_len
        self.atr_len = atr_len
        self.atr_k = atr_k
        self.wlr = wlr

        kdf_sig = self._gen_index_signal()
        result = self.backtest.run_stretegy_atr(kdf_sig, atr_k, wlr)
        return result

    def evaluate_performance(self, result):
        perf = self.backtest.calc_performance(result)

        return perf

    def objective(self, trial):
        kwargs = {
            "sptr_len": trial.suggest_int("sptr_len", 6, 30),
            "sptr_k": trial.suggest_float("sptr_k", 1, 4, step=0.5),
            "dema_len": trial.suggest_int("dema_len", 6, 30),
            "atr_len": trial.suggest_int("atr_len", 6, 30),
            "atr_k": trial.suggest_float("atr_k", 2, 8, step=0.5),
            "wlr": trial.suggest_float("wlr", 1, 2, step=0.5),
        }
        result = self.get_backtest_result(**kwargs)
        performance = self.evaluate_performance(result)

        return performance[self.target]


class Optimizer(AlpSuperDemAtr):
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
    test = AlpSuperDemAtr()
    result = test.get_backtest_result(
        test.sptr_len,
        test.sptr_k,
        test.dema_len,
        test.atr_len,
        test.atr_k,
        test.wlr,
    )
    performance = test.evaluate_performance(result)
    print(performance)
    optimizer = Optimizer()
    best_params, best_value = optimizer.optimize_params()
    print("Best parameters:")
    print(best_params)
    print("Best value:")
    print(best_value)
