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

    def double_atr(kdf, atr_f, atr_s):
        atr_fast = ta.atr(
            kdf["high"], kdf["low"], kdf["close"], length=atr_f, mamode="ema"
        )
        atr_slow = ta.atr(
            kdf["high"], kdf["low"], kdf["close"], length=atr_s, mamode="ema"
        )
        datr = pd.concat([atr_fast, atr_slow], axis=1)
        datr.columns = ["atr_fast", "atr_slow"]
        datr["Xvalue"] = np.where(
            (datr["atr_fast"] > datr["atr_slow"])
            & (datr["atr_fast"].shift(1) < datr["atr_slow"].shift(1)),
            datr["atr_slow"],
            np.where(
                (datr["atr_fast"] < datr["atr_slow"])
                & (datr["atr_fast"].shift(1) > datr["atr_slow"].shift(1)),
                datr["atr_slow"],
                0,
            ),
        )
        # 如果datr["Xvalue"]为0，那么填充为前一行的值
        datr["Xvalue"] = datr["Xvalue"].replace(0, method="ffill")
        return datr


class AlpSuperDemAtr:
    alpha_name = "super_dematr"
    symbol = "ETHUSDT"
    timeframe = "5m"
    start = datetime(2023, 10, 1, 0, 0, 0)
    window_days = 100

    sptr_len = 21
    sptr_k = 4
    dema_len = 24
    atr_f = 8
    atr_s = 21
    atr_profit = 6
    atr_loss = 5

    def __init__(self) -> None:
        self.backtest = BacktestEngine(
            self.alpha_name, self.symbol, self.timeframe, self.start, self.window_days
        )

    def _gen_index_signal(self) -> pd.DataFrame:
        kdf = self.backtest.kdf
        supertrend = Indicators.supertrend(kdf, self.sptr_len, self.sptr_k)
        kdf_sig = pd.concat([kdf, supertrend], axis=1)
        kdf_sig["dema"] = ta.dema(kdf_sig["close"], length=self.dema_len)
        datr = Indicators.double_atr(kdf_sig, self.atr_f, self.atr_s)
        kdf_sig["atr"] = datr["Xvalue"]
        kdf_sig["volume_ema"] = ta.ema(kdf_sig["volume_USDT"], length=self.atr_f)
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

        return kdf_sig[["high", "low", "close", "atr", "signal", "dema"]]

    def get_backtest_result(
        self, sptr_len, sptr_k, dema_len, atr_f, atr_s, atr_profit, atr_loss
    ) -> pd.DataFrame:
        self.sptr_len = sptr_len
        self.sptr_k = sptr_k
        self.dema_len = dema_len
        self.atr_f = atr_f
        self.atr_s = atr_s
        self.atr_profit = atr_profit
        self.atr_loss = atr_loss

        kdf_sig = self._gen_index_signal()
        result = self.backtest.run_stretegy_dematr(kdf_sig, atr_profit, atr_loss)
        return result

    def evaluate_performance(self, result):
        perf = self.backtest.calc_performance(result)

        return perf

    def objective(self, trial):
        kwargs = {
            "sptr_len": trial.suggest_int("sptr_len", 15, 30),
            "sptr_k": trial.suggest_float("sptr_k", 2, 4, step=0.5),
            "dema_len": trial.suggest_int("dema_len", 12, 50),
            "atr_f": trial.suggest_int("atr_f", 6, 15),
            "atr_s": trial.suggest_int("atr_s", 15, 30),
            "atr_profit": trial.suggest_int("atr_profit", 2, 12),
            "atr_loss": trial.suggest_int("atr_loss", 1, 6),
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
            result = self.get_backtest_result(**trial.params)
            performance = self.evaluate_performance(result)
            log_message += f"  Performance: {performance}\n\n"

        self._log(log_message)


if __name__ == "__main__":
    test = AlpSuperDemAtr()
    result = test.get_backtest_result(
        test.sptr_len,
        test.sptr_k,
        test.dema_len,
        test.atr_f,
        test.atr_s,
        test.atr_profit,
        test.atr_loss,
    )
    performance = test.evaluate_performance(result)
    print(performance)
    optimizer = Optimizer()
    best_params, best_value = optimizer.optimize_params()
    print("Best parameters:")
    print(best_params)
    print("Best value:")
    print(best_value)
