import os
import logging
from datetime import datetime
import operator
import warnings

import pandas as pd
import pandas_ta as ta
import numpy as np
import optuna

from cta_backtest import BacktestEngine

warnings.filterwarnings("ignore")


class Indicators:
    def macd(kdf, fast, slow, signaling):
        macd_df = ta.macd(kdf["close"], fast=fast, slow=slow, signal=signaling)
        macd_df.columns = ["diff", "macd", "dea"]
        condition1 = macd_df["diff"] > macd_df["dea"]
        condition2 = macd_df["diff"].shift(1) <= macd_df["dea"].shift(1)
        macd_df["GXvalue"] = np.where(condition1 & condition2, macd_df["dea"], 0)
        condition3 = macd_df["diff"] < macd_df["dea"]
        condition4 = macd_df["diff"].shift(1) >= macd_df["dea"].shift(1)
        macd_df["DXvalue"] = np.where(condition3 & condition4, macd_df["dea"], 0)

        return macd_df

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
        # 如果dematr["Xvalue"]为0，那么填充为前一行的值
        dematr["Xvalue"] = dematr["Xvalue"].replace(0, method="ffill")
        return dematr


class AlpMacdRvsAtr:
    alpha_name = "macd_rvs_atr"
    symbol = "ETHUSDT"
    timeframe = "5m"
    start = datetime(2023, 9, 24, 0, 0, 0)
    window_days = 100

    fast = 12
    slow = 25
    signaling = 8
    threshold = 1
    atr_len = 13
    atr_k = 6
    wlr = 1

    def __init__(self) -> None:
        self.backtest = BacktestEngine(
            self.alpha_name, self.symbol, self.timeframe, self.start, self.window_days
        )

    def _gen_index_signal(self) -> pd.DataFrame:
        kdf = self.backtest.kdf
        macd = Indicators.macd(kdf, self.fast, self.slow, self.signaling)
        kdf_sig = pd.concat([kdf, macd], axis=1)
        dematr = Indicators.dematr(kdf_sig, self.atr_len, self.slow)
        kdf_sig["atr"] = dematr["Xvalue"]
        kdf_sig["signal"] = 0
        # 零下金叉背离
        last_gx = 0
        last_price = 0
        for index, value in kdf_sig["GXvalue"].items():
            price = kdf_sig["low"].at[index]
            if value < last_gx and price > last_price:
                kdf_sig.at[index, "signal"] = 1
            if value < -self.threshold:
                last_gx = value
                last_price = kdf_sig["close"].at[index]
        # 零上死叉背离
        last_dx = 0
        last_price = 0
        for index, value in kdf_sig["DXvalue"].items():
            price = kdf_sig["high"].at[index]
            if value > last_dx and price < last_price:
                kdf_sig.at[index, "signal"] = -1
            if value > self.threshold:
                last_dx = value
                last_price = kdf_sig["close"].at[index]

        return kdf_sig[["high", "low", "close", "atr", "signal"]]

    def get_backtest_result(
        self, fast, slow, signaling, threshold, atr_len, atr_k, wlr
    ) -> pd.DataFrame:
        self.fast = fast
        self.slow = slow
        self.signaling = signaling
        self.threshold = threshold
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
            "fast": trial.suggest_int("fast", 6, 18),
            "slow": trial.suggest_int("slow", 16, 32),
            "signaling": trial.suggest_int("signaling", 8, 20),
            "threshold": trial.suggest_float("threshold", 0.5, 2, step=0.5),
            "atr_len": trial.suggest_int("atr_len", 8, 30),
            "atr_k": trial.suggest_int("atr_k", 2, 5),
            "wlr": trial.suggest_float("wlr", 1, 2, step=0.5),
        }
        result = self.get_backtest_result(**kwargs)
        performance = self.evaluate_performance(result)

        return performance[self.target]


class Optimizer(AlpMacdRvsAtr):
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
    test = AlpMacdRvsAtr()
    result = test.get_backtest_result(
        test.fast,
        test.slow,
        test.signaling,
        test.threshold,
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
