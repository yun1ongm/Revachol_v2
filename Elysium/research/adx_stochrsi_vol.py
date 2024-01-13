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
    def adx(kdf, period):
        adx = ta.adx(kdf["high"], kdf["low"], kdf["close"], length=period)
        adx.columns = ["adx", "plus", "minus"]
        return adx

    def stochrsi(kdf, period, kd):
        stochrsi = ta.stochrsi(kdf["close"], rsi_length=period, k=kd, d=kd)
        stochrsi.columns = ["k", "d"]
        condition1 = stochrsi["k"] > stochrsi["d"]
        condition2 = stochrsi["k"].shift(1) <= stochrsi["d"].shift(1)
        stochrsi["GXvalue"] = np.where(condition1 & condition2, stochrsi["d"], 0)
        condition3 = stochrsi["k"] < stochrsi["d"]
        condition4 = stochrsi["k"].shift(1) >= stochrsi["d"].shift(1)
        stochrsi["DXvalue"] = np.where(condition3 & condition4, stochrsi["d"], 0)
        return stochrsi


class AdxRsiVol:
    alpha_name = "adx_rsi_vol"
    symbol = "ETHUSDT"
    timeframe = "5m"
    start = datetime(2023, 9, 20, 0, 0, 0)
    window_days = 100

    adx_len = 16
    rsi_len = 16
    kd = 6
    vol_len = 19
    vol_k = 4

    def __init__(self) -> None:
        self.backtest = BacktestEngine(
            self.alpha_name, self.symbol, self.timeframe, self.start, self.window_days
        )

    def _gen_index_signal(self) -> pd.DataFrame:
        kdf = self.backtest.kdf
        adx = Indicators.adx(kdf, self.adx_len)
        stochrsi = Indicators.stochrsi(kdf, self.rsi_len, self.kd)
        kdf_sig = pd.concat(
            [kdf[["high", "low", "close", "volume"]], adx, stochrsi], axis=1
        )
        kdf_sig["volume_ema"] = ta.ema(kdf_sig["volume"], length=self.vol_len)
        kdf_sig["stop_price"] = ta.vwap(
            kdf["high"], kdf["low"], kdf["close"], kdf["volume"], length=self.vol_len
        )
        kdf_sig["signal"] = 0
        kdf_sig.loc[
            (kdf_sig["adx"] >= 40)
            & (kdf_sig["GXvalue"] < 20)
            & (0 < kdf_sig["GXvalue"]),
            "signal",
        ] = 1
        kdf_sig.loc[(kdf_sig["adx"] >= 40) & (kdf_sig["DXvalue"] > 80), "signal"] = -1

        return kdf_sig[["close", "volume", "volume_ema", "stop_price", "signal"]]

    def get_backtest_result(self, adx_len, rsi_len, kd, vol_len, vol_k) -> pd.DataFrame:
        self.adx_len = adx_len
        self.rsi_len = rsi_len
        self.kd = kd
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
            "adx_len": trial.suggest_int("adx_len", 6, 24),
            "rsi_len": trial.suggest_int("rsi_len", 8, 32),
            "kd": trial.suggest_int("kd", 4, 16),
            "vol_len": trial.suggest_int("atr_len", 8, 32),
            "vol_k": trial.suggest_int("atr_k", 1, 5),
        }

        result = self.get_backtest_result(**kwargs)
        performance = self.evaluate_performance(result)

        return performance[self.target]


class Optimizer(AdxRsiVol):
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
    test = AdxRsiVol()
    result = test.get_backtest_result(
        test.adx_len,
        test.rsi_len,
        test.kd,
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
