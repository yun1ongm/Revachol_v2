import os
import logging
from datetime import datetime
import operator
import warnings

import pandas as pd
import pandas_ta as ta
import numpy as np
import optuna

from Elysium.research.strategy.cta_backtest import BacktestEngine

warnings.filterwarnings("ignore")


class Indicators:
    def candle_dematr(kdf, atr_len, dema_len):
        candle = kdf[["open", "high", "low", "close"]]
        candle["body"] = candle["close"] - candle["open"]
        candle["whole"] = candle["high"] - candle["low"]
        candle["up_wick"] = np.where(
            candle["open"] > candle["close"],
            candle["high"] - candle["open"],
            candle["high"] - candle["close"],
        )
        candle["down_wick"] = np.where(
            candle["open"] > candle["close"],
            candle["close"] - candle["low"],
            candle["open"] - candle["low"],
        )
        candle["atr"] = ta.atr(kdf["high"], kdf["low"], kdf["close"],length = atr_len,mamode="ema")
        candle["dema"] = ta.dema(candle["close"], length= dema_len)

        candle["upwick2atr"] = candle["up_wick"]/candle["atr"]
        candle["downwick2atr"] = candle["down_wick"]/candle["atr"]
        candle["body2atr"] = candle["body"]/candle["atr"]
        candle["whole2atr"] = candle["whole"]/candle["atr"]

        return candle


class CandleDematr:
    alpha_name = "candle_dematr"
    symbol = "ETHUSDT"
    timeframe = "5m"
    start = datetime(2023, 10, 14, 0, 0, 0)
    window_days = 100

    atr_len = 31


    def __init__(self) -> None:
        self.backtest = BacktestEngine(
            self.alpha_name, self.symbol, self.timeframe, self.start, self.window_days
        )

    def _gen_index_signal(self) -> pd.DataFrame:
        kdf = self.backtest.kdf
        candle = Indicators.candle_atr(kdf, self.atr_len)
        candle["signal"] = 0

        candle.loc[
            (candle["volume"] > candle["volume_ma"] * self.vol_k)
            & (candle["hs_signal"] == 1),
            "signal",
        ] = 1
        candle.loc[
            (candle["volume"] > candle["volume_ma"] * self.vol_k)
            & (candle["hs_signal"] == -1),
            "signal",
        ] = -1

        return candle[["high", "low", "close", "signal", "atr"]]

    def get_backtest_result(
        self, hs_k, v_len, vol_k, atr_len, atr_k, wlr
    ) -> pd.DataFrame:
        self.hs_k = hs_k
        self.v_len = v_len
        self.vol_k = vol_k
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
            "hs_k": trial.suggest_float("hs_k", 1.5, 5, step=0.5),
            "v_len": trial.suggest_int("v_len", 8, 32),
            "vol_k": trial.suggest_float("vol_k", 1, 3, step=0.5),
            "atr_len": trial.suggest_int("atr_len", 8, 32),
            "atr_k": trial.suggest_int("atr_k", 1, 5),
            "wlr": trial.suggest_float("wlr", 1, 2, step=0.5),
        }

        result = self.get_backtest_result(**kwargs)
        performance = self.evaluate_performance(result)

        return performance[self.target]


class Optimizer(AlpHammerShootingstarAtr):
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
    test = AlpHammerShootingstarAtr()
    result = test.get_backtest_result(
        test.hs_k,
        test.v_len,
        test.vol_k,
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
