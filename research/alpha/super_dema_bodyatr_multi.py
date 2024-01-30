import os
import sys
import logging
import operator
import warnings
from datetime import datetime

import pandas as pd
import pandas_ta as ta
import optuna

sys.path.append("/Users/rivachol/Desktop/Rivachol_v2/")
from research.strategy.bodyatr_multi import StgyBodyatrMulti

warnings.filterwarnings("ignore")


class Indicators:
    def supertrend(kdf, sptr_len, sptr_k):
        supertrend = ta.supertrend(
            kdf["high"], kdf["low"], kdf["close"], sptr_len, sptr_k
        )
        supertrend.columns = ["stop_price", "direction", "lbound", "ubound"]

        return supertrend[["stop_price", "direction"]]

    def body2atr(kdf, atr_len):
        body2atr = pd.DataFrame()
        body2atr["atr"] = ta.atr(
            kdf["high"], kdf["low"], kdf["close"], length=atr_len, mamode="ema"
        )
        body2atr['body'] = kdf['close'] - kdf['open']
        body2atr['bodyatr'] = body2atr['body'] / body2atr['atr']
        return  body2atr[['bodyatr', 'atr']]


class AlpSuperDemaBodyatrMulti:
    alpha_name = "super_dema_bodyatr_multi"
    symbol = "ETHUSDT"
    timeframe = "5m"
    start = datetime(2023, 10, 20, 0, 0, 0)
    window_days = 100

    sptr_len = 20
    sptr_k = 4
    dema_len = 26
    atr_len = 11
    upbody_ratio = 2.4
    downbody_ratio = 1.1

    def __init__(self) -> None:
        self.strategy = StgyBodyatrMulti(
            self.alpha_name, self.symbol, self.timeframe, self.start, self.window_days
        )

    def _gen_index_signal(self) -> pd.DataFrame:
        kdf = self.strategy.kdf
        supertrend = Indicators.supertrend(kdf, self.sptr_len, self.sptr_k)
        body2atr = Indicators.body2atr(kdf, self.atr_len)
        kdf_sig = pd.concat([kdf, supertrend, body2atr], axis=1)
        kdf_sig["dema"] = ta.dema(kdf_sig["close"], length=self.dema_len)
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

        return kdf_sig[["close", "stop_price", "bodyatr", "signal"]]

    def get_backtest_result(
        self, sptr_len, sptr_k, dema_len, atr_len, upbody_ratio, downbody_ratio
    ) -> pd.DataFrame:
        self.sptr_len = sptr_len
        self.sptr_k = sptr_k
        self.dema_len = dema_len
        self.atr_len = atr_len
        self.upbody_ratio = upbody_ratio
        self.downbody_ratio = downbody_ratio

        kdf_sig = self._gen_index_signal()
        result = self.strategy.run(kdf_sig, upbody_ratio, downbody_ratio)
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
            "sptr_len": trial.suggest_int("sptr_len", 6, 30),
            "sptr_k": trial.suggest_float("sptr_k", 2, 4, step=0.5),
            "dema_len": trial.suggest_int("dema_len", 10, 50,step=2),
            "atr_len": trial.suggest_int("atr_len", 6, 30),
            "upbody_ratio": trial.suggest_float("upbody_ratio", 1, 2.5, step=0.1),
            "downbody_ratio": trial.suggest_float("downbody_ratio", 1, 2.5, step=0.1),
        }
        result = self.get_backtest_result(**kwargs)
        performance = self.evaluate_performance(result)

        return performance[self.target]


class Optimizer(AlpSuperDemaBodyatrMulti):
    num_evals = 100
    target = "score"
    print_log = True

    def __init__(self):
        super().__init__()
        self.start_date = self.strategy.kdf.index[0].strftime("%Y-%m-%d")
        self.end_date = self.strategy.kdf.index[-1].strftime("%Y-%m-%d")
        self._init_logger()
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
    test = AlpSuperDemaBodyatrMulti()
    result = test.get_backtest_result(
        test.sptr_len,
        test.sptr_k,
        test.dema_len,
        test.atr_len,
        test.upbody_ratio,
        test.downbody_ratio,
    )
    performance = test.evaluate_performance(result)
    print(performance)
    optimizer = Optimizer()
    best_params, best_value = optimizer.optimize_params()
    print("Best parameters:")
    print(best_params)
    print("Best value:")
    print(best_value)
