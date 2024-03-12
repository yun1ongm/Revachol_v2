import os
import logging
import operator
from datetime import datetime

import pandas as pd
import pandas_ta as ta
import numpy as np
import optuna

import warnings
warnings.filterwarnings("ignore")
import sys
sys.path.append("/Users/rivachol/Desktop/Rivachol_v2/")
from research.strategy.volcum_single import StgyVolcumSing


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
    
    def volume_cumshot(kdf, shot, period):
        cumulative = kdf["volume_USDT"].rolling(shot).sum()
        volume_ema = ta.ema(kdf["volume_USDT"],period)
        vol_cumshot = pd.concat([cumulative, volume_ema], axis=1)
        vol_cumshot.columns = ["cumulative", "volume_ema"]
        vol_cumshot["cumshot"] = vol_cumshot["cumulative"] / vol_cumshot["volume_ema"]
        return vol_cumshot
    
    # def calc_stop_by_atr(kdf, period, atr_ratio):
    #     dema = ta.dema(kdf["close"], length=period)
    #     atr = ta.atr(kdf["close"], length=period, mamode = 'ema')
    #     stopatr = pd.concat([dema,atr], axis=1)
    #     stopatr.columns = ['dema','atr']
    #     stopatr['stop_price'] = stopatr['dema']

    #     kdf["stop_price"] = 



class AdxRsiVolcumSing:
    alpha_name = "adx_rsi_volcum_sing"
    symbol = "ETHUSDT"
    timeframe = "5m"
    start = datetime(2023, 11, 15, 0, 0, 0)
    window_days = 100

    adx_len = 20
    rsi_len = 7
    kd = 8
    shot = 2
    dema_len = 39
    atr_loss = 3

    def __init__(self) -> None:
        self.strategy = StgyVolcumSing(
            self.alpha_name, self.symbol, self.timeframe, self.start, self.window_days
        )

    def _gen_index_signal(self) -> pd.DataFrame:
        kdf = self.strategy.kdf
        adx = Indicators.adx(kdf, self.adx_len)
        stochrsi = Indicators.stochrsi(kdf, self.rsi_len, self.kd)
        kdf_sig = pd.concat([kdf[["high", "low", "close"]], adx, stochrsi], axis=1)

        
        kdf_sig["signal"] = 0
        kdf_sig.loc[
            (kdf_sig["adx"] >= 30)
            & (kdf_sig["GXvalue"] < 20)
            & (0 < kdf_sig["GXvalue"]),
            "signal",
        ] = 1
        kdf_sig.loc[(kdf_sig["adx"] >= 30) & (kdf_sig["DXvalue"] > 80), "signal"] = -1

        return kdf_sig[["high", "low", "close", "atr", "signal", "dema"]]

    def get_backtest_result(
        self, adx_len, rsi_len, kd, dema_len, atr_f, atr_s, atr_profit, atr_loss
    ) -> pd.DataFrame:
        self.adx_len = adx_len
        self.rsi_len = rsi_len
        self.kd = kd
        self.dema_len = dema_len
        self.atr_f = atr_f
        self.atr_s = atr_s
        self.atr_profit = atr_profit
        self.atr_loss = atr_loss

        kdf_sig = self._gen_index_signal()
        result = self.strategy.run(kdf_sig, atr_profit, atr_loss)
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
            "adx_len": trial.suggest_int("adx_len", 6, 30),
            "rsi_len": trial.suggest_int("rsi_len", 6, 30),
            "kd": trial.suggest_int("kd", 4, 12),
            "dema_len": trial.suggest_int("dema_len", 12, 50),
            "atr_f": trial.suggest_int("atr_f", 6, 15),
            "atr_s": trial.suggest_int("atr_s", 15, 30),
            "atr_profit": trial.suggest_int("atr_profit", 2, 6),
            "atr_loss": trial.suggest_int("atr_loss", 1, 4),
        }

        result = self.get_backtest_result(**kwargs)
        performance = self.evaluate_performance(result)

        return performance[self.target]


class Optimizer(AdxRsiDemAtrSing):
    num_evals = 100
    target = "score"
    print_log = True

    def __init__(self):
        super().__init__()
        self.start_date = self.strategy.kdf.index[0].strftime("%Y-%m-%d")
        self.end_date = self.strategy.kdf.index[-1].strftime("%Y-%m-%d")
        self._init_logger()
        self._log(
            f"Start optimizing {self.alpha_name} for goal {self.target} on {self.symbol} {self.timeframe} from {self.start_date} to {self.end_date} based on goal of {self.target}"
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
            log_message += f"  Value: {trial.value}\n"
            result = self.get_backtest_result(**trial.params)
            performance = self.evaluate_performance(result)
            log_message += f"  Performance: {performance}\n\n"

        self._log(log_message)


if __name__ == "__main__":
    test = AdxRsiDemAtrSing()
    result = test.get_backtest_result(
        test.adx_len,
        test.rsi_len,
        test.kd,
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
    print(f"Best parameters: {best_params}")
    print(f"Best value: {best_value}")
