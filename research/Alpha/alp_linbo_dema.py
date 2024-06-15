import os
import logging
import operator
import optuna
import pandas as pd
import matplotlib.pyplot as plt
import sys

main_path = "/Users/rivachol/Desktop/Rivachol_v2/"
sys.path.append(main_path)
from research.backtest import BacktestFramework

import warnings

warnings.filterwarnings("ignore")


class AlpLinboDempct(BacktestFramework):
    """
    Args:
        money (float): initial money
        leverage (float): leverage
        params (dict): parameters for the alpha

    Return:
        position and signal in portfolio: pd.DataFrame

    """

    alpha_name = "alp_linbo_dempact"
    index_name = "idx_trendline"
    strategy_name = "stgy_dempct"
    symbols = ["BTCUSDT", "ETHUSDT", "SOlUSDT"]
    timeframe = "1m"
    logger = logging.getLogger(alpha_name)

    def __init__(self, money, leverage, params: dict) -> None:
        self._set_params(params)
        self.num_evals = 100
        self.target = "score"
        self.money = money
        self.leverage = leverage

    def _read_kdf_from_csv(self, symbol: str) -> pd.DataFrame:
        try:
            kdf = pd.read_csv(
                f"{main_path}test_data/{symbol}_{self.timeframe}.csv", index_col=0
            )
            kdf.index = pd.to_datetime(kdf.index)
            return kdf
        except:
            print(f"{symbol} testset not found")

    def _set_params(self, params: dict):
        self.swing = params["swing"]
        self.reset = params["reset"]
        self.slope = params["slope"]
        self.profit_pct = params["profit_pct"]
        self.loss_pct = params["loss_pct"]

    def generate_signal_position(self, kdf: pd.DataFrame) -> dict:
        try:
            index = IdxTrendline(kdf, self.swing, self.reset, self.slope)
            strategy = StgyDempact(
                self.profit_pct, self.loss_pct, self.money, self.leverage
            )
            idx_signal = index.generate_dema_signal()
            update_time = idx_signal.index[-1]
            stgy_signal = strategy.generate_portfolio(idx_signal)
            position = stgy_signal[f"position_{self.strategy_name}"][-1]
            signal = stgy_signal[f"signal_{self.strategy_name}"][-1]
            entry_price = stgy_signal["entry_price"][-1]
            signal_position = {
                "position": position,
                "signal": signal,
                "entry_price": entry_price,
                "update_time": update_time,
            }
            self.logger.info(f"{signal_position}")

            return signal_position
        except Exception as e:
            self.logger.exception(e)

    def get_backtest_result(self, params: dict) -> pd.DataFrame:
        self._set_params(params)
        merged_portfolio = pd.DataFrame()
        for symbol in self.symbols:
            kdf = self._read_kdf_from_csv(symbol)
            index = IdxTrendline(kdf, self.swing, self.reset, self.slope)
            strategy = StgyDempact(
                self.profit_pct, self.loss_pct, self.money, self.leverage
            )
            idx_signal = index.generate_dema_signal()
            portfolio = strategy.generate_portfolio(idx_signal)
            portfolio = strategy.generate_portfolio(idx_signal)
            if "value" not in merged_portfolio:
                merged_portfolio = pd.DataFrame(
                    0,
                    index=idx_signal.index,
                    columns=["value", "unrealized_pnl", "realized_pnl", "commission"],
                )
            merged_portfolio["value"] += portfolio["value"]
            merged_portfolio["unrealized_pnl"] += portfolio["unrealized_pnl"]
            merged_portfolio["realized_pnl"] += portfolio["realized_pnl"]
            merged_portfolio["commission"] += portfolio["commission"]

        return merged_portfolio

    def evaluate_performance(self, result):
        perf = self.calculate_performance(result)

        return perf

    def objective(self, trial):
        kwargs = {
            "swing": trial.suggest_int("swing", 14, 49, step=7),
            "reset": trial.suggest_int("reset", 130, 260, step=13),
            "slope": trial.suggest_float("slope", 0.2, 0.8, step=0.1),
            "profit_pct": trial.suggest_float("profit_pct", 0.0005, 0.01, step=0.0005),
            "loss_pct": trial.suggest_float("loss_pct", 0.0005, 0.01, step=0.0005),
        }
        result = self.get_backtest_result(kwargs)
        performance = self.evaluate_performance(result)

        return performance[self.target]

    def _init_optimizer(self) -> None:
        self._init_logger()
        self._log(
            f"Start optimizing {self.alpha_name} for goal {self.target} on {self.timeframe}"
        )

    def _init_logger(self) -> None:
        self.logger = logging.getLogger(self.alpha_name)
        self.logger.setLevel(logging.INFO)
        log_file = f"study_log/{self.alpha_name}.log"
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s, %(message)s")
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

    def _log(self, string) -> None:
        self.logger.info(string)

    def optimize_params(self):
        self._init_optimizer()
        study = optuna.create_study(direction="maximize")
        study.optimize(self.objective, n_trials=self.num_evals)
        sorted_trials = sorted(
            [trial for trial in study.trials if trial.value is not None],
            key=operator.attrgetter("value"),
            reverse=True,
        )
        top_5_trials = sorted_trials[:5]
        self._write_to_log(top_5_trials)

        return study.best_params, study.best_value

    def _write_to_log(self, trials):
        log_message = "Top 5 results:\n"
        for i, trial in enumerate(trials):
            log_message += f"Rank {i+1}:\n"
            log_message += f"  Params: {trial.params}\n"
            log_message += f"  Value: {trial.value}\n"
            result = self.get_backtest_result(trial.params)
            self.output_result(result, i + 1)
            performance = self.evaluate_performance(result)
            log_message += f"  Performance: {performance}\n\n"

        self._log(log_message)

    def output_result(self, result: pd.DataFrame, number) -> None:
        os.makedirs("result_book", exist_ok=True)
        result.to_csv(f"result_book/{self.alpha_name}_{number}.csv")
        # self._save_curve(result, number)

    def _save_curve(self, result: pd.DataFrame, number) -> None:
        plt.figure(figsize=(12, 6))
        plt.plot(result["value"], label="equity_curve")
        plt.legend()
        plt.grid()
        plt.title(f"Equity Curve {self.alpha_name}")
        plt.xlabel("Date")
        plt.ylabel("Equity")
        plt.savefig(f"result_book/{self.alpha_name}_{number}.png")


if __name__ == "__main__":
    params = {
        "swing": 39,
        "reset": 280,
        "slope": 0.8,
        "profit_pct": 0.002,
        "loss_pct": 0.006,
    }

    def backtest(params):
        alp_backtest = AlpLinboDempct(money=2000, leverage=5, params=params)
        best_params, best_value = alp_backtest.optimize_params()
        print(f"Best parameters: {best_params}")
        print(f"Best value: {best_value}")

    backtest(params)
