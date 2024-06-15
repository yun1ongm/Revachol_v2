import os
import logging
import operator
import optuna
import pandas as pd
import pandas_ta as pta
import matplotlib.pyplot as plt
import sys

main_path = "/Users/rivachol/Desktop/Rivachol_v2/"
sys.path.append(main_path)
from research.backtest import BacktestFramework
from index.indicators import Supertrend, Vwap
from strategy.trailing import DemaTrailing
import warnings

warnings.filterwarnings("ignore")


class AlpSuperVwap(BacktestFramework):
    alpha_name = "alp_super_vwap_trailing"
    symbols = ["BTCUSDT"]
    timeframe = "1m"
    logger = logging.getLogger(alpha_name)

    def __init__(self, money, leverage, params: dict):
        self.num_evals = 100
        self.target = "t_sharpe"
        self.money = money
        self.leverage = leverage
        self.params = params

    def _read_kdf_from_csv(self, symbol: str) -> pd.DataFrame:
        try:
            kdf = pd.read_csv(
                f"{main_path}test_data/{symbol}_{self.timeframe}.csv", index_col=0
            )
            kdf.index = pd.to_datetime(kdf.index)
            return kdf
        except:
            print(f"{symbol} testset not found")

    def _get_params(self, symbol: str) -> tuple:
        sptr_len = self.params[f"{symbol}_sptr_len"]
        sptr_k = self.params[f"{symbol}_sptr_k"]
        vwap_len = self.params[f"{symbol}_vwap_len"]
        tp_percent = self.params[f"{symbol}_tp_percent"]
        sl_percent = self.params[f"{symbol}_sl_percent"]

        return sptr_len, sptr_k, vwap_len, tp_percent, sl_percent

    def get_backtest_result(self, params: dict) -> pd.DataFrame:
        merged_portfolio = pd.DataFrame()
        self.params = params
        for symbol in self.symbols:
            sptr_len, sptr_k, vwap_len, tp_percent, sl_percent = self._get_params(
                symbol
            )
            kdf = self._read_kdf_from_csv(symbol)
            portfolio = self.generate_portfolio(
                kdf, sptr_len, sptr_k, vwap_len, tp_percent, sl_percent
            )
            # self.output_result(portfolio, symbol)
            if "value" not in merged_portfolio:
                merged_portfolio = pd.DataFrame(
                    0,
                    index=portfolio.index,
                    columns=["value", "unrealized_pnl", "realized_pnl", "commission"],
                )
            merged_portfolio["value"] += portfolio["value"]
            merged_portfolio["unrealized_pnl"] += portfolio["unrealized_pnl"]
            merged_portfolio["realized_pnl"] += portfolio["realized_pnl"]
            merged_portfolio["commission"] += portfolio["commission"]

        return merged_portfolio

    def objective(self, trial):
        kwargs = {}
        for symbol in self.symbols:
            kwargs.update(
                {
                    f"{symbol}_sptr_len": trial.suggest_int(
                        f"{symbol}_sptr_len", 9, 99, step=3
                    ),
                    f"{symbol}_sptr_k": trial.suggest_float(
                        f"{symbol}_sptr_k", 2, 4, step=0.5
                    ),
                    f"{symbol}_vwap_len": trial.suggest_int(
                        f"{symbol}_vwap_len", 30, 240, step=10
                    ),
                    f"{symbol}_tp_percent": trial.suggest_float(
                        f"{symbol}_tp_percent", 0.001, 0.02, step=0.001
                    ),
                    f"{symbol}_sl_percent": trial.suggest_float(
                        f"{symbol}_sl_percent", 0.001, 0.02, step=0.001
                    ),
                }
            )
        result = self.get_backtest_result(kwargs)
        performance = self.calculate_performance(result)

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
        top_3_trials = sorted_trials[:3]
        self._write_to_log(top_3_trials)

        return study.best_params, study.best_value

    def _write_to_log(self, trials):
        log_message = "Top 3 results:\n"
        for i, trial in enumerate(trials):
            log_message += f"Rank {i+1}:\n"
            log_message += f"  Params: {trial.params}\n"
            log_message += f"  Value: {trial.value}\n"
            result = self.get_backtest_result(trial.params)
            performance = self.calculate_performance(result)
            self.output_result(result, f"rank_{i+1}")
            log_message += f"  Performance: {performance}\n\n"

        self._log(log_message)

    def output_result(self, result: pd.DataFrame, tag: str) -> None:
        os.makedirs("result_book", exist_ok=True)
        result.to_csv(f"result_book/{self.alpha_name}_{tag}.csv")
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

    def generate_portfolio(
        self, kdf: pd.DataFrame, sptr_len, sptr_k, vwap_len, tp_percent, sl_percent
    ) -> pd.DataFrame:
        supertrend = Supertrend(kdf, sptr_len, sptr_k)
        supertrend_df = supertrend.get_indicator()
        vwap = Vwap(kdf, vwap_len)
        vwap_df = vwap.get_indicator()
        signal = pd.concat([kdf, supertrend_df, vwap_df], axis=1)
        signal["dema"] = pta.dema(kdf["close"], length=sptr_len)
        signal["signal"] = 0

        signal.loc[
            (signal["close"] > signal["vwap"])
            & (signal["direction"] == 1)
            & (signal["direction"].shift(1) == -1),
            "signal",
        ] = 1
        signal.loc[
            (signal["close"] < signal["vwap"])
            & (signal["direction"] == -1)
            & (signal["direction"].shift(1) == 1),
            "signal",
        ] = -1
        strategy = DemaTrailing(tp_percent, sl_percent, self.money, self.leverage)
        portfolio = strategy.get_result(signal)
        # position = portfolio[f"position"][-1]
        # signal = portfolio[f"signal"][-1]
        # entry_price = portfolio["entry_price"][-1]
        # stop_profit = portfolio["stop_profit"][-1]
        # stop_loss = portfolio["stop_loss"][-1]
        # update_time = portfolio.index[-1]
        # signal_position = {
        #     "position": position,
        #     "signal": signal,
        #     "entry_price": entry_price,
        #     "stop_profit": stop_profit,
        #     "stop_loss": stop_loss,
        #     "update_time": update_time,
        # }
        # self.logger.info(f"{signal_position}")
        return portfolio


if __name__ == "__main__":
    params = {
        "BTCUSDT_sptr_len": 24,
        "BTCUSDT_sptr_k": 3.0,
        "BTCUSDT_vwap_len": 20,
        "BTCUSDT_tp_percent": 12,
        "BTCUSDT_sl_percent": 2,
    }

    def backtest(params):
        alp_backtest = AlpSuperVwap(money=2000, leverage=5, params=params)
        best_params, best_value = alp_backtest.optimize_params()
        print(f"Best parameters: {best_params}")
        print(f"Best value: {best_value}")

    backtest(params)
    # alp_backtest = AlpAdxStochrsiOpenatr(money = 2000, leverage = 5, params = params)
    # result = alp_backtest.get_backtest_result(params)
    # #输出result成csv文件
    # alp_backtest.output_result(result, 1)
    # performance = alp_backtest.evaluate_performance(result)
    # print(performance)
