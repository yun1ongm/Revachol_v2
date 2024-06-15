import os
import sys

main_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.append(main_path)
import logging
import operator
import optuna
import pandas as pd
import pandas_ta as pta
import yaml
import matplotlib.pyplot as plt
from research.backtest import BacktestFramework
from index.indicators import Adx, StochRsi
from strategy.multiple import DemaStd
import warnings

warnings.filterwarnings("ignore")


class AlpAdxStochRsiMultiple(BacktestFramework):
    alpha_name = "alp_adx_stochrsi_multiple"
    pairs = ["BTCUSD"]
    timeframe = "1m"
    logger = logging.getLogger(alpha_name)

    def __init__(self, money, leverage, mode=0) -> None:
        self.mode = mode
        self.money = money
        self.leverage = leverage
        if mode == 0:  # 0 for backtest, 1 for production
            self.num_evals = 100
            self.target = "t_sharpe"
        else:
            self.params = self._read_params()

    def _read_params(self, rel_path="/production/config.yaml") -> dict:
        try:
            with open(main_path + rel_path, "r") as stream:
                config = yaml.safe_load(stream)
                params = config[self.alpha_name]
            return params
        except FileNotFoundError:
            self.logger.error("Config file not found")
            sys.exit(1)

    def _read_kdf_from_csv(self, pair: str) -> pd.DataFrame:
        symbol = pair.replace("USD", "USDT")
        try:
            kdf = pd.read_csv(
                f"{main_path}test_data/{symbol}_{self.timeframe}.csv", index_col=0
            )
            kdf.index = pd.to_datetime(kdf.index)
            return kdf
        except:
            print(f"{symbol} testset not found")

    def get_backtest_result(self, params: dict) -> pd.DataFrame:
        merged_portfolio = pd.DataFrame()
        self.params = params
        for pair in self.pairs:
            kdf = self._read_kdf_from_csv(pair)
            portfolio = self.generate_portfolio(pair, kdf)
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
        for pair in self.pairs:
            kwargs.update(
                {
                    f"{pair}_adx_len": trial.suggest_int(
                        f"{pair}_adx_len", 9, 99, step=3
                    ),
                    f"{pair}_rsi_len": trial.suggest_int(
                        f"{pair}_rsi_len", 9, 99, step=3
                    ),
                    f"{pair}_stoch_len": trial.suggest_int(
                        f"{pair}_stoch_len", 9, 99, step=3
                    ),
                    f"{pair}_kd": trial.suggest_int(f"{pair}_kd", 3, 6),
                    f"{pair}_tp_std": trial.suggest_int(
                        f"{pair}_tp_std", 3, 12, step=1
                    ),
                    f"{pair}_sl_std": trial.suggest_int(f"{pair}_sl_std", 2, 8, step=1),
                    f"{pair}_dema_len": trial.suggest_int(
                        f"{pair}_dema_len", 9, 99, step=3
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

    def _get_params(self, pair: str) -> tuple:
        adx_len = self.params[f"{pair:}_adx_len"]
        stoch_len = self.params[f"{pair}_stoch_len"]
        rsi_len = self.params[f"{pair}_rsi_len"]
        kd = self.params[f"{pair}_kd"]
        dema_len = self.params[f"{pair}_dema_len"]
        tp_std = self.params[f"{pair}_tp_std"]
        sl_std = self.params[f"{pair}_sl_std"]

        return adx_len, stoch_len, rsi_len, kd, dema_len, tp_std, sl_std

    def generate_portfolio(self, pair: str, kdf: pd.DataFrame) -> pd.DataFrame | float:
        adx_len, stoch_len, rsi_len, kd, dema_len, tp_std, sl_std = self._get_params(
            pair
        )
        adx = Adx(kdf, adx_len)
        adx_df = adx.get_indicator()
        stoch_rsi = StochRsi(kdf, stoch_len, rsi_len, kd)
        stoch_rsi_df = stoch_rsi.get_indicator()
        trading_signal = pd.concat([kdf, adx_df, stoch_rsi_df], axis=1)
        trading_signal["std"] = pta.stdev(kdf["close"], length=dema_len)
        trading_signal["dema"] = pta.dema(kdf["close"], length=dema_len)
        trading_signal["signal"] = 0

        trading_signal.loc[
            (trading_signal["adx"] >= 25)
            & (trading_signal["upcross"] < 20)
            & (trading_signal["upcross"] > 0),
            "signal",
        ] = 1
        trading_signal.loc[
            (trading_signal["adx"] >= 25) & (trading_signal["downcross"] > 80),
            "signal",
        ] = -1
        strategy = DemaStd(tp_std, sl_std, self.money, self.leverage)
        portfolio = strategy.get_result(trading_signal)
        if self.mode == 1:
            alpha_position = self._alpha_position(portfolio, trading_signal)
            return alpha_position
        return portfolio

    def _alpha_position(
        self, portfolio: pd.DataFrame, trading_signal: pd.DataFrame
    ) -> float:
        position = portfolio[f"position"][-1]
        signal = portfolio[f"signal"][-1]
        entry_price = portfolio["entry_price"][-1]
        take_profit = portfolio["take_profit"][-1]
        stop_loss = portfolio["stop_loss"][-1]
        update_time = portfolio.index[-1]
        if abs(position) >= 0.001:
            alpha_position = {
                "position": position,
                "signal": signal,
                "entry_price": entry_price,
                "take_profit": take_profit,
                "stop_loss": stop_loss,
                "update_time": update_time,
            }
            self.logger.info(f"{alpha_position}")
        if signal != 0:
            trigger_condition = {
                "adx": trading_signal["adx"][-1],
                "upcross": trading_signal["upcross"][-1],
                "downcross": trading_signal["downcross"][-1],
                "std": trading_signal["std"][-1],
                "dema": trading_signal["dema"][-1],
            }
            self.logger.info(f"trigger_condition: {trigger_condition}")
        return round(position, 3)


if __name__ == "__main__":
    params = {
        "BTCUSD_adx_len": 18,
        "BTCUSD_rsi_len": 9,
        "BTCUSD_stoch_len": 72,
        "BTCUSD_kd": 6,
        "BTCUSD_tp_std": 10,
        "BTCUSD_sl_std": 2,
        "BTCUSD_dema_len": 54,
    }

    def backtest():
        alp_backtest = AlpAdxStochRsiMultiple(money=2000, leverage=5)
        best_params, best_value = alp_backtest.optimize_params()
        print(f"Best parameters: {best_params}")
        print(f"Best value: {best_value}")

    backtest()
    # alp_backtest = AlpAdxStochRsiMultiple(money=2000, leverage=5, params=params)
    # result = alp_backtest.get_backtest_result(params)
    # # 输出result成csv文件
    # alp_backtest.output_result(result, 1)
    # performance = alp_backtest.calculate_performance(result)
    # print(performance)
