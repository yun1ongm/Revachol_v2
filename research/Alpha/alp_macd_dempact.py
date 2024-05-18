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
from Index.idx_macd_trend import IdxMacdTrend
from research.Strategy.stgy_dempact import StgyDempact
import warnings
warnings.filterwarnings("ignore")

class AlpMacdDema(BacktestFramework):
    """
        Args:
            money (float): initial money
            leverage (float): leverage
            params (dict): parameters for the alpha

        Return:
            position and signal in portfolio: pd.DataFrame
        
    """
    alpha_name = "alp_macd_dempct"
    index_name = "idx_macd_trend"
    strategy_name = "stgy_dempct"
    symbol = "BTCUSDT"
    timeframe = "5m"
    logger = logging.getLogger(alpha_name)

    def __init__(self, money, leverage, params:dict) -> None:
        self._set_params(params)
        self.num_evals = 100
        self.target = "t_sharpe"
        self.kdf = self._read_kdf_from_csv()
        self.money = money
        self.leverage = leverage

    def _read_kdf_from_csv(self) -> pd.DataFrame:
        kdf = pd.read_csv(f"{main_path}test_data/{self.symbol}_5m.csv", index_col=0)
        kdf.index = pd.to_datetime(kdf.index)
        return kdf

    def _set_params(self, params:dict) -> None:
        self.fast = params["fast"]
        self.slow = params["slow"]
        self.signaling = params["signaling"]
        self.threshold = params["threshold"]
        self.dema_len = params["dema_len"]
        self.profit_pct = params["profit_pct"]
        self.loss_pct = params['loss_pct']

    def generate_signal_position(self, kdf:pd.DataFrame) -> dict:
        try:
            index = IdxMacdTrend(kdf, self.fast, self.slow, self.signaling, self.threshold, self.dema_len)
            strategy = StgyDempact(self.profit_pct, self.loss_pct, self.money, self.leverage)
            idx_signal = index.generate_dematr_signal()
            update_time = idx_signal.index[-1]
            portfolio = strategy.generate_portfolio(idx_signal)
            position = portfolio[f"position"][-1]
            signal = portfolio[f"signal"][-1]
            entry_price = portfolio["entry_price"][-1]
            signal_position ={
                "position": position,
                "signal": signal,
                "entry_price": entry_price,
                "update_time": update_time
            }
            self.logger.info(f"{signal_position}")

            return signal_position
        except Exception as e:
            self.logger.exception(e)

    def get_backtest_result(
        self, params:dict
    ) -> pd.DataFrame:
        self._set_params(params)
        index = IdxMacdTrend(self.kdf, self.fast, self.slow, self.signaling, self.threshold, self.dema_len)
        strategy = StgyDempact(self.profit_pct, self.loss_pct, self.money, self.leverage)
        idx_signal = index.generate_dematr_signal()
        portfolio = strategy.generate_portfolio(idx_signal)
        return portfolio

    def evaluate_performance(self, result):
        perf = self.calculate_performance(result)

        return perf

    def objective(self, trial):
        kwargs = {
            "fast": trial.suggest_int("fast", 8, 16),
            "slow": trial.suggest_int("slow", 20, 32),
            "signaling": trial.suggest_int("signaling", 6, 12),
            "threshold": trial.suggest_float("threshold", 0.1, 1, step=0.1),
            "dema_len": trial.suggest_int("dema_len", 10, 100),
            "profit_pct": trial.suggest_float("profit_pct", 0.002, 0.04, step=0.002),
            "loss_pct": trial.suggest_float("loss_pct", 0.002, 0.02, step=0.002)
        }

        result = self.get_backtest_result(kwargs)
        performance = self.evaluate_performance(result)

        return performance[self.target]
    
    def _init_optimizer(self):
        self.start_date = self.kdf.index[0].strftime("%Y-%m-%d")
        self.end_date = self.kdf.index[-1].strftime("%Y-%m-%d")
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
            self.output_result(result,i+1)
            performance = self.evaluate_performance(result)
            log_message += f"  Performance: {performance}\n\n"

        self._log(log_message)

    def output_result(self, result:pd.DataFrame, number) -> None:
        os.makedirs("result_book", exist_ok=True)
        start_date = self.kdf.index[0].strftime("%Y-%m-%d")
        end_date = self.kdf.index[-1].strftime("%Y-%m-%d")
        result.to_csv(f"result_book/{self.alpha_name}_{start_date}to{end_date}_{number}.csv")
        self._save_curve(result, number)

    def _save_curve(self, result:pd.DataFrame, number) -> None:
        start_date = self.kdf.index[0].strftime("%Y-%m-%d")
        end_date = self.kdf.index[-1].strftime("%Y-%m-%d")
        plt.figure(figsize=(12, 6))
        plt.plot(result["value"], label="equity_curve")
        plt.legend()
        plt.grid()
        plt.title(f"Equity Curve {self.alpha_name}")
        plt.xlabel("Date")
        plt.ylabel("Equity")
        plt.savefig(f"result_book/{self.alpha_name}_{start_date}to{end_date}_{number}.png")

if __name__ == "__main__":
    params = {'fast': 11, 'slow': 21, 'signaling': 9, 'threshold': 0.3, 'dema_len': 100, 'profit_pct': 0.04, 'loss_pct': 0.01}
    def backtest(params):
        alp_backtest = AlpMacdDema(money = 2000, leverage = 5, params = params)
        best_params, best_value =  alp_backtest.optimize_params()
        print(f"Best parameters: {best_params}")
        print(f"Best value: {best_value}")

    backtest(params)

