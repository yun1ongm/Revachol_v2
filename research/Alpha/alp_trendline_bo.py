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
from Index.idx_trendline import IdxTrendline
from research.Strategy.stgy_dema import StgyDema
import warnings
warnings.filterwarnings("ignore")

class AlpTrendlineDema(BacktestFramework):
    """
        Args:
            money (float): initial money
            leverage (float): leverage
            params (dict): parameters for the alpha

        Return:
            position and signal in portfolio: pd.DataFrame
        
    """
    alpha_name = "alp_tredline_bo"  
    index_name = "idx_trendline"
    strategy_name = "stgy_dema"
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
    
    def _set_params(self, params:dict):
        self.swing = params["swing"]
        self.reset = params["reset"]
        self.slope = params["slope"]    
        self.profit_pct = params["profit_pct"]

    def generate_signal_position(self, kdf:pd.DataFrame) -> dict:
        try:
            index = IdxTrendline(kdf, self.swing, self.reset, self.slope)
            strategy = StgyDema(self.profit_pct, self.money, self.leverage)
            idx_signal = index.generate_dema_signal()
            update_time = idx_signal.index[-1]
            stgy_signal = strategy.generate_portfolio(idx_signal)
            position = stgy_signal[f"position_{self.strategy_name}"][-1]
            signal = stgy_signal[f"signal_{self.strategy_name}"][-1]
            entry_price = stgy_signal["entry_price"][-1]
            stop_price = stgy_signal["stop_price"][-1]
            signal_position ={
                "position": position,
                "signal": signal,
                "entry_price": entry_price,
                "stop_price": stop_price,
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
        index = IdxTrendline(self.kdf, self.swing, self.reset, self.slope)
        strategy = StgyDema(self.profit_pct, self.money, self.leverage)
        idx_signal = index.generate_dema_signal()
        portfolio = strategy.generate_portfolio(idx_signal)
        return portfolio

    def evaluate_performance(self, result):
        perf = self.calculate_performance(result)

        return perf
    
    def objective(self, trial):
        kwargs = {
            "swing": trial.suggest_int("swing", 10, 100),
            "reset": trial.suggest_int("reset", 100, 300),
            "slope": trial.suggest_float("slope", 0.2, 2, step=0.2),
            "profit_pct": trial.suggest_float("profit_pct", 0.01, 0.1, step=0.01),
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
    params = { "swing": 10, "reset": 100, "slope": 0.4, "profit_pct": 0.02}

    def backtest(params):
        alp_backtest = AlpTrendlineDema(money = 2000, leverage = 5, params = params)
        best_params, best_value =  alp_backtest.optimize_params()
        print(f"Best parameters: {best_params}")
        print(f"Best value: {best_value}")

    backtest(params)
