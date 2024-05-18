import os
import logging
import time
import operator
import optuna
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import sys
temp_path = "/Users/rivachol/Desktop/Rivachol_v2/"
sys.path.append(temp_path)
from Research.backtest import BacktestFramework
from Market.kline import KlineGenerator
from Index.idx_macd_rvs import IdxMacdRvs
from research.Strategy.stgy_openatr import StgyCanpctSing
import contek_timbersaw as timbersaw
import warnings
warnings.filterwarnings("ignore")

class AlpMacdRvsCanpctSing(BacktestFramework):
    """
        Args:
            money (float): initial money
            leverage (float): leverage
            sizer (float): sizer

        Return:
            position and signal in portfolio: pd.DataFrame
        
    """
    alpha_name = "alp_macd_rvs_canpct_sing"
    index_name = "idx_macd_rvs"
    strategy_name = "stgy_canpct_sing"
    symbol = "BTCUSDT"
    timeframe = "5m"
    logger = logging.getLogger(alpha_name)

    def __init__(self, money, leverage, sizer, params:dict, mode = 0) -> None:
        '''initialize the parameters
        Args:
        money: float
        leverage: float
        sizer: float
        params: dict
        mode: int (0 for backtest, 1 for live trading)
        '''
        self._set_params(params)
        if mode == 0:
            self.num_evals = 100
            self.target = "t_sharpe"
            market = KlineGenerator('BTCUSDT', '5m', mode = 0, 
                                    start = datetime(2023, 12, 10, 0, 0, 0), 
                                    window_days=100)
            self.kdf = market.kdf
            self.money = 10000
            self.leverage = 5
            self.sizer = 0.1 if self.symbol == "BTCUSDT" else 2
        else:
            self.money = money
            self.leverage = leverage
            self.sizer = sizer

    def _set_params(self, params:dict) -> None:
        '''set the parameters
        Args:
        params: dict
        '''
        self.fast = params["fast"]
        self.slow = params["slow"]
        self.signaling = params["signaling"]
        self.threshold = params["threshold"]
        self.dema_len = params["dema_len"]
        self.harvest_ratio = params["harvest_ratio"]
        self.retreat_ratio= params["retreat_ratio"]

    def generate_signal_position(self, kdf:pd.DataFrame) -> dict:
        try:
            index = IdxMacdRvs(kdf, self.fast, self.slow, self.signaling, self.threshold, self.dema_len)
            strategy = StgyCanpctSing(self.harvest_ratio, self.retreat_ratio, self.money, self.leverage, self.sizer)
            idx_signal = index.generate_cansure_signal()
            update_time = idx_signal.index[-1]
            portfolio = strategy.generate_portfolio(idx_signal)
            position = portfolio[f"position"][-1]
            signal = portfolio[f"signal"][-1]
            entry_price = portfolio["entry_price"][-1]
            stop_profit = portfolio["stop_profit"][-1]
            stop_loss = portfolio["stop_loss"][-1]
            signal_position ={
                "position": position,
                "signal": signal,
                "entry_price": entry_price,
                "stop_profit": stop_profit,
                "stop_loss": stop_loss,
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
        index = IdxMacdRvs(self.kdf, self.fast, self.slow, self.signaling, self.threshold, self.dema_len)
        strategy = StgyCanpctSing(self.harvest_ratio, self.retreat_ratio, self.money, self.leverage, self.sizer)
        idx_signal = index.generate_cansure_signal()
        portfolio = strategy.generate_portfolio(idx_signal)
        return portfolio

    def evaluate_performance(self, result):
        perf = self.calculate_performance(result)

        return perf

    def objective(self, trial):
        kwargs = {
            "fast": trial.suggest_int("fast", 6, 18),
            "slow": trial.suggest_int("slow", 20, 32),
            "signaling": trial.suggest_int("signaling", 4, 14),
            "threshold": trial.suggest_float("threshold", 0.2, 2, step=0.2),
            "dema_len": trial.suggest_int("dema_len", 15, 60),
            "harvest_ratio": trial.suggest_float("harvest_ratio",  0.001, 0.008, step=0.001),
            "retreat_ratio": trial.suggest_float("retreat_ratio",  0.001, 0.008, step=0.001),
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
        top_10_trials = sorted_trials[:10]
        self._write_to_log(top_10_trials)

        return study.best_params, study.best_value

    def _write_to_log(self, trials):
        log_message = "Top 10 results:\n"
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
    params = {'fast': 12, 'slow': 23, 'signaling': 9, 'threshold': 0.5, 'dema_len': 57, "harvest_ratio": 0.005, "retreat_ratio": 0.005}
    def live_trading(params):
        timbersaw.setup()
        alp = AlpMacdRvsCanpctSing(money = 500, leverage = 5, sizer = 0.1, params = params, mode = 1)
        market = KlineGenerator('BTCUSDT', '5m')
        while True:
            market.update_klines()
            alp.generate_signal_position(market.kdf)
            time.sleep(10)

    def backtest(params):
        alp_backtest = AlpMacdRvsCanpctSing(money = 500, leverage = 5, sizer = 0.1, params = params, mode = 0)
        best_params, best_value =  alp_backtest.optimize_params()
        print(f"Best parameters: {best_params}")
        print(f"Best value: {best_value}")

    #live_trading(params)
    backtest(params)

