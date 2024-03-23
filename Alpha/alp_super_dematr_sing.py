import os
import logging
import time
import operator
import optuna
import matplotlib.pyplot as plt
from datetime import datetime
import pandas as pd
import sys
temp_path = "/Users/rivachol/Desktop/Rivachol_v2/"
sys.path.append(temp_path)
from backtest import BacktestFramework
from Market.kline import KlineGenerator
from Index.idx_super_dema import IdxSuperDema
from Strategy.stgy_dematr_sing import StgyDematrSing
import contek_timbersaw as timbersaw
import warnings
warnings.filterwarnings("ignore")


class AlpSuperDematrSing(BacktestFramework):
    """
        Args:
            money (float): initial money
            leverage (float): leverage
            sizer (float): sizer
        
        Return:
            position and signal in portfolio: pd.DataFrame
    """
    alpha_name = "alp_super_dematr_sing"
    index_name = "idx_super_dema"
    strategy_name = "stgy_dematr_sing"
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
        '''
        self.money = money
        self.leverage = leverage
        self.sizer = sizer
        self._set_params(params)
        if mode == 0:
            self.num_evals = 100
            self.target = "t_sharpe"
            market = KlineGenerator('BTCUSDT', '5m', mode = 0, 
                                    start = datetime(2023, 12, 14, 0, 0, 0), 
                                    window_days=100)
            self.kdf = market.kdf
            self.money = 10000
            self.leverage = 5
            self.sizer = 0.1 if self.symbol == "BTCUSDT" else 2
        else:
            self.money = money
            self.leverage = leverage
            self.sizer = sizer
    
    def _set_params(self, params:dict):
        self.sptr_len = params["sptr_len"]
        self.sptr_k = params["sptr_k"]
        self.dema_len = params["dema_len"]
        self.atr_profit = params["atr_profit"]
        self.atr_loss = params["atr_loss"]

    def generate_signal_position(self, kdf:pd.DataFrame) -> float:
        try:
            index = IdxSuperDema(kdf, self.sptr_len, self.sptr_k, self.dema_len)
            strategy = StgyDematrSing(self.atr_profit, self.atr_loss, self.money, self.leverage, self.sizer)
            idx_signal = index.generate_dematr_signal()
            update_time = idx_signal.index[-1]
            stgy_signal = strategy.generate_portfolio(idx_signal)
            position = stgy_signal[f"position"][-1]
            signal = stgy_signal[f"signal"][-1]
            entry_price = stgy_signal["entry_price"][-1]
            stop_loss = stgy_signal["stop_loss"][-1]
            stop_profit = stgy_signal["stop_profit"][-1]
            signal_position ={
                "position": position,
                "signal": signal,
                "entry_price": entry_price,
                "stop_loss": stop_loss,
                "stop_profit": stop_profit,
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
        index = IdxSuperDema(self.kdf, self.sptr_len, self.sptr_k, self.dema_len)
        strategy = StgyDematrSing(self.atr_profit, self.atr_loss, self.money, self.leverage, self.sizer)
        idx_signal = index.generate_dematr_signal()
        portfolio = strategy.generate_portfolio(idx_signal)
        return portfolio

    def evaluate_performance(self, result):
        perf = self.calculate_performance(result)

        return perf
    
    def objective(self, trial):
        kwargs = {
            "sptr_len": trial.suggest_int("sptr_len",  8, 48, step=2),
            "sptr_k": trial.suggest_float("sptr_k", 2.5, 4, step=0.5),
            "dema_len": trial.suggest_int("dema_len", 12, 60, step=3),
            "atr_profit": trial.suggest_int("atr_profit", 2, 6),
            "atr_loss": trial.suggest_int("atr_loss", 2, 4),
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

    def _write_to_log(self, trials) -> None:
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
    params = {'sptr_len': 24, 'sptr_k': 3.0, 'dema_len': 15, 'atr_profit': 3, 'atr_loss': 4}
    def live_trading(params):
        timbersaw.setup()
        alp = AlpSuperDematrSing(money = 500, leverage = 5, sizer = 0.1, params = params, mode = 1)
        market = KlineGenerator('BTCUSDT', '5m')
        while True:
            market.update_klines()
            alp.generate_signal_position(market.kdf)
            time.sleep(10)

    def backtest(params):
        alp_backtest = AlpSuperDematrSing(money = 500, leverage = 5, sizer = 0.1, params = params, mode = 0)
        best_params, best_value =  alp_backtest.optimize_params()
        print(f"Best parameters: {best_params}")
        print(f"Best value: {best_value}")

    #live_trading(params)
    backtest(params)

    
