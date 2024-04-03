import os
import logging
import operator
import optuna
from datetime import datetime
import pandas as pd
import pandas_ta as pta
import sys
temp_path = "/Users/rivachol/Desktop/Rivachol_v2/"
sys.path.append(temp_path)
from Market.kline import KlineGenerator
from Strategy.stgy_makerjay import StgyMakerjay
import warnings
warnings.filterwarnings("ignore")

class AlpPopinjay:
    alpha_name = "alp_popinjay"
    sytayegy_name = "stgy_makerjay"
    symbol = "ETHUSDC"
    timeframe = "1m"
    logger = logging.getLogger(alpha_name)

    def __init__(self, money, params:dict, mode = 0) -> None:
        '''initialize the parameters
        Args:
        money: float
        sizer: float
        params: dict
        mode: int (0 for backtest, 1 for live trading)
        '''
        self._set_params(params)
        if mode == 0:
            self.num_evals = 100
            self.target = "score"
            market = KlineGenerator(self.symbol, self.timeframe, mode = 0, 
                                    start = datetime(2024, 3, 1, 0, 0, 0), 
                                    window_days=30)
            self.kdf = market.kdf
            self.money = money
        else:
            self.money = money
    
    def _set_params(self, params:dict) -> None:
        self.atr_len = params["atr_len"]
        self.threshold = params["threshold"]
        self.lot_k = params["lot_k"]

    def generate_maker_price(self, kdf:pd.DataFrame) -> dict:       
        kdf["atr"] = pta.atr(kdf["high"], kdf["low"], kdf["close"], length = self.atr_len, mamode = "EMA")
        kdf['buy1'] = kdf['close'].shift(1) - kdf['atr'].shift(1) 
        kdf['sell1'] = kdf['close'].shift(1) + kdf['atr'].shift(1) 
        kdf['buy2'] = kdf['close'].shift(1) - kdf['atr'].shift(1) * 2
        kdf['sell2'] = kdf['close'].shift(1) + kdf['atr'].shift(1) * 2
        return kdf[['high', 'low', 'close', 'volume_U', 'buy1', 'sell1', 'buy2', 'sell2']]
    
    def get_backtest_result(
        self, params:dict
    ) -> pd.DataFrame:
        self._set_params(params)
        strategy = StgyMakerjay(money = self.money, threshold = self.threshold, lot_k = self.lot_k)
        maker_price_df = self.generate_maker_price(self.kdf)
        portfolio = strategy.generate_portfolio(maker_price_df)
        return portfolio
    
    def evaluate(self, portfolio) -> float:
        strategy = StgyMakerjay(money = self.money, threshold = self.threshold, lot_k = self.lot_k)
        result =  strategy.calculate_performance(portfolio)
        return result
    
    def objective(self, trial):
        kwargs = {
            "atr_len": trial.suggest_int("atr_len", 5, 45),
            "threshold": trial.suggest_int("threshold", 50, 300, step=10),
            "lot_k": trial.suggest_float("lot_k", 1, 3, step=0.2),
        }
        portfolio = self.get_backtest_result(kwargs)
        result = self.evaluate(portfolio)

        return result[self.target]
    
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
            portfolio = self.get_backtest_result(trial.params)
            self.output_portfolio(portfolio,i+1)
            result = self.evaluate(portfolio)
            log_message += f"  Performance: {result}\n\n"

        self._log(log_message)

    def output_portfolio(self, portfolio:pd.DataFrame, number) -> None:
        os.makedirs("result_book", exist_ok=True)
        start_date = self.kdf.index[0].strftime("%Y-%m-%d")
        end_date = self.kdf.index[-1].strftime("%Y-%m-%d")
        portfolio.to_csv(f"result_book/{self.alpha_name}_{start_date}to{end_date}_{number}.csv")

if __name__ == "__main__":
    params = {'atr_len': 13, 'threshold': 60, 'lot_k': 1.0}
    def backtest(params):
        alp_backtest = AlpPopinjay(money = 2000, params = params, mode = 0)
        # portfolio = alp_backtest.get_backtest_result(params)
        # res = alp_backtest.evaluate(portfolio)
        # print(res)

        best_params, best_value =  alp_backtest.optimize_params()
        print(f"Best parameters: {best_params}")
        print(f"Best value: {best_value}")

    #live_trading(params)
    backtest(params)

