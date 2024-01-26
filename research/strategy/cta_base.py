from datetime import datetime, timedelta
import time

import pandas as pd
import numpy as np

from binance.um_futures import UMFutures
from retry import retry


class BacktestEngine:
    initial_money = 100000
    leverage = 4
    comm = 0.0004

    def __init__(self, alpha_name, symbol, timeframe, start, window_days) -> None:
        self.alpha_name = alpha_name
        self.symbol = symbol
        self.timeframe = timeframe
        self.window_days = window_days
        self.start = start
        self.client = UMFutures(timeout=3)
        self.sizer = self._determine_sizer(symbol)
        self.kdf = self._gen_testdata()

    def _determine_sizer(self, symbol: str) -> int:
        if symbol == "BTCUSDT":
            sizer = 2
        elif symbol == "ETHUSDT":
            sizer = 40

        return sizer

    @retry(tries=2)
    def _gen_testdata(self) -> pd.DataFrame:
        start_time = time.time()
        data = []
        for i in range(0, self.window_days):
            istart = self.start + timedelta(days=i)
            iend = istart + timedelta(days=1)
            starttime = int(istart.timestamp() * 1000)
            endtime = int(iend.timestamp() * 1000)
            ohlcv = self.client.continuous_klines(
                self.symbol,
                "PERPETUAL",
                self.timeframe,
                startTime=starttime,
                endtime=endtime,
            )
            ohlcv.pop()
            data = data + ohlcv
        raw_kdf = pd.DataFrame(
            data,
            columns=[
                "opentime",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "closetime",
                "volume_USDT",
                "num_trade",
                "taker_buy",
                "taker_buy_volume_USDT",
                "ignore",
            ],
        )
        kdf = self._convert_kdf_datatype(raw_kdf)
        print(f"Time used to generate test data: {time.time() - start_time} seconds")
        return kdf

    def _convert_kdf_datatype(self, kdf) -> pd.DataFrame:
        kdf.opentime = [
            datetime.utcfromtimestamp(int(x) / 1000.0) for x in kdf.opentime
        ]
        kdf.open = kdf.open.astype("float")
        kdf.high = kdf.high.astype("float")
        kdf.low = kdf.low.astype("float")
        kdf.close = kdf.close.astype("float")
        kdf.volume = kdf.volume.astype("float")
        kdf.closetime = [
            datetime.utcfromtimestamp(int(x) / 1000.0) for x in kdf.closetime
        ]
        kdf.volume_USDT = kdf.volume_USDT.astype("float")
        kdf.num_trade = kdf.num_trade.astype("int")
        kdf.taker_buy = kdf.taker_buy.astype("float")
        kdf.taker_buy_volume_USDT = kdf.taker_buy_volume_USDT.astype("float")
        kdf.ignore = kdf.ignore.astype("float")
        kdf.set_index("opentime", inplace=True)

        return kdf

    def initialize_portfolio_variables(self, kdf: pd.DataFrame) -> pd.DataFrame:
        """initialize portfolio dataframe
        
        Args:
            kdf (pd.DataFrame): candle dataframe
        
        Returns:
            pd.DataFrame: portfolio dataframe
        
        """
        portfolio = kdf[["open", "high", "low", "close", "volume_USDT"]]
        portfolio["value"] = np.zeros(len(portfolio))
        portfolio["signal"] = np.zeros(len(portfolio))
        portfolio["position"] = np.zeros(len(portfolio))
        portfolio["entry_price"] = np.zeros(len(portfolio))
        portfolio["stop_price"] = np.zeros(len(portfolio))
        portfolio["stop_loss"] = np.zeros(len(portfolio))
        portfolio["stop_profit"] = np.zeros(len(portfolio))
        portfolio["unrealized_pnl"] = np.zeros(len(portfolio))
        portfolio["realized_pnl"] = np.zeros(len(portfolio))
        portfolio["commission"] = np.zeros(len(portfolio))

        return portfolio

    def calc_performance(self, result) -> dict:
        trades = {
            "total": 0,
            "win": 0,
            "loss": 0,
            "cumulative_win": 0,
            "cumulative_loss": 0,
        }

        for pnl in result["realized_pnl"]:
            if pnl > 0:
                trades["total"] += 1
                trades["win"] += 1
                trades["cumulative_win"] += pnl
            elif pnl < 0:
                trades["total"] += 1
                trades["loss"] += 1
                trades["cumulative_loss"] += pnl

        final_value = result["value"][-1] + result["unrealized_pnl"][-1]
        avg_trade_pnl = (final_value - self.initial_money) / (trades["total"] + 0.0001)
        win_ratio = trades["win"] / (trades["total"] + 0.0001)
        avg_winning = trades["cumulative_win"] / (trades["win"] + 0.0001)
        avg_losing = trades["cumulative_loss"] / (trades["loss"] + 0.0001)
        single_avg_wlr = -avg_winning / (avg_losing + 0.0001)
        ret = final_value / self.initial_money - 1
        comm_ratio = result["commission"].sum() / self.initial_money
        score =  win_ratio * single_avg_wlr * (final_value - self.initial_money) 

        sigma_sum = np.sum(
            (pnl - avg_trade_pnl) ** 2 for pnl in result["realized_pnl"] if pnl != 0
        )
        sigma = np.sqrt(sigma_sum / (trades["total"] + 0.0001))
        diysharpe = (final_value - self.initial_money) / sigma

        performances = {
            "final_value": final_value,
            "win_ratio": win_ratio,
            "single_avg_wlr": single_avg_wlr,
            "total_trades": trades["total"],
            "return": ret,
            "diysharpe": diysharpe,
            "commission": comm_ratio,
            "score": score,
        }

        return performances
