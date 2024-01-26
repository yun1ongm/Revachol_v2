from datetime import datetime, timedelta
import time

import pandas as pd
import numpy as np

import ccxt
from retry import retry


class BacktestEngine:
    initial_money = 100000
    comm = 0.0004

    def __init__(self, alpha_name, symbol, timeframe, start, window_days) -> None:
        self.alpha_name = alpha_name
        self.symbol = symbol
        self.timeframe = timeframe
        self.window_days = window_days
        self.start = start
        self.cients = self._init_cex_client()
        self.sizer = self._determine_sizer(symbol)
        self.kdf = self._fetch_testdata()
        self.pfdf_volume = self._initialize_portfolio_variables(self.kdf)

    def _init_cex_client(self) -> dict:
        self.binance = ccxt.binanceusdm()
        self.bitget = ccxt.bitget()
        self.bybit = ccxt.bybit()
        self.okx = ccxt.okx()
        self.coinbase = ccxt.coinbase()
        self.upbit = ccxt.upbit()
        cex_clients = {
            'binance': self.binance,
            'bitget': self.bitget,
            'bybit': self.bybit,
            'okx': self.okx,
            'coinbase': self.coinbase,
            'upbit': self.upbit,
        }
        return cex_clients

    def _determine_sizer(self, symbol: str) -> int:
        if symbol == "BTCUSDT":
            sizer = 2
        elif symbol == "ETHUSDT":
            sizer = 40

        return sizer

    @retry(tries=2)
    def _fetch_binance_data(self) -> pd.DataFrame:
        try:
            self.binance.load_markets()
            market = self.binance.market(self.symbol)
            params = {
            'pair': market['id'],
            'contractType': 'PERPETUAL',
            'interval': self.binance.timeframes[self.timeframe],
            'startTime': int(self.start.timestamp() * 1000),
            'endTime': int(self.end.timestamp() * 1000),
            'limit': 500,
            }
            ohlcvs = self.binance.fapiPublicGetContinuousKlines(params)
            raw_kdf = pd.DataFrame(ohlcvs,columns=[
                "datetime",
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

            kdf.datetime = [
                datetime.utcfromtimestamp(int(x) / 1000.0) for x in kdf.datetime
            ]
            kdf.open = kdf.open.astype("float")
            kdf.high = kdf.high.astype("float")
            kdf.low = kdf.low.astype("float")
            kdf.close = kdf.close.astype("float")
            kdf.volume = kdf.volume.astype("float")
            kdf = kdf.set_index("datetime")
            kdf.closetime = [
                datetime.utcfromtimestamp(int(x) / 1000.0) for x in kdf.closetime
            ]
            kdf.volume_USDT = kdf.volume_USDT.astype("float")
            kdf.num_trade = kdf.num_trade.astype("int")
            kdf.taker_buy = kdf.taker_buy.astype("float")
            kdf.taker_buy_volume_USDT = kdf.taker_buy_volume_USDT.astype("float")
            kdf.ignore = kdf.ignore.astype("float")

            return kdf

        except Exception as e:
            print(type(e).__name__, str(e))
    
    def _fetch_test_data
        data = []
        for i in range(0, self.window_days):
            istart = self.start + timedelta(days=i)
            iend = istart + timedelta(days=1)
            starttime = int(istart.timestamp() * 1000)
            for client in self.cients.values():
                client.fetch_ohlcv(self.symbol,self.timeframe,limit=500)
            ohlcv = 
                self.symbol,
                "PERPETUAL",
                self.timeframe,
                startTime=starttime,
                endtime=endtime,
            )
            ohlcv.pop()
            data = data + ohlcv


        return kdf






    def _initialize_portfolio_variables(self, kdf: pd.DataFrame) -> pd.DataFrame:
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

    def run_stretegy_volume(
        self, kdf_signal: pd.DataFrame, volume_k: float
    ) -> pd.DataFrame:
        """this strategy aims to close position with high volume candle

        Args:
            kdf_signal (pd.DataFrame): dataframe with signal
            volume_k (float): volume threshold

        """
        value = self.initial_money
        position = 0

        for index, row in kdf_signal.iterrows():
            signal = row.signal
            close = row.close
            volume_USDT = row.volume_USDT
            volume_ema = row.volume_ema
            stop_price = row.stop_price
            realized_pnl = 0
            commission = 0
            volume_condition = volume_USDT >= volume_k * volume_ema

            if position > 0:
                unrealized_pnl = (close - entry_price) * position
                if close <= stop_price or volume_condition:
                    value += unrealized_pnl - self.comm * self.sizer * close
                    position = 0
                    realized_pnl = unrealized_pnl
                    commission = self.comm * self.sizer * close
                elif signal == -1:
                    entry_price = close
                    position = -self.sizer
                    value += unrealized_pnl - 2 * self.comm * self.sizer * close
                    realized_pnl = unrealized_pnl
                    commission = 2 * self.comm * self.sizer * close

            elif position < 0:
                unrealized_pnl = (close - entry_price) * position
                if close >= stop_price or volume_condition:
                    value += unrealized_pnl - self.comm * self.sizer * close
                    position = 0
                    realized_pnl = unrealized_pnl
                    commission = self.comm * self.sizer * close
                elif signal == 1:
                    entry_price = close
                    position = self.sizer
                    value += unrealized_pnl - 2 * self.comm * self.sizer * close
                    realized_pnl = unrealized_pnl
                    commission = 2 * self.comm * self.sizer * close

            else:
                entry_price = 0
                stop_price = 0
                unrealized_pnl = 0

                if signal == 1:
                    entry_price = close
                    position = self.sizer
                    value -= self.comm * self.sizer * close
                    commission = self.comm * self.sizer * close

                elif signal == -1:
                    entry_price = close
                    position = -self.sizer
                    value -= self.comm * self.sizer * close
                    commission = self.comm * self.sizer * close

            self.pfdf_volume["value"].at[index] = value
            self.pfdf_volume["signal"].at[index] = signal
            self.pfdf_volume["position"].at[index] = position
            self.pfdf_volume["entry_price"].at[index] = entry_price
            self.pfdf_volume["stop_price"].at[index] = stop_price
            self.pfdf_volume["unrealized_pnl"].at[index] = unrealized_pnl
            self.pfdf_volume["realized_pnl"].at[index] = realized_pnl
            self.pfdf_volume["commission"].at[index] = commission

        return self.pfdf_volume

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

        sigma_sum = np.sum(
            (pnl - avg_trade_pnl) ** 2 for pnl in result["realized_pnl"] if pnl != 0
        )
        sigma = np.sqrt(sigma_sum / (trades["total"] + 0.0001))
        diysharpe = (final_value - self.initial_money) / sigma

        performances = {
            "final_value": final_value,
            "avg_trade_pnl": avg_trade_pnl,
            "win_ratio": win_ratio,
            "single_avg_wlr": single_avg_wlr,
            "total_trades": trades["total"],
            "return": ret,
            "diysharpe": diysharpe,
        }

        return performances
