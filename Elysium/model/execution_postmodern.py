import logging
import os
import warnings
warnings.filterwarnings("ignore")
import sys
temp_path = "/Users/rivachol/Desktop/Elysium"
sys.path.append(temp_path)
from binance.um_futures import UMFutures
from binance.error import ClientError
from binance_api import key, secret
import pandas as pd


class ExecPostmodern:
    executor = "exec_postmodern"
    symbol = "ETHUSDT"
    slippage = -0.1
    equity = 200
    leverage = 5

    def __init__(self, interval) -> None:
        self._init_logger()
        self.client = self._connect_api(key=key, secret=secret)
        self.interval = interval

    def _init_logger(self) -> None:
        self.logger = logging.getLogger(self.executor)
        self.logger.setLevel(logging.INFO)
        log_file = f"log_book/{self.executor}.log"
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s, %(message)s")
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

    def _log(self, string) -> None:
        self.logger.info(string)

    def _connect_api(self, key, secret) -> UMFutures:
        """connect binance client with apikey and apisecret"""
        client = UMFutures(key=key, secret=secret, timeout=3)

        return client

    def _fetch_notional(self) -> float:
        """check position limit before sending order"""
        positions = pd.DataFrame(self.client.get_position_risk(recvWindow=6000))
        notional = positions.query("symbol == @self.symbol").loc[:, "notional"]
        notionalAmt = abs(float(notional))

        return notionalAmt

    def _maker_buy(self, amount, ticker) -> None:
        """send post-only buy order"""
        notionalAmt = self._fetch_notional()
        if notionalAmt > self.equity * self.leverage:
            self._log("Position limit reached. No more order would be sent.")
            return None
        else:
            price = round((ticker + self.slippage), 2)
            self._log(f"Ticker: {ticker} Executing buy price:{price}")
            try:
                self.orderId = self.client.new_order(
                    symbol=self.symbol,
                    side="BUY",
                    type="LIMIT",
                    quantity=amount,
                    timeInForce="GTX",
                    price=price,
                )
                self._log(f"Executing buy price:{price}")

            except ClientError as error:
                self._log(error)

    def _maker_sell(self, amount, ticker) -> None:
        """send post-only sell order"""
        notionalAmt = self._fetch_notional()
        if notionalAmt < -self.equity * self.leverage:
            self._log("Position limit reached. No more order would be sent.")
            return None
        else:
            price = round((ticker - self.slippage), 2)
            self._log(f"Ticker: {ticker} Executing sell price:{price}")
            try:
                self.orderId = self.client.new_order(
                    symbol=self.symbol,
                    side="SELL",
                    type="LIMIT",
                    quantity=amount,
                    timeInForce="GTX",
                    price=price,
                )

            except ClientError as error:
                self._log(error)

    def _cancel_open_orders(self) -> None:
        status = "NEW" or "PARTIALLY_FILLED"
        orders = pd.DataFrame(self.client.get_all_orders(symbol=self.symbol))
        open_orders = orders.query("status == @status")
        try:
            for orderId in open_orders["orderId"]:
                self.client.cancel_order(symbol=self.symbol, orderId=orderId)
        except:
            pass

    def _check_position_diff(self, signal_position: float) -> bool:
        """compare actual position and signal position & fill the gap if there is one"""
        positions = pd.DataFrame(self.client.get_position_risk(recvWindow=6000))
        positionAmt = positions.query("symbol == @self.symbol").loc[:, "positionAmt"]
        actual_position = float(positionAmt)
        self._log(f"actual position is {actual_position}")
        if actual_position == signal_position:
            return True
        else:
            position_diff = signal_position - actual_position
            ticker = self.client.ticker_price(self.symbol)
            price = float(ticker["price"])
            if position_diff > 0:
                self._log("@@@@@@@@@@@@  Sendong Buy Order @@@@@@@@@@@@")
                self._maker_buy(position_diff, price)
            else:
                self._log("@@@@@@@@@@@@ Sending Sell Order @@@@@@@@@@@@")
                self._maker_sell(-position_diff, price)
            return False

    def task(self, signal_position: float) -> None:
        """main task"""
        self._cancel_open_orders()
        signal_position = round(signal_position, 2)
        self._log(f"signal position: {signal_position}")
        if self._check_position_diff(signal_position):
            self._log(f"Position & signals are cross checked.\n-- -- -- --")
        else:
            self._log(f"Gap to match!\n-- -- -- -- -- -- -- -- -- ")


if __name__ == "__main__":
    test = ExecPostmodern(10)
    test.task(0)
