import logging
import warnings
warnings.filterwarnings("ignore")
import sys
temp_path = "/Users/rivachol/Desktop/Rivachol_v2/Elysium"
sys.path.append(temp_path)
from binance.um_futures import UMFutures
from binance.error import ClientError
from binance_api import key, secret
import pandas as pd
from retry import retry

class ExecPostmodern:
    executor = "exec_postmodern"
    symbol = "ETHUSDT"
    slippage = -0.01
    equity = 1000
    leverage = 5
    
    logger = logging.getLogger(executor)

    def __init__(self) -> None:
        self.client = self._connect_api(key=key, secret=secret)

    @retry(ClientError, tries=3, delay=1)  
    def _connect_api(self, key, secret) -> UMFutures:
        """connect binance client with apikey and apisecret"""
        client = UMFutures(key=key, secret=secret, timeout=3)

        return client

    def _fetch_notional(self) -> float:
        """check position limit before sending order"""
        positions = pd.DataFrame(self.client.get_position_risk(recvWindow=6000))
        notional = positions.query("symbol == @self.symbol").loc[:, "notional"]
        notionalAmt = float(notional)

        return notionalAmt

    @retry(ClientError, tries=3, delay=1)       
    def _maker_buy(self, amount, ticker) -> None:
        """send post-only buy order"""
        notionalAmt = self._fetch_notional()
        if notionalAmt > self.equity * self.leverage:
            self.logger.warning("Position limit reached. No more order would be sent.")
            return None
        else:
            price = round((ticker + self.slippage), 2)
            amount = round(amount, 2)
            self.logger.info(f"Ticker: {ticker} Executing buy price:{price}")
            try:
                self.orderId = self.client.new_order(
                    symbol=self.symbol,
                    side="BUY",
                    type="LIMIT",
                    quantity=amount,
                    timeInForce="GTX",
                    price=price,
                )
                self.logger.info(f"Executing buy price:{price}")

            except ClientError as error:
                self.logger.error(error)

    @retry(ClientError, tries=3, delay=1)  
    def _maker_sell(self, amount, ticker) -> None:
        """send post-only sell order"""
        notionalAmt = self._fetch_notional()
        if notionalAmt < -self.equity * self.leverage:
            self.logger.warning("Position limit reached. No more order would be sent.")
            return None
        else:
            price = round((ticker - self.slippage), 2)
            amount = round(amount, 2)
            self.logger.info(f"Ticker: {ticker} Executing sell price:{price}")
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
                self.logger.error(error)
            
    @retry(ClientError, tries=3, delay=1)  
    def _cancel_open_orders(self) -> None:
        status = "NEW" or "PARTIALLY_FILLED"
        orders = pd.DataFrame(self.client.get_all_orders(symbol=self.symbol))
        open_orders = orders.query("status == @status")
        try:
            for orderId in open_orders["orderId"]:
                self.client.cancel_order(symbol=self.symbol, orderId=orderId)
        except ClientError as error:
            self.logger.error(error)

    def _check_position_diff(self, signal_position: float) -> bool:
        """compare actual position and signal position & fill the gap if there is one"""
        positions = pd.DataFrame(self.client.get_position_risk(recvWindow=6000))
        positionAmt = positions.query("symbol == @self.symbol").loc[:, "positionAmt"]
        actual_position = float(positionAmt)
        self.logger.info(f"actual position is {actual_position}")
        if actual_position == signal_position:
            return True
        else:
            position_diff = signal_position - actual_position
            book_ticker = self.client.book_ticker(self.symbol)
            bid_price = float(book_ticker["bidPrice"])
            ask_price = float(book_ticker["askPrice"])
            if position_diff > 0:
                self.logger.warning("@@@@@@@@@@@@  Sending Buy Order @@@@@@@@@@@@")
                self._maker_buy(position_diff, bid_price)
            else:
                self.logger.warning("@@@@@@@@@@@@ Sending Sell Order @@@@@@@@@@@@")
                self._maker_sell(-position_diff, ask_price)
            return False

    def task(self, signal_position: float) -> bool:
        """main task"""
        self._cancel_open_orders()
        self.logger.info(f"signal position: {signal_position}")
        if self._check_position_diff(signal_position):
            self.logger.info(f"Position & signals are cross checked.\n-- -- -- --")
            return True
        else:
            self.logger.warning(f"Gap to match!\n-- -- -- -- -- -- -- -- -- ")
            return False


if __name__ == "__main__":
    test = ExecPostmodern()
    test.task(0)
