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
    """
    Executor Postmodern is a class that sends post-only orders to binance futures
    Args:
        symbol: str: symbol of the trading pair
    Attributes:
        client: UMFutures: binance futures client
        symbol: str: symbol of the trading pair
        slippage: int: slippage for the trading pair
        equity: int: equity of the trading account
        leverage: int: leverage of the trading account
        logger: logger for the class
    """
    executor = "exec_postmodern"
    equity = 1000
    leverage = 5
    
    logger = logging.getLogger(executor)

    def __init__(self, symbol) -> None:
        self.client = self._connect_api(key=key, secret=secret)
        self.symbol = symbol
        self.slippage = self._determine_slippage(symbol)

    def _determine_slippage(self, symbol: str) -> int:
        if symbol == "BTCUSDT":
            slippage = -5
        elif symbol == "ETHUSDT":
            slippage = -0.3

        return slippage

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
        orders = pd.DataFrame(self.client.get_all_orders(symbol=self.symbol))
        if not orders.empty:
            try:
                open_orders = orders.query('status == ["NEW", "PARTIALLY_FILLED"]')
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
    import contek_timbersaw as timbersaw
    timbersaw.setup()
    test = ExecPostmodern(symbol = "BTCUSDT")
    complete = test.task(0)
    print(complete)
