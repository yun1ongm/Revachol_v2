import logging
import warnings
warnings.filterwarnings("ignore")
import time
import sys
main_path = "/Users/rivachol/Desktop/Rivachol_v2"
sys.path.append(main_path)
from binance.um_futures import UMFutures
from binance.error import ClientError
import contek_timbersaw as timbersaw
import pandas as pd
import yaml
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
        config = self._read_config()
        self.client = self._connect_api(key=config["bn_api"]["key"], secret=config["bn_api"]["secret"])
        self.symbol = symbol
        self.slippage = self._determine_slippage(symbol)
        self.interval = 15

    def _determine_slippage(self, symbol: str) -> int:
        if symbol == "BTCUSDT":
            slippage = -6
        elif symbol == "ETHUSDT":
            slippage = -0.4

        return slippage
    
    def _read_config(self, rel_path = "/config.yaml") -> dict:
        try:
            with open(main_path + rel_path, 'r') as stream:
                config = yaml.safe_load(stream)
        except FileNotFoundError:
            self.logger.error('Config file not found')
            sys.exit(1)
        return config
    
    @retry(tries=3, delay=1)  
    def _read_position(self, rel_path = "/signal_position.yaml") -> float:
        try:
            with open(main_path + rel_path, 'r') as stream:
                signal_position_dict = yaml.safe_load(stream)
                signal_position = float(signal_position_dict["signal_position"])
            return signal_position
        except FileNotFoundError:
            self.logger.error('Position is not read from the file.')
            return 0

    @retry(tries=3, delay=1)  
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

    @retry(tries=3, delay=1)       
    def _maker_buy(self, amount, ticker) -> None:
        """send post-only buy order"""
        notionalAmt = self._fetch_notional()
        if notionalAmt > self.equity * self.leverage:
            self.logger.warning("Position limit reached. No more order would be sent.")
            return None
        else:
            price = round((ticker + self.slippage), 2)
            amount = round(amount, 3)
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

    @retry(tries=3, delay=1)  
    def _maker_sell(self, amount, ticker) -> None:
        """send post-only sell order"""
        notionalAmt = self._fetch_notional()
        if notionalAmt < -self.equity * self.leverage:
            self.logger.warning("Position limit reached. No more order would be sent.")
            return None
        else:
            price = round((ticker - self.slippage), 2)
            amount = round(amount, 3)
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
            
    @retry(tries=3, delay=1)  
    def _cancel_open_orders(self) -> None:
        try:
            orders = pd.DataFrame(self.client.get_all_orders(symbol=self.symbol))
            if not orders.empty:
                open_orders = orders.query('status == ["NEW", "PARTIALLY_FILLED"]')
                for orderId in open_orders["orderId"]:
                    self.client.cancel_order(symbol=self.symbol, orderId=orderId)
        except ClientError as error:
            self.logger.error(error)

    def _check_position_diff(self, signal_position: float) -> bool:
        """compare actual position and signal position & fill the gap if there is one"""
        try:
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
        except ClientError as error:
            self.logger.error(error)
            return False

    def task(self) -> bool:
        """main task of the executor"""
        self._cancel_open_orders()
        signal_position = self._read_position()
        self.logger.info(f"signal position: {signal_position}")
        if self._check_position_diff(signal_position):
            self.logger.info(f"Position & signals are cross checked.\n-- -- -- --")
            return True
        else:
            self.logger.warning(f"Gap to match!\n-- -- -- -- -- -- -- -- -- ")
            return False
        
    def run(self) -> None:
        while True:
            try:
                complete = self.task()
                if complete:
                    time.sleep(self.interval)
                else:
                    time.sleep(self.interval / 5)
            except Exception as e:
                self.logger.critical(e)
                self.logger.critical("Restarting the executor in 10 seconds...")
                time.sleep(10)
                continue

if __name__ == "__main__":
    timbersaw.setup()
    executor = ExecPostmodern(symbol = "BTCUSDT")
    executor.run()
