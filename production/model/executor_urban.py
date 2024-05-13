import logging
import warnings
warnings.filterwarnings("ignore")
import time
from datetime import datetime, timedelta
import sys
main_path = "/Users/rivachol/Desktop/Rivachol_v2"
sys.path.append(main_path)
from production.binance_execution.traders import Traders
import contek_timbersaw as timbersaw
import pandas as pd
import yaml
from retry import retry

class ExecPostmodern(Traders):
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
    executor = "postmodern"
    model_name = "model_urban"
    equity = 1000
    leverage = 5
    
    logger = logging.getLogger(executor)

    def __init__(self, symbol) -> None:
        super().__init__(symbol)
        self.interval = 10
        self.update_time = datetime.utcnow() - timedelta(days=1)
    
    @retry(tries=3, delay=1)  
    def _read_position(self, rel_path = "/production/signal_position.yaml") -> float:
        try:
            with open(main_path + rel_path, 'r') as stream:
                signal_position_dict = yaml.safe_load(stream)
                signal_position = float(signal_position_dict["model_position"])
                self.update_time = datetime.strptime(signal_position_dict["update_time"], "%Y-%m-%d %H:%M:%S")
            return signal_position
        except FileNotFoundError:
            self.logger.error('Position is not read from the file.')
            return 0
        
    @retry(tries=2, delay=3)
    def check_update_position(self) -> bool:
        """check if the signal position is updated"""
        signal_position = self._read_position()
        if datetime.utcnow() - self.update_time < timedelta(minutes=5):
            return True
        else:
            return False


    def check_position_diff(self, signal_position: float) -> bool:
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
        except Exception as error:
            self.logger.error(error)
            return False

    def task(self) -> bool:
        """main task of the executor"""
        self.cancel_open_orders()
        signal_position = self._read_position()
        self.logger.info(f"signal position: {signal_position}")
        if self.check_position_diff(signal_position):
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
                    time.sleep(self.interval / 10)
            except Exception as e:
                self.logger.critical(e)
                self.logger.critical("Restarting the executor in 10 seconds...")
                time.sleep(self.interval)    

if __name__ == "__main__":
    timbersaw.setup()
    executor = ExecPostmodern(symbol = "BTCUSDT")
    executor.run()
