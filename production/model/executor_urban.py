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

    def __init__(self, market) -> None:
        super().__init__(market)
        self.interval = 10
        self.update_time = datetime.utcnow() - timedelta(days=1)
    
    @retry(tries=3, delay=1)  
    def _read_position(self, rel_path = "/production/signal_position.yaml") -> float:
        try:
            with open(main_path + rel_path, 'r') as stream:
                signal_position_dict = yaml.safe_load(stream)
                model_position = float(signal_position_dict[self.model_name]["model_position"])
                self.update_time = datetime.strptime(signal_position_dict[self.model_name]["update_time"], "%Y-%m-%d %H:%M:%S")
                self.logger.info(f"Retrieve signal position: {model_position} update_time:{self.update_time}")
            return model_position
        except FileNotFoundError:
            self.logger.error('Position is not read from the file.')
            return 0

    def check_position_diff(self, signal_position: float) -> bool:
        """compare actual position and signal position & fill the gap if there is one"""
        try:
            positions = pd.DataFrame(self.client.get_position_risk(recvWindow=6000))
            positionAmt = positions.query("symbol == @self.market").loc[:, "positionAmt"]
            actual_position = float(positionAmt)
            self.logger.info(f"actual position is {actual_position}")
            if actual_position == signal_position:
                self.logger.info(f"Position & signals are cross checked.\n-- -- -- --")
                return True
            else:
                self.logger.warning(f"Gap to match!\n-- -- -- -- -- -- -- -- -- ")
                position_diff = signal_position - actual_position
                book_ticker = self.client.book_ticker(self.market)
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
        compelete = self.check_position_diff(self._read_position())
        return compelete
        
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
                time.sleep(self.interval)    

if __name__ == "__main__":
    timbersaw.setup()
    executor = ExecPostmodern(market = "BTCUSDT")
    executor.run()
