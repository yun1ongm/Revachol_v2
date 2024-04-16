import logging
import warnings
warnings.filterwarnings("ignore")
import time
import sys
main_path = "/Users/rivachol/Desktop/Rivachol_v2/"
sys.path.append(main_path)
from binance.um_futures import UMFutures
import okx.Account as Account
import okx.Trade as Trade
import contek_timbersaw as timbersaw
import pandas as pd
import yaml
from retry import retry

class ExecCrystallball:
    """
    Executor Crystallball is a class that realizes the trading strategy of exchange arbitrage.
    Args:
        
    Attributes:
  
    """
    executor = "exec_crystallball"
    equity = 100
    leverage = 5

    logger = logging.getLogger(executor)

    def __init__(self) -> None:
        config = self._read_config()
        self._connect_api(config)
        self.binance_symbol_mapping = self._generate_mapping(self.symbols_list, 'binance')
        self.okx_symbol_mapping = self._generate_mapping(self.symbols_list, 'okx')

    def _generate_mapping(self, symbols_list: list, exchange: str):
        mapping = {}
        for symbol in symbols_list:
            base = symbol.split('USD')[0]
            if exchange == 'binance':
                mapping[symbol] = base + 'USDT'
            elif exchange == 'okx':
                mapping[symbol] = base + '-USDT-SWAP'
        return mapping
    
    def _read_config(self, rel_path = "config.yaml") -> dict:
        try:
            with open(main_path + rel_path, 'r') as stream:
                config = yaml.safe_load(stream)
        except FileNotFoundError:
            self.logger.error('Config file not found')
            sys.exit(1)
        return config

    @retry(tries=3, delay=1)  
    def _connect_api(self, config) ->None:
        """connect binance client with apikey and apisecret"""
        self.binance = UMFutures(key = config["bn_api"]["key"], secret = config["bn_api"]["secret"])
        self.okx =  Account.AccountAPI(config["okx_api"]["key"], config["okx_api"]["secret"], config["okx_api"]["passphrase"], flag="0", debug=False)
        self.okx_trade = Trade.TradeAPI(config["okx_api"]["key"], config["okx_api"]["secret"], config["okx_api"]["passphrase"], flag="0", debug=False)
        self.logger.info("API connected successfully")

    def _check_positions_balance(self) -> tuple:
        """check positions balance"""
        bn_positions = pd.DataFrame(self.binance.get_position_risk(recvWindow=6000))
        okx_positions = pd.DataFrame(self.okx.get_position_risk()['data'][0]['posData'])
        self._check_by_symbol(bn_positions, okx_positions)

        return bn_notional_total, okx_notional_total
    
    def _check_positions_limit(self, bn_positions, okx_positions) -> None:
        """check position limit before sending order"""
        bn_notional_total = bn_positions['notional'].sum()
        okx_notional_total = okx_positions['notional'].sum()
        if bn_notional_total >= self.equity * self.leverage or okx_notional_total >= self.equity * self.leverage:
            self.logger.warning("Position limit reached. No more order would be sent.")
        else:
            self.logger.info(f"bn position level: {bn_notional_total/(self.equity * self.leverage)} 
                             okx position level: {okx_notional_total/(self.equity * self.leverage)}")
    def _check_positions(self) -> bool:
        bn_notional_total, okx_notional_total = self._fetch_notional()
        if bn_notional_total >= self.equity * self.leverage or okx_notional_total >= self.equity * self.leverage:
            self.logger.warning("Position limit reached. No more order would be sent.")
            return False
        else:
            self.logger.info(f"bn position level: {bn_notional_total/(self.equity * self.leverage)} 
                             okx position level: {okx_notional_total/(self.equity * self.leverage)}")
            return True
        
    def _check_holding_time(self):
        """check holding time"""
        pass
 
    def _bn_fok_buy(self, symbol, amount, price) -> None:
        try:
            self.orderId = self.binance.new_order(
                    symbol= symbol,
                    side="BUY",
                    type="LIMIT",
                    quantity=amount,
                    timeInForce="FOK",
                    price=price,
                )
            self.logger.info(f"Binance Trade made: buy price:{price} amount:{amount}")

        except Exception as error:
            self.logger.error(error)
            

    def _bn_fok_sell(self, symbol, amount, price) -> None:
        try:
            self.orderId = self.binance.new_order(
                    symbol= symbol,
                    side="SELL",
                    type="LIMIT",
                    quantity=amount,
                    timeInForce="FOK",
                    price=price,
                )
            self.logger.info(f"Binance Trade made: sell price:{price} amount:{amount}")

        except Exception as error:
            self.logger.error(error)

    def _okx_fok_buy(self, symbol, amount, price) -> None:
        try:
            amount = int(amount)
            res = self.okx_trade.place_order(instId=symbol, tdMode="cross", side="buy", ordType="limit", sz=amount, px=price, ccy="USDT")
            self.logger.info(f"Okx Trade made: buy price:{price} amount:{amount}")

        except Exception as error:
            self.logger.error(error)

    def _okx_fok_sell(self, symbol, amount, price) -> None:
        try:
            amount = int(amount)
            res = self.okx_trade.place_order(instId=symbol, tdMode="cross", side="sell", ordType="limit", sz=amount, px=price, ccy="USDT")
            self.logger.info(f"Okx Trade made: buy price:{price} amount:{amount}")

        except Exception as error:
            self.logger.error(error)

    def close_positions(self, symbol, amount) -> None:
        pass
    
    def task(self) -> bool:
        """main task of the executor"""
        if self._check_position_threshold():
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
    executor = ExecCrystallball()
    executor.run()
