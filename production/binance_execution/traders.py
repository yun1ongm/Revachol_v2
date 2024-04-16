import logging
import warnings
warnings.filterwarnings("ignore")
import sys
main_path = "/Users/rivachol/Desktop/Rivachol_v2"
sys.path.append(main_path)
from binance.um_futures import UMFutures
import pandas as pd
import yaml
from retry import retry

class Traders:

    logger = logging.getLogger('account')

    def __init__(self, symbol) -> None:
        config = self._read_config()
        self.client = self._connect_api(key=config["bn_api"]["key"], secret=config["bn_api"]["secret"])
        self.symbol = symbol
        self.slippage  = self._determine_slippage(symbol)

    def _read_config(self) -> dict:
        rel_path = "/production/config.yaml"
        try:
            with open(main_path + rel_path, 'r') as stream:
                config = yaml.safe_load(stream)
        except FileNotFoundError:
            self.logger.error('Config file not found')
            sys.exit(1)
        return config

    @retry(tries=2, delay=1)  
    def _connect_api(self, key, secret) -> UMFutures:
        """connect binance client with apikey and apisecret"""
        client = UMFutures(key=key, secret=secret, timeout=3)

        return client
    
    def _determine_slippage(self, symbol: str) -> float:
        if symbol == "BTCUSDT" or "BTCUSDC":
            slippage = -7
            self.digit = 1
        elif symbol == "ETHUSDT" or "ETHUSDC":
            slippage = -0.35
            self.digit = 2
        elif symbol == "SOLUSDT" or "SOLUSDC":
            slippage = -0.07
            self.digit = 3
        return slippage
    
    @retry(tries=2, delay=1)       
    def _maker_buy(self, amount, ticker) -> None:
        """send post-only buy order"""
        price = round((ticker + self.slippage), self.digit)
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

        except Exception as error:
            self.logger.error(error)

    @retry(tries=3, delay=1)  
    def _maker_sell(self, amount, ticker) -> None:
        """send post-only sell order"""
        price = round((ticker - self.slippage), self.digit)
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

        except Exception as error:
            self.logger.error(error)
    
    @retry(tries=1, delay=1)       
    def send_batch_order(self, order_df:pd.DataFrame) -> list:
        """send buy and sell orders based on the maker price dataframe
        Args:
            order_df (pd.DataFrame): dataframe with symbol, lot, buy1, sell1, buy2, sell2
            response (list): response from binance api
            """
        symbol =order_df["symbol"][-1]
        lot =   order_df["lot"][-1]
        buy1 =  round(order_df["buy1"][-1], self.digit)
        sell1 =  round(order_df["sell1"][-1], self.digit)
        buy2 =  round(order_df["buy2"][-1], self.digit)
        sell2 =  round(order_df["sell2"][-1], self.digit)
        self.logger.info(f"close: {order_df.close[-1]} buy1: {buy1} sell1: {sell1} buy2: {buy2} sell2: {sell2}")
        try:
            batchOrders = [
                {
                    "symbol":symbol,
                    "side": "SELL",
                    "type": "LIMIT",
                    "quantity": f"{lot}",
                    "timeInForce": "GTX",
                    "reduceOnly": "false",
                    "price": f"{sell1}"
                },
                {
                    "symbol":symbol,
                    "side": "SELL",
                    "type": "LIMIT",
                    "quantity": f"{lot*2}",
                    "timeInForce": "GTX",
                    "reduceOnly": "false",
                    "price": f"{sell2}"
                },
                {
                    "symbol":symbol,
                    "side": "BUY",
                    "type": "LIMIT",
                    "quantity": f"{lot}",
                    "timeInForce": "GTX",
                    "reduceOnly": "false",
                    "price": f"{buy1}"
                },
                {
                    "symbol":symbol,
                    "side": "BUY",
                    "type": "LIMIT",
                    "quantity": f"{lot*2}",
                    "timeInForce": "GTX",
                    "reduceOnly": "false",
                    "price": f"{buy2}"
                },
                ]
            response = self.client.new_batch_order(batchOrders)
            return response
        except Exception as error:
            self.logger.error(error)
            
    @retry(tries=1, delay=1)  
    def cancel_open_orders(self) -> None:
        try:
            orders = pd.DataFrame(self.client.get_all_orders(symbol= self.symbol))
            orders_unfin = 0
            if not orders.empty:
                open_orders = orders.query('status == ["NEW", "PARTIALLY_FILLED"]')
                for orderId in open_orders["orderId"]:
                    orders_unfin += 1
                    self.client.cancel_order(symbol= self.symbol, orderId=orderId)
        except Exception as error:
            self.logger.error(error)
        self.logger.info(f"Cancelled {orders_unfin} open orders.")

    def fetch_positions(self) -> tuple:
        try:
            positions = pd.DataFrame(self.client.get_position_risk(recvWindow=6000))
            unpnl = positions.query("symbol == @self.symbol").loc[:, "unRealizedProfit"]
            unpnl_float = float(unpnl)
            notional = positions.query("symbol == @self.symbol").loc[:, "notional"]
            notional_float = float(notional)
            return unpnl_float, notional_float
        except Exception as error:
            self.logger.error(error)
    
    def check_threshold(self, threshold) -> bool:
        """ check if the position is over the threshold"""
        try:
            unpnl_float, notional_float= self.fetch_positions()
            if unpnl_float < -threshold or notional_float > 5000:
                self.logger.critical(f"*** unrealizedPnl or notional is over the threshold. Close the position ***")
                return True
            elif unpnl_float >  threshold or notional_float > 5000:
                self.logger.critical(f"*** unrealizedPnl or notional is over the threshold. Close the position ***")
                return True
            else:
                return False
        except Exception as error:
            self.logger.error(error)
            return False
    
    def close_position(self) -> None:
        try:
            positions = pd.DataFrame(self.client.get_position_risk(recvWindow=6000))
            positionAmt = positions.query("symbol == @self.symbol").loc[:, "positionAmt"]
            position = float(positionAmt)
            if position > 0:
                self.client.new_order(
                    symbol=self.symbol,
                    side="SELL",
                    type="MARKET",
                    quantity=abs(position),
                )
            elif position < 0:
                self.client.new_order(
                    symbol=self.symbol,
                    side="BUY",
                    type="MARKET",
                    quantity=abs(position),
                )
        except Exception as error:
            self.logger.error(error)

