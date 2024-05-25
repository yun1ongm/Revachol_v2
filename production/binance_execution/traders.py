import sys
main_path = "/Users/rivachol/Desktop/Rivachol_v2"
sys.path.append(main_path)
import warnings
warnings.filterwarnings("ignore")
# -*- coding: utf-8 -*-

import logging
from binance.um_futures import UMFutures
import pandas as pd
import yaml
from retry import retry

class Traders:
    columns = [
        "opentime",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "closetime",
        "volume_U",
        "num_trade",
        "taker_buy",
        "taker_buy_volume_U",
        "ignore",
        ]

    logger = logging.getLogger(__name__)

    def __init__(self, market) -> None:
        self.config = self._read_config()
        self.client = self._connect_api(key=self.config["bn_api"]["key"], secret=self.config["bn_api"]["secret"])
        self.market= market
        self.slippage  = self._determine_slippage(market)
        self.try_count = 0

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
    
    def _determine_slippage(self, market: str) -> float:
        self.threshold = 100
        if market == "BTCUSDT" or "BTCUSDC":
            slippage = -7
            self.digit = 1
        elif market == "ETHUSDT" or "ETHUSDC":
            slippage = -0.35
            self.digit = 2
        elif market == "SOLUSDT" or "SOLUSDC":
            slippage = -0.07
            self.digit = 3
        return slippage
    
    @retry(tries=2, delay=1)       
    def _maker_buy(self, amount, ticker) -> dict:
        """send post-only buy order"""
        price = round((ticker + self.slippage), self.digit)
        amount = round(amount, 3)
        self.logger.info(f"Ticker: {ticker} Executing buy price:{price}")
        try:
            response = self.client.new_order(
                symbol=self.market,
                side="BUY",
                type="LIMIT",
                quantity=amount,
                timeInForce="GTX",
                price=price,
            )
            self.logger.info(f"Executing buy price:{price}")
            return response

        except Exception as error:
            self.logger.error(error)

    @retry(tries=3, delay=1)  
    def _maker_sell(self, amount, ticker) -> dict:
        """send post-only sell order"""
        price = round((ticker - self.slippage), self.digit)
        amount = round(amount, 3)
        self.logger.info(f"Ticker: {ticker} Executing sell price:{price}")
        try:
            response = self.client.new_order(
                symbol=self.market,
                side="SELL",
                type="LIMIT",
                quantity=amount,
                timeInForce="GTX",
                price=price,
            )
            return response

        except Exception as error:
            self.logger.error(error)
    
    @retry(tries=1, delay=1)       
    def send_batch_order(self, orders_df:pd.DataFrame) -> list:
        """send buy and sell orders based on the maker price dataframe
        Args:
            order_df (pd.DataFrame): dataframe with symbol, lot, buy1, sell1, buy2, sell2
            response (list): response from binance api
            """
        market = orders_df.index[-1]
        lot = orders_df["lot"][-1]
        buy1 =  round(orders_df["buy1"][-1], self.digit)
        sell1 =  round(orders_df["sell1"][-1], self.digit)
        buy2 =  round(orders_df["buy2"][-1], self.digit)
        sell2 =  round(orders_df["sell2"][-1], self.digit)
        try:
            batchOrders = [
                {
                    "symbol":market,
                    "side": "SELL",
                    "type": "LIMIT",
                    "quantity": f"{lot}",
                    "timeInForce": "GTC",
                    "reduceOnly": "false",
                    "price": f"{sell1}"
                },
                {
                    "symbol":market,
                    "side": "SELL",
                    "type": "LIMIT",
                    "quantity": f"{lot*2}",
                    "timeInForce": "GTC",
                    "reduceOnly": "false",
                    "price": f"{sell2}"
                },
                {
                    "symbol":market,
                    "side": "BUY",
                    "type": "LIMIT",
                    "quantity": f"{lot}",
                    "timeInForce": "GTC",
                    "reduceOnly": "false",
                    "price": f"{buy1}"
                },
                {
                    "symbol":market,
                    "side": "BUY",
                    "type": "LIMIT",
                    "quantity": f"{lot*2}",
                    "timeInForce": "GTC",
                    "reduceOnly": "false",
                    "price": f"{buy2}"
                },
                ]
            response = self.client.new_batch_order(batchOrders)
            self.logger.info(f"batch order sent-- buy1: {buy1} sell1: {sell1} buy2: {buy2} sell2: {sell2}")
            return response
        except Exception as error:
            self.logger.error(error)
             
    def cancel_open_orders(self) -> None:
        try:
            orders = pd.DataFrame(self.client.get_all_orders(symbol= self.market))
            orders_unfin = 0
            if not orders.empty:
                open_orders = orders.query('status == ["NEW", "PARTIALLY_FILLED"]')
                for orderId in open_orders["orderId"]:
                    orders_unfin += 1
                    self.client.cancel_order(symbol= self.market, orderId=orderId)
            if orders_unfin == 0:
                self.logger.info("No open orders to cancel")
                self.try_count = 0
            else:
                self.logger.info(f"Cancelled {orders_unfin} open orders.")
                self.try_count += 1
        except Exception as error:
            self.logger.warning(f"Failed to cancel order because of {error}")  
    
    def cancel_order_by_id(self, orderId) -> None:
        try:
            response = self.client.cancel_order(symbol= self.market, orderId=orderId)
            self.logger.info(f"Cancelled order {orderId}")
            return response
        except Exception as error:
            self.logger.warning(f"Failed to cancel order {orderId} because of {error}")  

    def fetch_positions(self) -> tuple:
        try:
            positions = pd.DataFrame(self.client.get_position_risk(recvWindow=6000))
            unpnl = positions.query("symbol == @self.market").loc[:, "unRealizedProfit"]
            unpnl_float = float(unpnl)
            notional = positions.query("symbol == @self.market").loc[:, "notional"]
            abs_notional = abs(float(notional))
            return unpnl_float, abs_notional
        except Exception as error:
            self.logger.error(error)
    
    def close_position(self) -> None:
        try:
            positions = pd.DataFrame(self.client.get_position_risk(recvWindow=6000))
            positionAmt = positions.query("symbol == @self.market").loc[:, "positionAmt"]
            position = float(positionAmt)
            if position > 0:
                response = self.client.new_order(
                    symbol=self.market,
                    side="SELL",
                    type="MARKET",
                    quantity=abs(position),
                )
                return response
            elif position < 0:
                response = self.client.new_order(
                    symbol=self.market,
                    side="BUY",
                    type="MARKET",
                    quantity=abs(position),
                )
                return response
            else:
                self.logger.info("No position to close")
        except Exception as error:
            self.logger.error(error)

    def get_order_info(self, orderId) -> dict:
        try:
            response = self.client.query_order(
        symbol=self.market, orderId=orderId, recvWindow=2000
    )
            return response
        except Exception as error:
            self.logger.error(error)

