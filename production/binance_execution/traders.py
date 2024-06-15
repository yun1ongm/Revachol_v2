import sys
import os

main_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.append(main_path)
import warnings

warnings.filterwarnings("ignore")
# -*- coding: utf-8 -*-

import logging
from binance.um_futures import UMFutures
import pandas as pd
import yaml


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

    def __init__(self, pairs) -> None:
        self.config = self._read_config()
        self.client = self._connect_api(
            key=self.config["bn_api"]["key"], secret=self.config["bn_api"]["secret"]
        )
        self.symbols = self.convert_to_circle(pairs)
        self.try_count = 0

    def convert_to_circle(self, symbols) -> list:
        return [(symbol + "C") for symbol in symbols]

    def _read_config(self) -> dict:
        rel_path = "/production/config.yaml"
        try:
            with open(main_path + rel_path, "r") as stream:
                config = yaml.safe_load(stream)
        except FileNotFoundError:
            self.logger.error("Config file not found")
            sys.exit(1)
        return config

    def _connect_api(self, key, secret) -> UMFutures:
        """connect binance client with apikey and apisecret"""
        client = UMFutures(key=key, secret=secret, timeout=5)

        return client

    def _order_settings(self, market: str) -> tuple:
        if market == "BTCUSDT" or "BTCUSDC":
            slippage = -7
            digit = 1
        elif market == "ETHUSDT" or "ETHUSDC":
            slippage = -0.35
            digit = 2
        elif market == "SOLUSDT" or "SOLUSDC":
            slippage = -0.07
            digit = 3
        return slippage, digit

    def maker_buy(self, amount: float, ticker: float, symbol: str) -> dict:
        """send post-only buy order"""
        slippage, digit = self._order_settings(symbol)
        price = round((ticker - slippage), digit)
        amount = round(amount, 3)
        self.logger.info(f"Ticker: {ticker} Executing buy price:{price}")
        try:
            response = self.client.new_order(
                symbol=symbol,
                side="BUY",
                type="LIMIT",
                quantity=amount,
                timeInForce="GTX",
                price=price,
            )
            return response

        except Exception as error:
            self.logger.error(error)

    def maker_sell(self, amount: float, ticker: float, symbol: str) -> dict:
        """send post-only sell order"""
        slippage, digit = self._order_settings(symbol)
        price = round((ticker - slippage), digit)
        amount = round(amount, 3)
        self.logger.info(f"Ticker: {ticker} Executing sell price:{price}")
        try:
            response = self.client.new_order(
                symbol=symbol,
                side="SELL",
                type="LIMIT",
                quantity=amount,
                timeInForce="GTX",
                price=price,
            )
            return response

        except Exception as error:
            self.logger.error(error)

    def taker_buy(self, amount: float, symbol: str) -> dict:
        """send market buy order"""
        amount = round(amount, 3)
        try:
            response = self.client.new_order(
                symbol=symbol, side="BUY", type="MARKET", quantity=amount
            )
            return response
        except Exception as error:
            self.logger.error(error)

    def taker_sell(self, amount: float, symbol: str) -> dict:
        """send market sell order"""
        amount = round(amount, 3)
        try:
            response = self.client.new_order(
                symbol=symbol, side="SELL", type="MARKET", quantity=amount
            )
            return response
        except Exception as error:
            self.logger.error(error)

    def send_batch_order(self, orders_df: pd.DataFrame) -> list:
        """send buy and sell orders based on the maker price dataframe
        Args:
            order_df (pd.DataFrame): dataframe with symbol, lot, buy1, sell1, buy2, sell2
            response (list): response from binance api
        """
        market = orders_df.index[-1]
        lot = orders_df["lot"][-1]
        buy1 = round(orders_df["buy1"][-1], self.digit)
        sell1 = round(orders_df["sell1"][-1], self.digit)
        buy2 = round(orders_df["buy2"][-1], self.digit)
        sell2 = round(orders_df["sell2"][-1], self.digit)
        try:
            batchOrders = [
                {
                    "symbol": market,
                    "side": "SELL",
                    "type": "LIMIT",
                    "quantity": f"{lot}",
                    "timeInForce": "GTC",
                    "reduceOnly": "false",
                    "price": f"{sell1}",
                },
                {
                    "symbol": market,
                    "side": "SELL",
                    "type": "LIMIT",
                    "quantity": f"{lot*2}",
                    "timeInForce": "GTC",
                    "reduceOnly": "false",
                    "price": f"{sell2}",
                },
                {
                    "symbol": market,
                    "side": "BUY",
                    "type": "LIMIT",
                    "quantity": f"{lot}",
                    "timeInForce": "GTC",
                    "reduceOnly": "false",
                    "price": f"{buy1}",
                },
                {
                    "symbol": market,
                    "side": "BUY",
                    "type": "LIMIT",
                    "quantity": f"{lot*2}",
                    "timeInForce": "GTC",
                    "reduceOnly": "false",
                    "price": f"{buy2}",
                },
            ]
            response = self.client.new_batch_order(batchOrders)
            self.logger.info(
                f"batch order sent-- buy1: {buy1} sell1: {sell1} buy2: {buy2} sell2: {sell2}"
            )
            return response
        except Exception as error:
            self.logger.error(error)

    def cancel_open_orders(self) -> None:
        for symbol in self.symbols:
            try:
                response = self.client.cancel_open_orders(
                    symbol=symbol, recvWindow=2000
                )
            except Exception as error:
                self.logger.warning(f"{error}")

    def cancel_order_by_id(self, orderId: int, symbol: str) -> None:
        try:
            response = self.client.cancel_order(symbol=symbol, orderId=orderId)
            self.logger.info(f"Cancelled order {orderId}")
            return response
        except Exception as error:
            self.logger.warning(f"Failed to cancel order {orderId} because of {error}")

    def fetch_positions(self) -> tuple:
        try:
            positions = pd.DataFrame(self.client.get_position_risk(recvWindow=6000))
            unpnl = positions.query("symbol == @self.symbols").loc[
                :, "unRealizedProfit"
            ]
            unpnl_float = float(unpnl)
            notional = positions.query("symbol == @self.symbols").loc[:, "notional"]
            abs_notional = abs(float(notional))
            return unpnl_float, abs_notional
        except Exception as error:
            self.logger.error(error)

    def close_position(self) -> None:
        try:
            positions = pd.DataFrame(
                self.client.get_position_risk(recvWindow=6000)
            ).query("symbol == @self.symbols")
            for _, symbol in positions.iterrows():
                position = float(symbol["positionAmt"])
                if position > 0:
                    response = self.client.new_order(
                        symbol=self.symbols,
                        side="SELL",
                        type="MARKET",
                        quantity=abs(position),
                    )
                    return response
                elif position < 0:
                    response = self.client.new_order(
                        symbol=self.symbols,
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
                symbol=self.symbols, orderId=orderId, recvWindow=2000
            )
            return response
        except Exception as error:
            self.logger.error(error)


if __name__ == "__main__":
    test = Traders(["BTCUSDT", "ETHUSDT", "SOLUSDT"])
    test.cancel_open_orders()
