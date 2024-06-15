# -*- coding: utf-8 -*-
import warnings

warnings.filterwarnings("ignore")
import sys
import os

main_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.append(main_path)
import logging
import psutil
from production.binance_execution.traders import Traders
import contek_timbersaw as timbersaw
import pandas as pd
import json
import asyncio
import aiohttp


class ExecBest(Traders):
    """
    Executor Best is a class that sends post-only orders to binance futures
    """

    executor = "best"
    model_name = "model_best"
    traded_pairs = ["BTCUSD"]
    equity = 2000
    leverage = 5

    logger = logging.getLogger(executor)

    def __init__(self) -> None:
        super().__init__(self.traded_pairs)
        self.position = {}
        self.process = psutil.Process()
        self.interval = 20

    def _read_position(self) -> dict:
        try:
            with open(
                f"{main_path}/production/signal_position/{self.model_name}.json", "r"
            ) as f:
                symbol_position = json.load(f)
            return symbol_position
        except FileNotFoundError:
            self.logger.error("Signal position file not found")
            return None

    async def push_discord(self, payload: dict) -> None:
        try:
            headers = {"Content-Type": "application/json"}
            discord_url = self.config["discord_webhook"]["url"]
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    discord_url, data=json.dumps(payload), headers=headers
                ) as response:
                    pass
        except Exception as e:
            self.logger.warning(e)
        finally:
            response.close()

    async def check_position_diff(self, c_symbol_position: dict) -> None:
        """compare actual position and signal position & fill the gap if there is one"""
        try:
            positions = pd.DataFrame(
                self.client.get_position_risk(recvWindow=6000)
            ).query("symbol == @self.symbols")
            for _, row in positions.iterrows():
                symbol = row["symbol"]
                actual_position = float(row["positionAmt"])
                self.logger.info(f"{symbol} actual position is {actual_position}")
                c_symbol = symbol.replace("USDC", "USD")
                merged_position = c_symbol_position[c_symbol]["merged_position"]
                updated_time = c_symbol_position[c_symbol]["updated_time"]
                await self.push_discord(
                    {
                        "content": f"Retrieve {symbol} position: {merged_position} updated_time:{updated_time}"
                    }
                )
                if abs(actual_position - merged_position) < 0.0001:
                    self.logger.info(
                        f"Position & signals are cross checked.\n-- -- -- -- -- -- -- -- --"
                    )

                else:
                    self.logger.warning(f"Gap to match!\n-- -- -- -- -- -- -- -- -- ")
                    position_diff = merged_position - actual_position
                    # book_ticker = self.client.book_ticker(symbol)
                    # bid_price = float(book_ticker["bidPrice"])
                    # ask_price = float(book_ticker["askPrice"])
                    if position_diff > 0:
                        self.taker_buy(position_diff, symbol)
                    else:
                        self.taker_sell(-position_diff, symbol)

        except Exception as error:
            self.logger.error(error)

    async def task(self) -> None:
        """main task of the executor"""
        self.cancel_open_orders()
        symbol_position = self._read_position()
        await self.check_position_diff(symbol_position)
        memory_info = self.process.memory_info()
        self.logger.info(f"Memory usage: {memory_info.rss / 1024 / 1024:.2f} MB")
        self.logger.info(
            f"CPU usage: {self.process.cpu_percent(interval=1):.2f}%\n-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --"
        )

    async def run(self) -> None:
        while True:
            try:
                await self.task()
                await asyncio.sleep(self.interval)

            except Exception as e:
                self.logger.critical(e)
                await asyncio.sleep(self.interval)


if __name__ == "__main__":
    timbersaw.setup()
    executor = ExecBest()
    asyncio.run(executor.run())
