import warnings
warnings.filterwarnings("ignore")
import sys
import os
main_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.append(main_path)
# -*- coding: utf-8 -*-

import logging
import psutil
from production.binance_execution.traders import Traders
import contek_timbersaw as timbersaw
import pandas as pd
import yaml
import json
import asyncio
import aiohttp


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
    equity = 2000
    leverage = 5
    
    logger = logging.getLogger(executor)

    def __init__(self, market) -> None:
        super().__init__(market)
        self.position = {}
        self.process = psutil.Process()
        self.interval = 20
     
    def _read_position(self) -> None:
        #读取/Users/rivachol/Desktop/Rivachol_v2/production/signal_position下的所有yaml文件
        rel_path = "/production/signal_position/"

        try:
            files = os.listdir(main_path + rel_path)
            if len(files) == 0:
                self.logger.error('No position file found.')
            else:
                for file in files:
                    symbol = file.split(".")[0]
                    with open(main_path + rel_path + file, 'r') as stream:
                        data = yaml.safe_load(stream)
                        self.position[symbol] = {"position": data["position"], "update_time": data["update_time"]}
        except FileNotFoundError:
            self.logger.error('Position is not read from the file.')
     
    async def push_discord(self, payload: dict) -> None:
        try:
            headers = {'Content-Type': 'application/json'}
            discord_url = self.config["discord_webhook"]["url"]
            async with aiohttp.ClientSession() as session:
                async with session.post(discord_url, data=json.dumps(payload), headers=headers) as response:
                    pass
        except Exception as e:
            self.logger.warning(e)
        finally:
            response.close()

    async def check_position_diff(self, signal_position: float) -> bool:
        """compare actual position and signal position & fill the gap if there is one"""
        try:
            positions = pd.DataFrame(self.client.get_position_risk(recvWindow=6000))
            positionAmt = positions.query("symbol == @self.market").loc[:, "positionAmt"]
            actual_position = float(positionAmt)
            self.logger.info(f"actual position is {actual_position}")
            await self.push_discord({"content": f"Retrieve signal position: {signal_position} update_time:{self.update_time}"})
            if abs(actual_position - signal_position) < 0.00001:
                self.logger.info(f"Position & signals are cross checked.\n-- -- -- -- -- -- -- -- --")
                return True
            else:
                self.logger.warning(f"Gap to match!\n-- -- -- -- -- -- -- -- -- ")
                position_diff = signal_position - actual_position
                book_ticker = self.client.book_ticker(self.market)
                bid_price = float(book_ticker["bidPrice"])
                ask_price = float(book_ticker["askPrice"])
                if position_diff > 0:
                    self.logger.warning("@@@@@@@@@@@@  Sending Buy Order @@@@@@@@@@@@")
                    await self._maker_buy(position_diff, bid_price)
                else:
                    self.logger.warning("@@@@@@@@@@@@ Sending Sell Order @@@@@@@@@@@@")
                    await self._maker_sell(-position_diff, ask_price)
            return False

        except Exception as error:
            self.logger.error(error)
            return False

    async def task(self) -> bool:
        """main task of the executor"""
        self.cancel_open_orders()
        signal_position = self._read_position()
        compelete = await self.check_position_diff(signal_position)
        memory_info = self.process.memory_info()
        self.logger.info(f"Memory usage: {memory_info.rss / 1024 / 1024:.2f} MB")
        self.logger.info(f"CPU usage: {self.process.cpu_percent(interval=1):.2f}%\n-- -- -- --")
        return compelete
        
    async def run(self) -> None:
        while True:
            try:
                complete = await self.task()
                if complete:
                    await asyncio.sleep(self.interval)
                else:
                    await asyncio.sleep(self.interval / 2)
            except Exception as e:
                self.logger.critical(e)
                await asyncio.sleep(self.interval)    

if __name__ == "__main__":
    timbersaw.setup()
    executor = ExecPostmodern(market = "BTCUSDC")
    asyncio.run(executor.run())
