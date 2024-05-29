import sys
import os
main_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.append(main_path)
import warnings
warnings.filterwarnings("ignore")

# -*- coding: utf-8 -*-
import yaml
import logging
import pandas as pd
import json
import aiohttp
import asyncio
import contek_timbersaw as timbersaw
from production.alpha.alp_adx_stochrsi_openatr import AlpAdxStochrsiOpenatr
from production.alpha.alp_super_openatr  import AlpSuperOpenatr
from production.alpha.alp_linbo_dempact import AlpLinboDempact
from production.kline import KlineGenerator

class ModelUrban:
    """
    Model Urban is a model that combines multiple alphas to generate a single signal position
    Args:
        symbol: str: symbol of the trading pair
        timeframe: str: timeframe of the trading pair
    
    Attributes:
        alphas: list: list of alpha classes
        market: MarketEngine: market engine class
        signal_position: float: combined signal position from all alphas
    """
    model_name = "model_urban"
    alpha_name = ["alp_adx_stochrsi_openatr", "alp_super_openatr", "alp_linbo_dempact"]
    logger = logging.getLogger(model_name)

    def __init__(self) -> None:
        config = self._read_config()
        self.discord_url = config['discord_webhook']["url"]
        self._init_alpha(self.alpha_name, config)
        self.prev_model_position = {alpha:{"position":0} for alpha in self.alpha_name}
        self.model_position = {alpha:{"position":0} for alpha in self.alpha_name}
        self.interval = 20
    
    def _read_config(self, rel_path = "/production/config.yaml") -> dict:
        try:
            with open(main_path + rel_path, 'r') as stream:
                config = yaml.safe_load(stream)
            return config
        except FileNotFoundError:
            self.logger.error('Config file not found')
            sys.exit(1)
    
    def _init_alpha(self, alpha_name:list, config) -> None:
        self.alphas = []
        for name in alpha_name:
            if name == "alp_adx_stochrsi_openatr":
                self.alphas.append(AlpAdxStochrsiOpenatr(money = 1000, leverage = 5,
                                                         params = config["alpha_params"][name]))
            if name == "alp_super_openatr":
                self.alphas.append(AlpSuperOpenatr(money = 1000, leverage = 5,
                                                   params = config["alpha_params"][name]))
            if name == "alp_linbo_dempact":
                self.alphas.append(AlpLinboDempact(money = 1000, leverage = 5,
                                                   params = config["alpha_params"][name]))
        if len(self.alphas) == 0:
            self.logger.error("Alpha not found")
            sys.exit(1)

    async def push_discord(self, payload: dict) -> None:
        try:
            headers = {'Content-Type': 'application/json'}
            async with aiohttp.ClientSession() as session:
                async with session.post(self.discord_url, data=json.dumps(payload), headers=headers) as response:
                    pass
        except Exception as e:
            self.logger.warning(e)
        finally:
            response.close()

    def read_market(self, symbol:str, timeframe:str) -> pd.DataFrame:
        try:
            with open(main_path + f"/production/data/{symbol}_{timeframe}.csv", 'r') as csv_file:
                market = pd.read_csv(csv_file)
                market["opentime"] = pd.to_datetime(market["opentime"], format="%Y-%m-%d %H:%M:%S")
                market["closetime"] = pd.to_datetime(market["closetime"], format="%Y-%m-%d %H:%M:%S")
                market.set_index("opentime", inplace=True)
            return market
        except Exception as error:
            self.logger.error(error)
            self.push_discord({"content": f"Terminated with Error: {error}"})
            raise

    async def merging_signal(self, kdf: pd.DataFrame) -> None:
        try:
            for alpha in self.alphas:
                alpha_name = alpha.alpha_name
                self.model_position[alpha_name] = alpha.generate_signal_position(kdf)

                if self.model_position[alpha_name]["position"] != self.prev_model_position[alpha_name]["position"]:
                    change = self.model_position[alpha_name]["position"] - self.prev_model_position[alpha_name]["position"]
                    self.logger.warning(f"{alpha_name} Signal Position Change:{change}")
                    await self.push_discord({"content": f"{alpha_name} Signal Position Change:{change}\n{self.model_position[alpha_name]}"})
                    self.prev_model_position[alpha_name] = self.model_position[alpha_name]

            merged_position = sum([self.model_position[alpha]["position"] for alpha in self.alpha_name])
            await self._export_signal_position(kdf, merged_position)
            self.logger.info(f"{self.model_name} Position:{merged_position}\n-- -- -- -- -- -- -- -- --")

        except Exception as error:
            self.logger.error(error)
            await self.push_discord({"content": f"Terminated with Error: {error}"})
            raise

    async def _export_signal_position(self, kdf: pd.DataFrame, merged_position: float) -> None:
        """export signal position to a yaml file"""
        export_path = "/production/signal_position.yaml"
        with open(main_path + export_path, "w") as file:
            model_signal = {
                self.model_name:
                    {"update_time": str(kdf.index[-1]),
                     "model_position": str(round(merged_position, 3)), },
            }
            await self.push_discord({"content": f"Model signal position: {merged_position}, update_time: {kdf.closetime[-1]}\n-- -- -- -- -- -- -- -- --"})
            yaml.dump(model_signal, file)

    # async def _cooldown(self, kdf: pd.DataFrame, timeframe_int: int) -> bool:
    #     """check if the time of the candle matches the current time"""
    #     candle_time = kdf.closetime[-1]
    #     utc_time = datetime.utcnow()
    #     if utc_time - timedelta(minutes=timeframe_int) < candle_time:
    #         return True
    #     else:
    #         return False

    async def run(self, symbol: str, timeframe: str) -> None:
        market = KlineGenerator(symbol, timeframe)
        while True:
            update = await market.update_klines()
            if update:
                kdf = self.read_market(symbol, timeframe)
                await self.merging_signal(kdf)
                await asyncio.sleep(self.interval)
            else:
                await asyncio.sleep(self.interval / 2)

if __name__ == "__main__":
    timbersaw.setup()
    model = ModelUrban()
    asyncio.run(model.run("BTCUSDT", "1m"))