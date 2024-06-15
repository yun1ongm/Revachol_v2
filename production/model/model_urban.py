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

    def read_market(self, symbols:list, timeframe:str) -> dict:
        market = {}
        for symbol in symbols:
            try:
                with open(main_path + f"/production/data/{symbol}_{timeframe}.csv", 'r') as csv_file:
                    symbol_market = pd.read_csv(csv_file)
                    symbol_market["opentime"] = pd.to_datetime(symbol_market["opentime"].apply(lambda x: x + ' 00:00:00' if len(x) == 10 else x), format="%Y-%m-%d %H:%M:%S")
                    symbol_market["closetime"] = pd.to_datetime(symbol_market["closetime"], format="%Y-%m-%d %H:%M:%S")
                    symbol_market.set_index("opentime", inplace=True)
                    market[symbol] = symbol_market
            except Exception as error:
                self.logger.error(error)
        return market

    async def merging_alpha(self, market:dict) -> None:
        for symbol in market.keys():
            try:
                symbol_position = {}
                kdf = market[symbol]
                for alpha in self.alphas:
                    alpha_name = alpha.alpha_name
                    symbol_position[alpha_name]= alpha.generate_signal_position(kdf)

                merged_position = sum([symbol_position[alpha]['position'] for alpha in self.alpha_name])
                self.logger.info(f"{self.model_name} {symbol} Position:{merged_position}\n-- -- -- -- -- -- -- -- --")
                await self._export_signal_position(symbol, kdf, merged_position)
            except Exception as error:
                self.logger.error(error)
                await self.push_discord({"content": f"Terminated with Error: {error}"})
                raise


    async def _export_signal_position(self, symbol:str, kdf: pd.DataFrame, merged_position: float) -> None:
        """export signal position to a yaml file"""
        export_dir = os.path.join(main_path, "production", "signal_position")
        os.makedirs(export_dir, exist_ok=True)
        export_path = os.path.join(export_dir, f"{symbol}.yaml")
        with open(export_path, "w") as file:
            model_signal = {
                self.model_name:
                    {"update_time": str(kdf.index[-1]),
                    "model_position": str(round(merged_position, 3)), },
            }
            await self.push_discord({"content": f"{symbol} signal position: {merged_position}, update_time: {kdf.closetime[-1]}\n-- -- -- -- -- -- -- -- --"})
            yaml.dump(model_signal, file)

    async def run(self, timeframe: str) -> None:
        market = KlineGenerator(timeframe)
        while True:
            await market.update_klines()
            data_dict = self.read_market(market.symbols, timeframe)
            await self.merging_alpha(data_dict)
            await asyncio.sleep(self.interval)

if __name__ == "__main__":
    timbersaw.setup()
    model = ModelUrban()
    asyncio.run(model.run("1m"))