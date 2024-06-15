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
from research.Alpha.alp_adx_stochrsi_demastd import AlpAdxStochRsiMultiple
from production.kline import KlineGenerator


class ModeLBest:
    """
    Model Best is a model using ts alpha with different params on different trading pairs
    """

    model_name = "model_best"
    alpha_names = ["alp_adx_stochrsi_multiple"]
    traded_pairs = ["BTCUSD"]
    logger = logging.getLogger(model_name)

    def __init__(self) -> None:
        config = self._read_config()
        self.discord_url = config["discord_webhook"]["url"]
        self._init_alpha()
        self.interval = 20

    def _read_config(self, rel_path="/production/config.yaml") -> dict:
        try:
            with open(main_path + rel_path, "r") as stream:
                config = yaml.safe_load(stream)
            return config
        except FileNotFoundError:
            self.logger.error("Config file not found")
            sys.exit(1)

    def _init_alpha(self) -> None:
        self.alphas = []
        for name in self.alpha_names:
            if name == "alp_adx_stochrsi_multiple":
                self.alphas.append(
                    AlpAdxStochRsiMultiple(money=1800, leverage=5, mode=1)
                )

        if len(self.alphas) == 0:
            self.logger.error("Alpha not found")
            sys.exit(1)

    async def push_discord(self, payload: dict) -> None:
        try:
            headers = {"Content-Type": "application/json"}
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.discord_url, data=json.dumps(payload), headers=headers
                ) as response:
                    pass
        except Exception as e:
            self.logger.warning(e)
        finally:
            response.close()

    def read_market(self, timeframe: str) -> dict:
        market = {}
        for pair in self.traded_pairs:
            symbol = pair.replace("USD", "USDT")
            try:
                with open(
                    main_path + f"/production/data/{symbol}_{timeframe}.csv", "r"
                ) as csv_file:
                    symbol_market = pd.read_csv(csv_file)
                    symbol_market["opentime"] = pd.to_datetime(
                        symbol_market["opentime"].apply(
                            lambda x: x + " 00:00:00" if len(x) == 10 else x
                        ),
                        format="%Y-%m-%d %H:%M:%S",
                    )
                    symbol_market["closetime"] = pd.to_datetime(
                        symbol_market["closetime"], format="%Y-%m-%d %H:%M:%S"
                    )
                    symbol_market.set_index("opentime", inplace=True)
                    market[pair] = symbol_market
            except Exception as error:
                self.logger.error(error)
        return market

    async def merging_alpha(self, market: dict) -> None:
        pair_position = {}
        for pair in market.keys():
            kdf = market[pair]
            updated_time = kdf.closetime[-1].strftime("%Y-%m-%d %H:%M:%S")
            alpha_positions = {}
            for alpha in self.alphas:
                alpha_name = alpha.alpha_name
                alpha_position = alpha.generate_portfolio(pair, kdf)
                alpha_positions[alpha_name] = alpha_position
            merged_position = sum(
                alpha_positions[alpha_name] for alpha_name in alpha_positions.keys()
            )
            alpha_positions["merged_position"] = round(merged_position, 3)
            alpha_positions["updated_time"] = updated_time
            pair_position[pair] = alpha_positions
            self.logger.info(
                f"{self.model_name} {pair} Position:{merged_position}\n-- -- -- -- -- -- -- -- --"
            )
            await self.push_discord(
                {
                    "content": f"{self.model_name} {pair} Position:{merged_position}, update_time: {updated_time}\n-- -- -- -- -- -- -- -- --"
                }
            )
        await self._export_symbol_position(pair_position)

    async def _export_symbol_position(self, symbol_position: dict) -> None:
        """export signal position to a yaml file"""
        export_dir = os.path.join(main_path, "production", "signal_position")
        os.makedirs(export_dir, exist_ok=True)
        export_path = os.path.join(export_dir, f"{self.model_name}.json")

        def default(obj):
            if isinstance(obj, pd.Timestamp):
                return obj.isoformat()
            raise TypeError(
                f"Object of type {obj.__class__.__name__} is not JSON serializable"
            )

        with open(export_path, "w") as file:
            json.dump(symbol_position, file, default=default, indent=4)
        self.logger.info(f"Signal position exported successfully")

    async def run(self, timeframe: str) -> None:
        market = KlineGenerator(self.traded_pairs, timeframe)
        while True:
            await market.update_klines()
            data_dict = self.read_market(timeframe)
            await self.merging_alpha(data_dict)
            await asyncio.sleep(self.interval)


if __name__ == "__main__":
    timbersaw.setup()
    model = ModeLBest()
    asyncio.run(model.run("1m"))
