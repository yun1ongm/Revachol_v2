import asyncio
import websockets
import json
from binance.um_futures import UMFutures
import sys
sys.path.append("/Users/rivachol/Desktop/Rivachol_v2/")
import contek_timbersaw as timbersaw
import logging
import warnings
warnings.filterwarnings("ignore")

class BookCatcher:
    base_url = "wss://fstream.binance.com/ws/"

    logger = logging.getLogger(__name__)

    def __init__(self, symbol: str,levels: float, interval= "500ms"):
        self.connection = None
        self.url = f"{self.base_url}{symbol.lower()}@depth{levels}@{interval}"

    async def connect(self) -> None:
        self.connection = await websockets.connect(self.url)

    async def send(self, message):
        await self.connection.send(message)

    async def receive(self):
        return await self.connection.recv()

    async def listen(self, rate: int = 1):
        while True:
            raw_message = await self.connection.recv()
            print(self._parse_message(raw_message))
            await asyncio.sleep(rate)
    
    def _parse_message(self, raw_message):
        parsed_message = {}
        message_dict = json.loads(raw_message)
        if "e" in message_dict:
            if message_dict["e"] == "depthUpdate":
                parsed_message["event_type"] = message_dict["e"]
                parsed_message["event_time"] = message_dict["E"]
                parsed_message["symbol"] = message_dict["s"]
                parsed_message["bids"] = message_dict["b"]
                parsed_message["asks"] = message_dict["a"]
        return parsed_message
    
    async def subscribe(self, stream):
        subscribe_message = {
            "method": "SUBSCRIBE",
            "params": [stream],
            "id": 1
        }
        subscribe_message_str = json.dumps(subscribe_message)
        await self.send(subscribe_message_str)
    
    async def unsubscribe(self, stream):
        unsubscribe_message = {
            "method": "UNSUBSCRIBE",
            "params": [stream],
            "id": 312
        }
        unsubscribe_message_str = json.dumps(unsubscribe_message)
        await self.send(unsubscribe_message_str)
    
    async def close(self):
        await self.connection.close()

if __name__ == "__main__":
    ws = BookCatcher("BTCUSDC", "5")
    loop = asyncio.get_event_loop()
    loop.run_until_complete(ws.connect())
    while True:
        loop.run_until_complete(ws.listen(1))

