import asyncio
import websockets
import json

class BPWebSocket:

    url = "wss://ws.backpack.exchange"

    def __init__(self):
        self.connection = None

    async def connect(self) -> None:
        self.connection = await websockets.connect(self.url)

    async def send(self, message):
        await self.connection.send(message)

    async def receive(self):
        return await self.connection.recv()

    async def listen(self, rate: int = 1):
        while True:
            message = await self.connection.recv()
            print(message)
            await asyncio.sleep(rate)

    async def subscribe(self, stream):
        subscribe_message = {
            "method": "SUBSCRIBE",
            "params": [stream]
        }
        subscribe_message_str = json.dumps(subscribe_message)
        await self.send(subscribe_message_str)
    
    async def unsubscribe(self, stream):
        unsubscribe_message = {
            "method": "UNSUBSCRIBE",
            "params": [stream]
        }
        unsubscribe_message_str = json.dumps(unsubscribe_message)
        await self.send(unsubscribe_message_str)
    
    async def close(self):
        await self.connection.close()

if __name__ == "__main__":
    ws = BPWebSocket()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(ws.connect())
    loop.run_until_complete(ws.subscribe("depth.SOL_USDC"))
    while True:
        loop.run_until_complete(ws.listen(1))

