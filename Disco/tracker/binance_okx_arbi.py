import logging
import time
import datetime
import concurrent.futures
import okx.MarketData as MarketData
from binance.um_futures import UMFutures
import math
import sys
temp = "/Users/rivachol/Desktop/Rivachol_v2/Disco/"
sys.path.append(temp)
from tracker.okx_api import okx_apikey, okx_apisecret

import math

class BnOkxArbi:
    arbitrage_name = "binance_okx_arbitrage"
    symbols_list = ["BTCUSD", "ETHUSD"]
    binance_symbol_mapping = {
        "BTCUSD": "BTCUSDT",
        "ETHUSD": "ETHUSDT",
    }
    okx_symbol_mapping = {
        "BTCUSD": "BTC-USD",
        "ETHUSD": "ETH-USD",
    }
    arbitrage_threshold = 0.001

    def __init__(self):
        self.binance = UMFutures()
        self.okx =  MarketData.MarketAPI(flag="0")

    def _fetch_tickers(self) -> dict:
        try:
            tickers = {}
            for symbol in self.symbols_list:
                okx_symbol = self.okx_symbol_mapping[symbol]
                binance_symbol = self.binance_symbol_mapping[symbol]
                okx_ticker = self.okx.get_tickers(instType = "SWAP", uly = okx_symbol)
                binance_ticker = self.binance.ticker_price(binance_symbol)
                okx_last = float(okx_ticker['data'][0]['last'])
                binance_last = float(binance_ticker['price'])
                tickers[symbol] = {'okx_last': okx_last, 'binance_last': binance_last}
            return tickers
        except Exception as e:
            print(e)

    def is_arbi_opportunity(self):
        tickers = self._fetch_tickers()
        for symbol, ticker in tickers.items():
            okx_last = ticker['okx_last']
            binance_last = ticker['binance_last']
            diff = okx_last - binance_last
            diff_percentage = round(diff / binance_last,4)
            print(f'{symbol} diff: {diff_percentage}')


    def task(self):
        time_start = datetime.datetime.now()
        count = 0
        max_workers = 10
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self.calculate_arbi, symbol): symbol
                for symbol in symbols
            }
            for future in concurrent.futures.as_completed(futures):
                symbol = futures[future]
                try:
                    difference = future.result()
                    count += 1
                except Exception as e:
                    self._log(e)
                    continue
                if difference:
                    if difference > self.arbitrage_threshold:
                        self._log(
                            f"Arbitrage opportunity found for {symbol} with Price Difference:{difference}!!!"
                        )
                    elif difference < -self.arbitrage_threshold:
                        self._log(
                            f"Arbitrage opportunity found for {symbol} with Price Difference:{difference}!!!"
                        )
        time_cost_in_seconds = (datetime.datetime.now() - time_start).seconds
        self._log(
            f"this round check {count} symbols and costs {time_cost_in_seconds:.2f} s\n -- -- -- -- -- -- -- -- --"
        )
        time.sleep(math.ceil(60 / max_workers))


if __name__ == "__main__":
    arbi = BnOkxArbi()
    while True:
        start = time.time()
        arbi.is_arbi_opportunity()
        end = time.time()
        print(f'cost {end-start:.2f} seconds')
        time.sleep(10)