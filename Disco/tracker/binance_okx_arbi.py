import logging
import time
from retry import retry

import okx.MarketData as MarketData
from binance.um_futures import UMFutures
import sys
temp = "/Users/rivachol/Desktop/Rivachol_v2/"
sys.path.append(temp)
import contek_timbersaw as timbersaw

class BnOkxArbi:
    """
    This class is used to track arbitrage opportunities between Binance and Okx
    """
    arbitrage_name = "binance_okx_arbitrage"
    symbols_list = ["BTCUSD", "ETHUSD", "SOLUSD", "BCHUSD", "ORDIUSD", 
                    "DOGEUSD", "XRPUSD"]

    bin_comm = 0.0005
    okx_comm = 0.0005

    logger = logging.getLogger(arbitrage_name)

    def __init__(self):
        self.binance = UMFutures()
        self.okx =  MarketData.MarketAPI(flag="0",debug=False)
        self.binance_symbol_mapping = self._generate_mapping(self.symbols_list, 'binance')
        self.okx_symbol_mapping = self._generate_mapping(self.symbols_list, 'okx')

    def _generate_mapping(self, symbols_list: list, exchange: str):
        mapping = {}
        for symbol in symbols_list:
            base = symbol.split('USD')[0]
            if exchange == 'binance':
                mapping[symbol] = base + 'USDT'
            elif exchange == 'okx':
                mapping[symbol] = base + '-USDT-SWAP'
        return mapping

    @retry(tries=3, delay=1)
    def fetch_tickers(self) -> dict:
        try:
            tickers = {}
            for symbol in self.symbols_list:
                okx_symbol = self.okx_symbol_mapping[symbol]
                binance_symbol = self.binance_symbol_mapping[symbol]
                okx_ticker = self.okx.get_orderbook(instId = okx_symbol)
                bin_ticker = self.binance.book_ticker(binance_symbol)
                tickers[symbol] = {'okx_ticker': okx_ticker, 'bin_ticker': bin_ticker}
            return tickers
        except Exception as e:
            print(e)
            return None
    
    def parse_orderbook(self, tickers:dict) -> dict:
        order_book = {}
        for symbol, ticker in tickers.items():
            okx_ticker = ticker['okx_ticker']['data'][0]
            binance_ticker = ticker['bin_ticker']

            okx_bid = float(okx_ticker['bids'][0][0])
            okx_bid_size = float(okx_ticker['bids'][0][1]) / 100
            okx_ask = float(okx_ticker['asks'][0][0])
            okx_ask_size = float(okx_ticker['asks'][0][1]) / 100
            bin_bid = float(binance_ticker['bidPrice'])
            bin_bid_size = float(binance_ticker['bidQty'])
            bin_ask = float(binance_ticker['askPrice'])
            bin_ask_size = float(binance_ticker['askQty'])

            if okx_bid != 0 and okx_ask != 0 and bin_bid != 0 and bin_ask != 0:
                order_book[symbol] = {
                    'okx_bid': okx_bid,
                    'okx_bid_size': okx_bid_size,
                    'okx_ask': okx_ask,
                    'okx_ask_size': okx_ask_size,
                    'bin_bid': bin_bid,
                    'bin_bid_size' : bin_bid_size,
                    'bin_ask': bin_ask,
                    'bin_ask_size' : bin_ask_size
                }
        
        return order_book
    
    def is_arbi_trade(self, order_book: dict) -> bool:
        for symbol, book in order_book.items():
            offset = round(book['bin_bid'] * self.okx_comm*2,4)
            positive_gap = round((book['okx_bid'] - book['bin_ask']),4)
            negative_gap = round((book['bin_bid'] - book['okx_ask']),4)
            self.logger.info(f'{symbol}:positive gap: {positive_gap}, negative gap: {negative_gap}, offset: {offset}')

            if positive_gap > offset:
                okx_bid = book['okx_bid']
                bin_ask = book['bin_ask']
                self.logger.warning(f'Buy {symbol} on Binance at {bin_ask}, Sell {symbol} on Okx at {okx_bid}')
                okx_bid_size = book['okx_bid_size']
                bin_ask_size = book['bin_ask_size']
                trading_lot = min(okx_bid_size, bin_ask_size)
                self.logger.warning(f'size: {trading_lot}')
                return True
            
            elif negative_gap > offset:
                okx_ask = book['okx_ask']
                bin_bid = book['bin_bid']
                self.logger.warning(f'Buy {symbol} on Okx at {okx_ask}, Sell {symbol} on Binance at {bin_bid}')
                okx_ask_size = book['okx_ask_size']
                bin_bid_size = book['bin_bid_size']
                trading_lot = min(okx_ask_size, bin_bid_size)
                self.logger.warning(f'size: {trading_lot}')
                return True
            
        return False
            
    def main(self):
        while True:
            tickers = self.fetch_tickers()
            if tickers:
                orderbook = self.parse_orderbook(tickers)
                flag = self.is_arbi_trade(orderbook)
                if flag:
                    self.logger.warning('------------------trade to be made!----------------------')
            time.sleep(2)

if __name__ == "__main__":
    timbersaw.setup()
    arbi = BnOkxArbi()
    arbi.main()
