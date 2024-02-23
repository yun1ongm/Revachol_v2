import logging
import time

import okx.MarketData as MarketData
from binance.um_futures import UMFutures
import sys
temp = "/Users/rivachol/Desktop/Rivachol_v2/Disco/"
sys.path.append(temp)
import contek_timbersaw as timbersaw
timbersaw.setup()

class BnOkxArbi:
    arbitrage_name = "binance_okx_arbitrage"
    symbols_list = ["BTCUSD", "ETHUSD"]
    binance_symbol_mapping = {
        "BTCUSD": "BTCUSDT",
        "ETHUSD": "ETHUSDT",
    }
    okx_symbol_mapping = {
        "BTCUSD": "BTC-USD-SWAP",
        "ETHUSD": "ETH-USD-SWAP",
    }
    bin_comm = 0.0005
    okx_comm = 0.0005

    logger = logging.getLogger(arbitrage_name)

    def __init__(self):
        self.binance = UMFutures()
        self.okx =  MarketData.MarketAPI(flag="0",debug=False)

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
            order_book[symbol] = {
                'okx_bid': float(okx_ticker['bids'][0][0]),
                'okx_bid_size': float(okx_ticker['bids'][0][1]) / 100, 
                'okx_ask': float(okx_ticker['asks'][0][0]),
                'okx_ask_size': float(okx_ticker['asks'][0][1]) / 100,
                'bin_bid': float(binance_ticker['bidPrice']),
                'bin_bid_size' : float(binance_ticker['bidQty']),
                'bin_ask': float(binance_ticker['askPrice']),
                'bin_ask_size' : float(binance_ticker['askQty'])
            }
        self.logger.info(f'Order book: {order_book}')
        
        return order_book
    
    def is_arbi_trade(self, order_book: dict) -> bool:
        for symbol, book in order_book.items():
            okx_bid_offset = book['okx_bid']*(1-self.okx_comm)
            okx_ask_offset = book['okx_ask']* (1+self.okx_comm)
            bin_bid_offset= book['bin_bid']*(1-self.bin_comm)
            bin_ask_offset = book['bin_ask']* (1+self.bin_comm)

            if okx_bid_offset > bin_ask_offset:
                self.logger.warning(f'Buy {symbol} on Binance, Sell {symbol} on Okx')
                okx_bid_size = book['okx_bid_size']
                bin_ask_size = book['bin_ask_size']
                trading_lot = min(okx_bid_size, bin_ask_size)
                self.logger.warning(f'size: {trading_lot}')
                return True
            
            elif bin_bid_offset > okx_ask_offset:
                self.logger.warning(f'Buy {symbol} on Okx, Sell {symbol} on Binance')
                okx_ask_size = book['okx_ask_size']
                bin_bid_size = book['bin_bid_size']
                trading_lot = min(okx_ask_size, bin_bid_size)
                self.logger.warning(f'size: {trading_lot}')
                return True
            
            else:
                return False
            
    def main(self):
        while True:
            tickers = self.fetch_tickers()
            if tickers:
                orderbook = self.parse_orderbook(tickers)
                flag = self.is_arbi_trade(orderbook)
                if flag:
                    self.logger.warning('trade to be made!\n--------------------------')
            time.sleep(1)

if __name__ == "__main__":
    arbi = BnOkxArbi()
    arbi.main()
