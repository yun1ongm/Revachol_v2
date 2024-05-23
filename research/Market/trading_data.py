import sys
sys.path.append("/Users/rivachol/Desktop/Rivachol_v2/")
import warnings
warnings.filterwarnings("ignore")

import requests
import pandas as pd
from datetime import datetime

class DataGenerator:

    base_url = "https://fapi.binance.com"

    def get_hist_trades(self, symbol, limit=100):
        url = f"{self.base_url}/fapi/v1/trades"
        params = {
            "symbol": symbol,
            "limit": limit
        }
        res = requests.get(url, params=params)
        hist_trades = pd.DataFrame(res.json())
        hist_trades.columns= ["id", "price", "qty", "qty_u", "datetime", "sell"]
        hist_trades.price =  hist_trades.price.astype("float")
        hist_trades.qty =  hist_trades.qty.astype("float")
        hist_trades.qty_u =  hist_trades.qty_u.astype("float")
        hist_trades.datetime = [
            datetime.utcfromtimestamp(int(x) / 1000.0) for x in hist_trades.datetime
        ]
        hist_trades.set_index("datetime", inplace=True)

        return hist_trades
    
    def get_funding_rate(self, symbol, startTime, endTime, limit=30):
        url = f"{self.base_url}/fapi/v1/fundingRate"
        params = {
            "symbol": symbol,
            "startTime": startTime,
            "endTime": endTime,
            "limit": limit
        }
        res = requests.get(url, params=params)
        funding = pd.DataFrame(res.json())
        funding.columns = ["symbol", "funding_time", "funding_rate", "mark_price"]
        funding.funding_rate = funding.funding_rate.astype("float")
        funding.funding_time = [
            datetime.utcfromtimestamp(int(x) / 1000.0) for x in funding.funding_time
        ]
        funding.marker_price = funding.mark_price.astype("float")
        funding.set_index("funding_time", inplace=True)
        
        return funding
    
    def get_open_interest(self, symbol, period, limit=30):
        res = self.client.open_interest_hist(symbol, period, limit)
        oi = pd.DataFrame(res)
        oi.columns = ["symbol", "open_interest", "open_interest_U", "datetime"]
        oi.open_interest = oi.open_interest.astype("float")
        oi.open_interest_U = oi.open_interest_U.astype("float")
        oi.datetime = [
            datetime.utcfromtimestamp(int(x) / 1000.0) for x in oi.datetime
        ]
        oi.set_index("datetime", inplace=True)
        
        return oi
    
    def get_taker_long_short_ratio(self, symbol, period, limit=30):
        res = self.client.taker_long_short_ratio(symbol, period, limit)
        taker = pd.DataFrame(res)
        taker.columns = ["buy_sell_ratio", "sell_vol", "buy_vol", "datetime"]
        taker.buy_sell_ratio = taker.buy_sell_ratio.astype("float")
        taker.sell_vol = taker.sell_vol.astype("float")
        taker.buy_vol = taker.buy_vol.astype("float")
        taker.datetime = [
            datetime.utcfromtimestamp(int(x) / 1000.0) for x in taker.datetime
        ]
        taker.set_index("datetime", inplace=True)
        
        return taker
    
    def get_long_short_ratio(self, symbol, period):
        res = self.client.top_long_short_position_ratio(symbol, period)
        long_short = pd.DataFrame(res)
        long_short.columns = ["symbol", "long_account", "long_short_ratio", "short_account", "datetime"]
        long_short.long_account = long_short.long_account.astype("float")
        long_short.long_short_ratio = long_short.long_short_ratio.astype("float")
        long_short.short_account = long_short.short_account.astype("float")
        long_short.datetime = [
            datetime.utcfromtimestamp(int(x) / 1000.0) for x in long_short.datetime
        ]
        long_short.set_index("datetime", inplace=True)

        return long_short
    
    def get_premium_index(self, symbol, period):
        res = self.client.premium_index_klines(symbol, period)
        premium = pd.DataFrame(res)
        premium.columns = ["symbol", "index_price", "mark_price", "index_basis", "datetime"]
        premium.index_price = premium.index_price.astype("float")
        premium.mark_price = premium.mark_price.astype("float")
        premium.index_basis = premium.index_basis.astype("float")
        premium.datetime = [
            datetime.utcfromtimestamp(int(x) / 1000.0) for x in premium.datetime
        ]
        premium.set_index("datetime", inplace=True)

        return premium
    def generate_testdata(self):
        symbol = "BTCUSDT"
        limit = 100
        startTime = 1622505600000
        endTime = 1622592000000
        period = "1h"
        hist_trades = self.get_hist_trades_trades(symbol, limit)
        funding = self.get_funding_rate(symbol, startTime, endTime, limit)
        oi = self.get_open_interest(symbol, period, limit)
        taker = self.get_taker_long_short_ratio(symbol, period, limit)
        print(hist_trades.head())
        print(funding.head())
        print(oi.head())
        print(taker.head())
        
        return hist_trades, funding, oi, taker

if __name__ == "__main__":
    test = DataGenerator()
    test.generate_testdata()
