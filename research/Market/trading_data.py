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
    
    def get_24h_ticker(self):
        url = f"{self.base_url}/fapi/v1/ticker/24hr"
        res = requests.get(url)
        ticker = pd.DataFrame(res.json())
        ticker.columns = ["symbol", "price_change", "price_change_percent", "weighted_avg_price", "last_price", "last_qty", "open_price", "high_price", "low_price", "volume", "quote_volume", "open_time", "close_time", "first_id", "last_id", "count"]
        ticker.price_change = ticker.price_change.astype("float")
        ticker.price_change_percent = ticker.price_change_percent.astype("float")
        ticker.weighted_avg_price = ticker.weighted_avg_price.astype("float")
        ticker.last_price = ticker.last_price.astype("float")
        ticker.last_qty = ticker.last_qty.astype("float")
        ticker.open_price = ticker.open_price.astype("float")
        ticker.high_price = ticker.high_price.astype("float")
        ticker.low_price = ticker.low_price.astype("float")
        ticker.volume = ticker.volume.astype("float")
        ticker.quote_volume = ticker.quote_volume.astype("float")
        ticker.open_time = [
            datetime.utcfromtimestamp(int(x) / 1000.0) for x in ticker.open_time
        ]
        ticker.close_time = [
            datetime.utcfromtimestamp(int(x) / 1000.0) for x in ticker.close_time
        ]
        ticker.first_id = ticker.first_id.astype("int")
        ticker.last_id = ticker.last_id.astype("int")
        ticker["count"] = ticker["count"].astype("int")

        return ticker
    
    def get_qulified_ticker(self):
        ticker = self.get_24h_ticker()
        v5_ticker = ticker.sort_values("quote_volume", ascending=False).head(5)
        p5_ticker = ticker.sort_values("price_change_percent", ascending=False).head(5)
        #筛选出同时在两个列表中的币种
        qulified_ticker = pd.merge(v5_ticker, p5_ticker, on="symbol")
        if len(qulified_ticker) == 0:
            return "No qulified ticker"
        return qulified_ticker 
    
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
