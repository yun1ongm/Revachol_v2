from binance.um_futures import UMFutures
from datetime import datetime
import time
import pandas as pd

class market_bot:
    def __init__(self):   
        self.client = UMFutures(timeout=1)
        self.symbol = 'ETHUSDT'
        self.timeframe = '5m'
        self.kdf = self.get_CKlines_df() #获取K线数据

    def get_CKlines_df(self): #获取K线数据
        ohlcv = self.client.continuous_klines(self.symbol, "PERPETUAL",self.timeframe,limit =100)
        #移除未形成K线
        ohlcv.pop()
        kdf = pd.DataFrame(ohlcv, columns=["opentime","open","high","low","close","volume","closetime",
                                           "quota_volume","num_trade","taker_buy","taker_buy_quota_volume","ignore"])
        kdf.opentime = [datetime.utcfromtimestamp(int(x) / 1000.0) for x in kdf.opentime]  # update datetime format
        kdf.open = kdf.open.astype("float")
        kdf.high = kdf.high.astype("float")
        kdf.low = kdf.low.astype("float")
        kdf.close = kdf.close.astype("float")
        kdf.volume = kdf.volume.astype("float")
        kdf.closetime = [datetime.utcfromtimestamp(int(x) / 1000.0) for x in kdf.closetime]  # update datetime format
        kdf.quota_volume = kdf.quota_volume.astype("float")
        kdf.num_trade = kdf.num_trade.astype("int")
        kdf.taker_buy = kdf.taker_buy.astype("float")
        kdf.taker_buy_quota_volume = kdf.taker_buy_quota_volume.astype("float")
        kdf.ignore = kdf.ignore.astype("float")
        kdf.set_index('opentime', inplace=True)  # set datetime as index

        return kdf
    
    def update_CKlines_df(self):# 更新K线数据
        latest_ohlcv = self.client.continuous_klines(self.symbol, "PERPETUAL", self.timeframe, limit=2)
        latest_ohlcv.pop()  # 移除未形成K线
        latest_kdf = pd.DataFrame(latest_ohlcv, columns=["opentime", "open", "high", "low", "close", "volume", "closetime",
                                                     "quota_volume", "num_trade", "taker_buy", "taker_buy_quota_volume", "ignore"])
        latest_kdf.opentime = [datetime.utcfromtimestamp(int(x) / 1000.0) for x in latest_kdf.opentime]  # 更新日期时间格式
        latest_kdf.open = latest_kdf.open.astype("float")
        latest_kdf.high = latest_kdf.high.astype("float")
        latest_kdf.low = latest_kdf.low.astype("float")
        latest_kdf.close = latest_kdf.close.astype("float")
        latest_kdf.volume = latest_kdf.volume.astype("float")
        latest_kdf.closetime = [datetime.utcfromtimestamp(int(x) / 1000.0) for x in latest_kdf.closetime]  # 更新日期时间格式
        latest_kdf.quota_volume = latest_kdf.quota_volume.astype("float")
        latest_kdf.num_trade = latest_kdf.num_trade.astype("int")
        latest_kdf.taker_buy = latest_kdf.taker_buy.astype("float")
        latest_kdf.taker_buy_quota_volume = latest_kdf.taker_buy_quota_volume.astype("float")
        latest_kdf.ignore = latest_kdf.ignore.astype("float")
        latest_kdf.set_index('opentime', inplace=True)  # 设置日期时间为索引

        if latest_kdf.index[-1] <= self.kdf.index[-1]:
            print('already latest')
        else:
            self.kdf = pd.concat([self.kdf, latest_kdf])  # 将最新K线数据添加到已有数据DataFrame
            self.kdf = self.kdf.iloc[1:]  # 移除最开始的一条数据
            print('kdf updated')


    def get_aggrtrade(self,symbol):
        aggtrades  = self.client.agg_trades(symbol)
        aggtdf = pd.DataFrame(aggtrades)
        aggtdf.columns=['id','price','volume','first_id','last_id','time','taker_buy']
        aggtdf.price = aggtdf.price.astype("float")
        aggtdf.volume = aggtdf.volume.astype("float")

        return aggtdf
    
if __name__ =='__main__':
    test = market_bot()
    while True:
        test.update_CKlines_df()
        print(f'{test.kdf.iloc[-1]}\n----------------------------------------------------')
        time.sleep(10)