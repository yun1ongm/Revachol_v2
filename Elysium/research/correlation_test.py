from binance_api import key,secret
import ccxt
from datetime import datetime, timedelta
import time
import pandas as pd

class correlation:
    def __init__(self):
        self.exchange = self.binanceusdm(key = key,secret = secret)
        self.printlog = True

    def binanceusdm(self,key,secret):# 登录代码
        exchange = ccxt.binanceusdm(config={'apiKey':key,
                                            'secret':secret,
                                            'enableRateLimit': True,
                                            'timeout':100000,
                                            'requests_trust_env':True,
                                            'options': {'adjustForTimeDifference':True, 
                                                        'defaultType': 'future',}
                                            }) 
        return exchange
    
    def log(self, txt):#打印日志
        if self.printlog:
            current_timestamp = time.time()
            local_time = time.localtime(current_timestamp)
            current_time = time.strftime( "%Y-%m-%d %H:%M:%S", local_time)
            log_line = '%s, %s' % (current_time, txt)
            print(log_line)
            with open('strategy_log.txt', 'a') as f:
                f.write(log_line + '\n')    

    def gen_data(self,symbol,timeframe,start,window_days):
        self.exchange.load_markets()
        market = self.exchange.market(symbol)
        data = []
        for i in range(0,window_days):
            istart = start + timedelta(days=i)
            iend = istart + timedelta(days=1)
            starttime =int(istart.timestamp()*1000)
            endtime = int(iend.timestamp()*1000)
            params = {
                'pair': market['id'],
                'contractType': 'PERPETUAL',  # 'PERPETUAL', 'CURRENT_MONTH', 'NEXT_MONTH', 'CURRENT_QUARTER', 'NEXT_QUARTER'
                'interval': self.exchange.timeframes[timeframe],
                "startTime":starttime,
                "endTime":endtime,
                'limit': 1500
            }
            ohlcv = self.exchange.fapipublic_get_continuousklines(params)
            ohlcv.pop()
            data=data+ohlcv
        kdf = pd.DataFrame(data, columns=["opentime","open","high","low","close","volume","closetime",
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
    
    def lag_15():

    def lag_30():

    def lag_45():
    
    def lag_60():
        