import pandas as pd
import pandas_ta as ta
import numpy as np
from eth_5m_market import market_bot
import time
import warnings
warnings.filterwarnings("ignore")

class Indicators:
    def dema(kdf,period):
        dema = ta.dema(kdf["close"],length=period)
        dema = pd.concat([kdf["close"],dema], axis=1)
        dema.columns = ["close","dema"]
        return dema
    
    def engulfing(kdf):
        engulf = kdf[['open','close','high','low']]
        engulf["body"] = engulf["close"] - engulf["open"]
        # 前一根阳线
        condition1 = engulf["body"].shift(1) > 0 
        # 后一根阴线吞没
        condition2 = engulf["close"] < engulf["low"].shift(1)
        # 前一根阴线
        condition3 = engulf["body"].shift(1) < 0 
        # 后一根阳线吞没
        condition4 = engulf["close"] > engulf["high"].shift(1)
        engulf['engulf'] = np.where(condition1 & condition2, -1,
                                    np.where(condition3 & condition4, 1, 0))
        
        return engulf.engulf
    
    def hammer(kdf,hammer_k):
        hammer = kdf[['open','close','high','low']]
        hammer["body"] = hammer["close"] - hammer["open"]
        hammer["upwick"] = np.where(hammer["body"] >0, hammer["high"]-hammer["close"],hammer["high"]-hammer["open"])
        hammer["downwick"] = np.where(hammer["body"] >0, hammer["open"] - hammer["low"],hammer["close"] - hammer["low"])
        # 前一根阳线
        condition1 = hammer["body"].shift(1) > 0 
        # 后一根阴线倒锤且柄大于锤头K倍
        condition2 = hammer["body"] < 0 
        condition3 = hammer["upwick"] >= abs(hammer["body"])*hammer_k
        # 前一根阴线
        condition4 = hammer["body"].shift(1) < 0 
        # 后一根阳线锤子且柄大于锤头K倍
        condition5 = hammer["body"] > 0 
        condition6 = hammer["downwick"] >=  abs(hammer["body"])*hammer_k
        hammer['hammer'] = np.where(condition1 & condition2 & condition3, -1,
                                    np.where(condition4 & condition5 & condition6, 1, 0))
        return hammer.hammer

class alp_engulf_hammer_rvs():
    def __init__(self):
        self.market = market_bot()   #实例化market_bot
        self.vol_period=15
        self.vol_k=3
        self.dema_period=10
        self.hammer_k=3
        self.atrk = 5
        self.wlr = 1 
        
        self.index_signal = None
        self.order_signal = None

    def gen_index_signal(self,kdf):
        kdf_sig = Indicators.dema(kdf,self.dema_period)
        kdf_sig["atr"] =  ta.atr(kdf['high'], kdf['low'], kdf['close'], length=self.vol_period)
        kdf_sig["volume"] = kdf["volume"]
        kdf_sig["vol_ma"] = ta.ema(kdf["volume"],length = self.vol_period)
        kdf_sig['engulf'] = Indicators.engulfing(kdf)
        kdf_sig['hammer'] = Indicators.hammer(kdf,self.hammer_k)
        kdf_sig["signal"] = np.zeros(len(kdf_sig))
        # 低位反转
        kdf_sig.loc[(kdf_sig["close"]<kdf_sig["dema"]) & (kdf_sig["volume"]>= kdf_sig["vol_ma"] * self.vol_k) & (kdf_sig["engulf"]==1), "signal"] = 1
        kdf_sig.loc[(kdf_sig["close"]<kdf_sig["dema"]) & (kdf_sig["volume"]>= kdf_sig["vol_ma"] * self.vol_k) & (kdf_sig["hammer"]==1), "signal"] = 1
        # 高位反转
        kdf_sig.loc[(kdf_sig["close"]>kdf_sig["dema"]) & (kdf_sig["volume"]>= kdf_sig["vol_ma"] * self.vol_k) & (kdf_sig["engulf"]==-1), "signal"] = -1
        kdf_sig.loc[(kdf_sig["close"]>kdf_sig["dema"]) & (kdf_sig["volume"]>= kdf_sig["vol_ma"] * self.vol_k) & (kdf_sig["hammer"]==-1), "signal"] = -1

        return kdf_sig

    def gen_order_signal(self,index_signal):
        port_info = index_signal
        #初始化仓位头寸变量
        port_info["position"] = np.zeros(len(port_info))
        port_info["entry_price"] = np.zeros(len(port_info))
        port_info["stop_loss"] =  np.zeros(len(port_info))
        port_info["stop_profit"] = np.zeros(len(port_info))
        port_info["order"] = np.zeros(len(port_info))
        sizer = 0.01
        position = 0
        entryprice = 0
        stoploss = 0
        stopprofit = 0

        for index, row in index_signal.iterrows():
            signal = row["signal"]
            close = row["close"]
            atr = row["atr"]
            order = 0
            #开仓
            if  position == 0 and signal == 1:
                order = sizer
                entryprice = close
                stoploss = entryprice - atr * self.atrk
                stopprofit = entryprice+(entryprice-stoploss)*self.wlr
                position += order

            elif position == 0 and signal == -1:
                order = -sizer
                entryprice = close
                stoploss = entryprice + atr * self.atrk
                stopprofit = entryprice-(stoploss-entryprice)*self.wlr
                position += order

            else:#止盈止损反手开仓 
                if position >0 :
                    if close > stopprofit or close <= stoploss:
                        order = -sizer
                        position += order

                    elif signal == -1:
                        order = -sizer*2
                        position += order
                        entryprice = close
                        stoploss = entryprice + atr * self.atrk
                        stopprofit = entryprice-(stoploss-entryprice)* self.wlr

                elif position <0:
                    if close < stopprofit or  close >= stoploss:
                        order = sizer
                        position += order
                    elif signal == 1:
                        order = sizer*2
                        position += order
                        entryprice = close
                        stoploss = entryprice - atr * self.atrk
                        stopprofit = entryprice+(entryprice-stoploss)* self.wlr

            #记录当前值 
            port_info.loc[index,"order"] = order 
            port_info.loc[index,"position"] = position
            port_info.loc[index,"entry_price"] = entryprice
            port_info.loc[index,"stop_loss"] =  stoploss
            port_info.loc[index,"stop_profit"] = stopprofit

        return port_info[["position","order"]]
    
def loop():
    test = alp_engulf_hammer_rvs()
    print("signal gets started generating")
    
    while True:
        try: 
            #更新K线数据
            test.market.update_CKlines_df()
            #生成信号
            test.index_signal = test.gen_index_signal(test.market.kdf)
            #订单指令
            test.order_signal = test.gen_order_signal(test.index_signal) 
            print(f'alp_engulf_hammer_rvs position:{test.order_signal["position"][-1]}')
            print(f'alp_engulf_hammer_rvs order:{test.order_signal["order"][-1]}\n-----------------------------')
            time.sleep(10) 
        except Exception as e:
            print(e)
            time.sleep(10)

if __name__ == '__main__':
    loop()