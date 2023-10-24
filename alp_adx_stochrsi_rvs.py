import pandas as pd
import pandas_ta as ta
import numpy as np
from eth_5m_market import market_bot
import time
import warnings
warnings.filterwarnings("ignore")

class Indicators:
    def adx(kdf,period):
        adx = ta.adx(kdf['high'], kdf['low'], kdf['close'], length=period)
        adx.columns = ["adx","plus","minus"]
        return adx

    def stochrsi(kdf,period,kd):
        stochrsi = ta.stochrsi(kdf['close'],rsi_length=period,k=kd,d=kd)
        stochrsi.columns = ["k","d"]
        condition1 = stochrsi['k'] > stochrsi['d']  
        condition2 =  stochrsi['k'].shift(1) <=  stochrsi['d'].shift(1)
        stochrsi['GXvalue'] = np.where(condition1 & condition2, stochrsi['d'], 0) 
        condition3 = stochrsi['k'] < stochrsi['d']  
        condition4 = stochrsi['k'].shift(1) >=  stochrsi['d'].shift(1)
        stochrsi['DXvalue'] = np.where(condition3 & condition4, stochrsi['d'], 0)
        return stochrsi
    
class alp_adx_stochrsi_rvs:
    def __init__(self):
        self.market = market_bot()   #实例化market_bot
        self.adx_len = 10
        self.rsi_len = 10
        self.kd = 10
        self.atrk = 5
        self.wlr = 1 

        self.index_signal = None
        self.order_signal = None
        
    def gen_index_signal(self,kdf):
        adx = Indicators.adx(kdf,self.adx_len)
        stochrsi = Indicators.stochrsi(kdf,self.rsi_len,self.kd)
        kdf_sig = pd.concat([kdf["close"],adx,stochrsi], axis=1)
        kdf_sig["atr"] =  ta.atr(kdf['high'], kdf['low'], kdf['close'], length=self.adx_len,mamode="ema")
        kdf_sig["signal"] = 0
        kdf_sig.loc[(kdf_sig["adx"]>=20) & (kdf_sig["GXvalue"]<20)  & (0<kdf_sig["GXvalue"]), "signal"] = 1
        kdf_sig.loc[(kdf_sig["adx"]>=20) & (kdf_sig["DXvalue"]>80), "signal"] = -1

        return kdf_sig[["close","signal","atr"]]

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
    test = alp_adx_stochrsi_rvs()
    print("signal gets started generating")
    
    while True:
        try: 
            #更新K线数据
            test.market.update_CKlines_df()
            #生成信号
            test.index_signal = test.gen_index_signal(test.market.kdf)
            #订单指令
            test.order_signal = test.gen_order_signal(test.index_signal) 
            print(f'alp_adx_stochrsi_rvs position:{test.order_signal["position"][-1]}')
            print(f'alp_adx_stochrsi_rvs order:{test.order_signal["order"][-1]}\n-----------------------------')
            time.sleep(10) 
        except Exception as e:
            print(e)
            time.sleep(10)

if __name__ == '__main__':
    loop()
      