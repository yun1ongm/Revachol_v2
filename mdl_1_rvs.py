# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from binance.um_futures import UMFutures
from binance_api import key,secret
from binance.error import ClientError
import time
import warnings
warnings.filterwarnings("ignore")
from alp_adx_stochrsi_rvs import alp_adx_stochrsi_rvs
from alp_engulf_hammer_rvs import alp_engulf_hammer_rvs
from multiprocessing import Process, Queue
import pickle

class mdl_1_rvs_exec:
    def __init__(self):
        self.connect_api(key = key,secret = secret)    
        self.printlog = True

        self.task_node = True
        self.task_ts = None

        self.alpha_1 = alp_adx_stochrsi_rvs()
        self.alpha_2 = alp_engulf_hammer_rvs()
        self.queue = Queue()
        self.symbol = 'ETHUSDT'
    
    def connect_api(self,key,secret): # 登录代码
        self.client = UMFutures(key=key,
                           secret=secret,
                           timeout=1)
        
    def log(self, txt):#打印日志
        if self.printlog:
            current_timestamp = time.time()
            local_time = time.localtime(current_timestamp)
            current_time = time.strftime( "%Y-%m-%d %H:%M:%S", local_time)
            log_line = '%s, %s' % (current_time, txt)
            print(log_line)
            with open('execution_log.txt', 'a') as f:
                f.write(log_line + '\n')    

    def market_buy(self,symbol,amount): # 下市价买单
        try:
            self.orderId = self.client.new_order(
            symbol=symbol,
            side="BUY",
            type="MARKET",
            quantity=amount,
            timeInForce="GTC",)
            
        except ClientError as error:
            self.log(error)

    def market_sell(self,symbol,amount): # 下市价卖单
        try:
            self.orderId = self.client.new_order(
            symbol=symbol,
            side="SELL",
            type="MARKET",
            quantity=amount,
            timeInForce="GTC",)
            
        except ClientError as error:
            self.log(error)

    def limit_buy(self,symbol, amount, price): # 下限价买单
        try:
            self.orderId = self.client.new_order(
            symbol=symbol,
            side="BUY",
            type="LIMIT",
            quantity=amount,
            timeInForce="GTC",
            price=price)
            
        except ClientError as error:
            self.log(error)
          
    def limit_sell(self,symbol, amount, price): # 下限价卖单
        try:
            self.orderId = self.client.new_order(
            symbol=symbol,
            side="SELL",
            type="LIMIT",
            quantity=amount,
            timeInForce="GTC",
            price=price)
            
        except ClientError as error:
            self.log(error)

    def fetch_last_order(self,symbol): # 获取最后订单
        orders = self.client.get_all_orders(symbol=symbol, recvWindow=2000)
        last_order = orders[-1]
        return last_order

    def cancel_order(self,symbol,orderId):#取消订单
        try:
            self.client.cancel_order(symbol=symbol, orderId=orderId, recvWindow=2000)
            
        except ClientError as error:
            self.log(error)

    def merged_signal(self):
        df_merged = self.alpha_1.order_signal.merge(self.alpha_2.order_signal,on = 'opentime')
        df_merged['total_position'] = df_merged['position_x'] + df_merged['position_y']
        df_merged['total_order'] = df_merged['order_x'] + df_merged['order_y']
        return df_merged[['total_order','total_position']]

    def alpha_loop(self):
        while True:
            try:
                #更新K线数据
                self.alpha_1.market.update_CKlines_df()
                self.alpha_2.market.update_CKlines_df()
                #生成信号
                self.alpha_1.index_signal = self.alpha_1.gen_index_signal(self.alpha_1.market.kdf)
                self.alpha_2.index_signal = self.alpha_2.gen_index_signal(self.alpha_2.market.kdf)
                #订单指令
                self.alpha_1.order_signal = self.alpha_1.gen_order_signal(self.alpha_1.index_signal) 
                self.alpha_2.order_signal = self.alpha_2.gen_order_signal(self.alpha_2.index_signal) 
                self.log(f'alp_adx_stochrsi_rvs position:{self.alpha_1.order_signal["position"][-1]}')
                self.log(f'alp_engulf_hammer_rvs position:{self.alpha_2.order_signal["position"][-1]}')

                #序列化signal发送到通信队列
                signal = self.merged_signal()
                signal_pickle =pickle.dumps(signal)
                self.queue.put(signal_pickle)
                self.log(f"Combined Signal Position:{signal['total_position'][-1]}")
                time.sleep(10) 
            except Exception as e:
                self.log(e)
                time.sleep(10)

    def order_unfilled_check(self):  #未成交订单重新以新价格挂单   
        last_order = self.fetch_last_order(self.symbol)
        orderId = last_order["orderId"]
        side = last_order["side"] 
        if last_order["status"] in ['NEW','PARTIALLY_FILLED']: 
            self.log['there is an opening order!']
            orderAmt = float(last_order['origQty'])
            ticker = self.client.ticker_price(self.symbol)
            price = float(ticker['price'])
            if side =="BUY":
                self.cancel_order(self.symbol,orderId)
                self.limit_buy(self.symbol,orderAmt,price)
                self.log("++++++++++++Sendong Buy Order++++++++++++")
            if side =="SELL":
                self.cancel_order(self.symbol,orderId)
                self.limit_sell(self.symbol,-orderAmt,price)
                self.log("------------Sending Sell Order------------")
            return False
        else:
            return True

    def position_diff_check(self,signal_position):
        # 获取当前持仓信息
        positions = pd.DataFrame(self.client.get_position_risk(recvWindow=6000))
        position = positions.query('symbol == @self.symbol') 
        positionAmt =  float(position['positionAmt'])
        self.log(f'actual position is {positionAmt}')
        # 检查仓位是否一致
        if positionAmt == signal_position:
            return True
        else:  # 如有差异，以现价开仓
            position_diff = signal_position - positionAmt
            ticker = self.client.ticker_price(self.symbol)
            price = float(ticker['price'])
            if position_diff > 0:
                self.log("++++++++++++Sendong Buy Order++++++++++++")
                self.limit_buy(self.symbol,position_diff,price)
            else:
                self.log("------------Sending Sell Order------------")
                self.limit_sell(self.symbol,-position_diff,price)
            return False

    def task(self):
        # 首先，检查未成交订单情况
        self.task_node = self.order_unfilled_check()
        # 随后，提取队列结果
        signal_pickle = self.queue.get()
        signal = pickle.loads(signal_pickle)
        queue_ts = signal.index[-1]
        signal_position = signal['total_position'][-1]
        self.log(f'signal position:{signal_position}')
        # 其次，检查时间戳是否对应
        if self.task_node and self.task_ts ==queue_ts:
            # 最后，检查信号和实际仓位是否一致，并完成订单发送
            self.task_node = self.position_diff_check(signal_position)
            if self.task_node:
                self.log(f'position and signals are cross checked.\n-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --') 
            else:
                self.log(f'To matching...\n-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --') 

        elif self.task_ts != queue_ts: #更新任务时间和任务状态
            self.task_ts = queue_ts
            self.log(f'task refreshed at 5m start with ts of {queue_ts}\n\n--------------------------------------')
        else:
            self.log(f'mismatch because of an opening order!\n-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --')
 
    def model_run(self):
        self.log("model initiating......\n---------------------------------")
        while True:  
            try:
                self.task()
                time.sleep(10)
            except Exception as e:
                self.log(e)
                time.sleep(10)

    def main(self):    
        p1 = Process(target=self.alpha_loop)
        p2 = Process(target=self.model_run)

        p1.start()
        p2.start()

        p1.join()
        p2.join()

if __name__ == '__main__':
    test = mdl_1_rvs_exec()
    test.main()







