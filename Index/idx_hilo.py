import pandas as pd
import pandas_ta as pta
import numpy as np
class IdnetifierHilo:
    index_name = "identifier_hilo"
    
    def __init__(self, kdf, recog, atr_len, atr_k) -> None:
        self.kdf = kdf
        self.hilo_recog = recog
        self.atr_len = atr_len
        self.atr_k = atr_k

    def identify_hilo(self):
        candle_hilos = self.kdf[['high', 'low', 'close']]
        candle_hilos['atr'] = pta.atr(candle_hilos['high'], candle_hilos['low'], candle_hilos['close'], length = self.atr_len, mamode = "EMA")
        candle_hilos['highs'] = candle_hilos['high']
        candle_hilos['lows'] = candle_hilos['low']
        highs_count  = 0
        lows_count = 0

        for i in range(1, len(candle_hilos)- 1):
            candle_hilos.at[candle_hilos.index[i], 'highs'] = candle_hilos.at[candle_hilos.index[i-1], 'highs']
            candle_hilos.at[candle_hilos.index[i], 'lows'] = candle_hilos.at[candle_hilos.index[i-1], 'lows']

            if candle_hilos.at[candle_hilos.index[i], 'high'] > candle_hilos.at[candle_hilos.index[i - 1], 'high'] and candle_hilos.at[candle_hilos.index[i], 'high'] > candle_hilos.at[candle_hilos.index[i + 1], 'high']:
                if candle_hilos.at[candle_hilos.index[i], 'highs'] >= candle_hilos.at[candle_hilos.index[i], 'high']:
                    highs_count += 1
                else: #更新高点
                    highs_count = 0
                    candle_hilos.at[candle_hilos.index[i], 'highs'] = candle_hilos.at[candle_hilos.index[i], 'high']
            else:
                highs_count += 1

            if candle_hilos.at[candle_hilos.index[i], 'low'] < candle_hilos.at[candle_hilos.index[i - 1], 'low'] and candle_hilos.at[candle_hilos.index[i], 'low'] < candle_hilos.at[candle_hilos.index[i + 1], 'low']:
                if candle_hilos.at[candle_hilos.index[i], 'lows'] <= candle_hilos.at[candle_hilos.index[i], 'low']:
                    lows_count += 1
                else:#更新低点
                    lows_count = 0
                    candle_hilos.at[candle_hilos.index[i], 'lows'] = candle_hilos.at[candle_hilos.index[i], 'low']
            else:
                lows_count += 1

            candle_hilos.at[candle_hilos.index[i], 'highs_count'] = highs_count
            candle_hilos.at[candle_hilos.index[i], 'lows_count'] = lows_count
        return candle_hilos[['highs', 'lows', 'highs_count', 'lows_count', 'atr', 'close', 'high', 'low']]
    
    def generate_grid_level(self) -> pd.DataFrame:
        """
                        ---sell_lv5---
                        ---sell_lv4---
                        ---sell_lv3---
                        ---sell_lv2---
        ---high---      ---sell_lv1---  
                        ---close---
                        ---buy_lv1---
                        ---buy_lv2---
        ---mid---       ---buy_lv3---    
                        ---buy_lv4--- 
                        ---buy_lv5--- 
        ---low---
        """
        grid_info = self.identify_hilo() 
        grid_info['start_level'] = np.nan
        grid_info['grid_gap'] = np.nan
        grid_info['sell_lv1'] = np.nan
        grid_info['sell_lv2'] = np.nan
        grid_info['buy_lv1'] = np.nan
        grid_info['buy_lv2'] = np.nan
        
        for index, row in grid_info.iterrows():
            high = row['highs']
            low = row['lows']
            highs_count = row['highs_count']
            lows_count = row['lows_count']
            atr = row['atr']
            start_level = row['start_level']
            grid_gap = row['grid_gap']
            close = row['close']
            sell_lv1 = row['sell_lv1']
            sell_lv2 = row['sell_lv2']
            buy_lv1 = row['buy_lv1']
            buy_lv2 = row['buy_lv2']

            if sell_lv1 and high > sell_lv1:
                if high > sell_lv2:
                    start_level = sell_lv2
                    sell_lv1 = start_level + grid_gap
                    sell_lv2 = start_level + grid_gap * 2
                    buy_lv1 = start_level - grid_gap
                    buy_lv2 = start_level - grid_gap * 2
                else:
                    start_level = sell_lv1
                    sell_lv1 = start_level + grid_gap
                    sell_lv2 = start_level + grid_gap * 2
                    buy_lv1 = start_level - grid_gap
                    buy_lv2 = start_level - grid_gap * 2
            if buy_lv1 and low < buy_lv1:
                if low < buy_lv2:
                    start_level = buy_lv2
                    sell_lv1 = start_level + grid_gap
                    sell_lv2 = start_level + grid_gap * 2
                    buy_lv1 = start_level - grid_gap
                    buy_lv2 = start_level - grid_gap * 2
                else:
                    start_level = buy_lv1
                    sell_lv1 = start_level + grid_gap
                    sell_lv2 = start_level + grid_gap * 2
                    buy_lv1 = start_level - grid_gap
                    buy_lv2 = start_level - grid_gap * 2


            if highs_count <= self.hilo_recog or lows_count <= self.hilo_recog:
                start_level = close
                grid_gap = atr * self.atr_k
                sell_lv1 = start_level + grid_gap
                sell_lv2 = start_level + grid_gap * 2
                buy_lv1 = start_level - grid_gap
                buy_lv2 = start_level - grid_gap * 2
            
            grid_info = self.record_values(grid_info, index, start_level, grid_gap, sell_lv1, sell_lv2, buy_lv1, buy_lv2)
        
        # 将grid_info列中的nan值替换为前一行的值
        grid_info.fillna(method = 'ffill', inplace = True)

        return grid_info[['atr', 'close', 'high', 'low', 'sell_lv1', 'sell_lv2', 'buy_lv1', 'buy_lv2']]


    def record_values(self, grid_info, index, start_level, grid_gap, sell_lv1, sell_lv2, buy_lv1, buy_lv2):
        grid_info.at[index, 'start_level'] = start_level
        grid_info.at[index, 'grid_gap'] = grid_gap
        grid_info.at[index, 'sell_lv1'] = sell_lv1
        grid_info.at[index, 'sell_lv2'] = sell_lv2
        grid_info.at[index, 'buy_lv1'] = buy_lv1
        grid_info.at[index, 'buy_lv2'] = buy_lv2
        return grid_info





     


