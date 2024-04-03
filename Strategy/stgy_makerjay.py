import pandas as pd
import numpy as np

class StgyMakerjay:
    """
        Args:
            kdf_signal (pd.DataFrame): dataframe with klines and signal
            money (float): initial money

        Return:
            position (float)
    """
    strategy_name = "stgy_makerjay"

    def __init__(self, money, threshold, lot_k, min_sizer = 0.007) -> None:
        self.money = money
        self.lot1 = min_sizer
        self.lot2 = min_sizer * lot_k
        self.thd1 = self.lot1 * threshold
        self.thd2 = self.lot2 * threshold
        self.comm_rate = 0

        # place holder for the variables
        self.position = 0
        self.avg_buy = 0
        self.avg_sell = 0
        self.turnover_buy = 0
        self.turnover_sell = 0

    def _made_trade(self, high, low, buy1, buy2, sell1, sell2) -> float:
        """ when high is higher than sell1, sell2, we make one lot of sell trade at sell1, two lots at sell2;
            when low is lower than buy1, buy2, we make one lot of buy trade at buy1, two lots at buy2"""
        commission = 0
        sizer_buy, sizer_sell = self._determine_sizer()

        if high > sell1:
            self.avg_sell = (self.avg_sell * self.turnover_sell + sell1 * sizer_sell )/(self.turnover_sell + sizer_sell)
            self.position -= sizer_sell
            self.turnover_sell += sizer_sell
            commission += self.comm_rate * sizer_sell * sell1

        if high > sell2:
            self.avg_sell = (self.avg_sell * self.turnover_sell + sell2 * sizer_sell * 2)/(self.turnover_sell + sizer_sell* 2)
            self.position -= sizer_sell* 2
            self.turnover_sell += sizer_sell* 2
            commission += self.comm_rate * sizer_sell * sell2 * 2

        if low < buy1:
            self.avg_buy = (self.avg_buy * self.turnover_buy + buy1 * sizer_buy )/(self.turnover_buy + sizer_buy)
            self.position += sizer_buy
            self.turnover_buy += sizer_buy
            commission += self.comm_rate * sizer_buy * buy1
    
        if low < buy2:
            self.avg_buy = (self.avg_buy * self.turnover_buy + buy2 * sizer_buy* 2 )/(self.turnover_buy + sizer_buy* 2)
            self.position += sizer_buy* 2
            self.turnover_buy += sizer_buy* 2
            commission += self.comm_rate * sizer_buy * buy2* 2

        return commission
    
    def _determine_sizer(self) -> tuple:
        if self.position < -self.thd1:
            sizer_buy = self.lot2 
            sizer_sell = self.lot1 
        elif self.position > self.thd1:
            sizer_sell = self.lot2 
            sizer_buy = self.lot1 
        else:
            sizer_buy = self.lot1 
            sizer_sell = self.lot1 
        return sizer_buy, sizer_sell
    
    def _calculate_values(self, close) -> tuple:
        if self.position > 0:
            unrealized_pnl = self.position * (close - self.avg_buy)
            realized_pnl = self.turnover_sell * (self.avg_sell - self.avg_buy)
        elif self.position < 0:
            unrealized_pnl = self.position * (close - self.avg_sell)
            realized_pnl = self.turnover_buy * (self.avg_sell - self.avg_buy)
        else:
            unrealized_pnl = 0
            realized_pnl = self.turnover_buy * (self.avg_sell - self.avg_buy)
        value = self.money + unrealized_pnl + realized_pnl
        return value, unrealized_pnl, realized_pnl
    
    def _manage_position(self, close) -> float:
        commission = 0
        if self.position < -self.thd2:
            self.avg_buy = (self.avg_buy * self.turnover_buy + close * (self.turnover_sell - self.turnover_buy))/self.turnover_sell
            self.position = 0 
            self.turnover_buy = self.turnover_sell
            commission = self.comm_rate * (self.turnover_sell - self.turnover_buy) * close

        if self.position > self.thd2:
            self.avg_sell = (self.avg_sell * self.turnover_sell + close * (self.turnover_buy - self.turnover_sell))/self.turnover_buy
            self.position = 0
            self.turnover_sell = self.turnover_buy
            commission = self.comm_rate * (self.turnover_buy - self.turnover_sell) * close
    
        return commission

    
    def _initialize_portfolio_variables(self, maker_price_df: pd.DataFrame) -> pd.DataFrame:
        portfolio = maker_price_df[["high", "low", "close"]]
        portfolio["value"] = np.zeros(len(portfolio))
        portfolio["position"] = np.zeros(len(portfolio))
        portfolio["unrealized_pnl"] = np.zeros(len(portfolio))
        portfolio["realized_pnl"] = np.zeros(len(portfolio))
        portfolio["avg_buy"] = np.zeros(len(portfolio))
        portfolio["avg_sell"] = np.zeros(len(portfolio))
        portfolio["inventory"] = np.zeros(len(portfolio))
        portfolio["commission"] = np.zeros(len(portfolio))
        return portfolio
    
    def _record_values(self, portfolio, index, value, unrealized_pnl, realized_pnl, commission) -> pd.DataFrame:
        portfolio["value"].at[index] = value
        portfolio["position"].at[index] = self.position
        portfolio["unrealized_pnl"].at[index] = unrealized_pnl
        portfolio["realized_pnl"].at[index] = realized_pnl
        portfolio["avg_buy"].at[index] = self.avg_buy
        portfolio["avg_sell"].at[index] = self.avg_sell
        portfolio["inventory"].at[index] = abs(self.turnover_buy - self.turnover_sell)
        portfolio["commission"].at[index] = commission

        return portfolio

    def generate_portfolio(self, maker_price_df: pd.DataFrame) -> pd.DataFrame:
        """
        Args:
            index_signal (pd.DataFrame): dataframe with columns high, low, close, signal, atr, dema
        """
        portfolio = self._initialize_portfolio_variables(maker_price_df)

        for index, row in maker_price_df.iterrows():
            high = row.high
            low = row.low
            close = row.close
            buy1 = row.buy1
            sell1 = row.sell1
            buy2 = row.buy2
            sell2 = row.sell2

            comm_clearance = self._manage_position(close)
            comm_fee = self._made_trade(high, low, buy1, buy2, sell1, sell2)
            value, unrealized_pnl, realized_pnl = self._calculate_values(close)
            portfolio = self._record_values(portfolio, index, value, unrealized_pnl, realized_pnl, comm_clearance+comm_fee)
        
        return portfolio
        
    def calculate_performance(self, result) -> pd.DataFrame:
        net_value = result["value"][-1] - self.money
        max_drawdown = np.max(np.maximum.accumulate(result["value"]) - result["value"])
        max_inventory = np.max(result["inventory"])
        ret = net_value / self.money 
        comm_ratio = result["commission"].sum() / self.money
        score = net_value / max_drawdown

        result = pd.DataFrame({
            "net_value": net_value,
            "max_drawdown": max_drawdown,
            "max_inventory": max_inventory,
            "score": score,
            "return": ret,
            "comm_ratio": comm_ratio
        }, index=[0])
        return result

