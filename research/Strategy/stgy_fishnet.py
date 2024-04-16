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
    strategy_name = "stgy_fishnet"

    def __init__(self, money, up_thd, down_thd, min_sizer = 0.007) -> None:
        self.money = money
        self.lot = min_sizer
        self.win_threshold = up_thd * self.money
        self.loss_threshold = down_thd * self.money
        self.comm_rate = 0

        # place holder for the variables
        self.position = 0
        self.avg_buy = 0
        self.avg_sell = 0
        self.turnover_buy = 0
        self.turnover_sell = 0

    def _made_trade(self, high, low, buy1, buy2, sell1, sell2) -> float:

        commission = 0

        if high > sell1:
            self.avg_sell = (self.avg_sell * self.turnover_sell + sell1 * self.lot )/(self.turnover_sell + self.lot)
            self.position -= self.lot
            self.turnover_sell += self.lot
            commission += self.comm_rate * self.lot * sell1

        if high > sell2:
            self.avg_sell = (self.avg_sell * self.turnover_sell + sell2 * self.lot * 2)/(self.turnover_sell + self.lot* 2)
            self.position -= self.lot* 2
            self.turnover_sell += self.lot* 2
            commission += self.comm_rate * self.lot * sell2 * 2

        if low < buy1:
            self.avg_buy = (self.avg_buy * self.turnover_buy + buy1 * self.lot )/(self.turnover_buy + self.lot)
            self.position += self.lot
            self.turnover_buy += self.lot
            commission += self.comm_rate * self.lot * buy1
    
        if low < buy2:
            self.avg_buy = (self.avg_buy * self.turnover_buy + buy2 * self.lot* 2 )/(self.turnover_buy + self.lot* 2)
            self.position += self.lot* 2
            self.turnover_buy += self.lot* 2
            commission += self.comm_rate * self.lot * buy2* 2

        return commission
    
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
    
    def _manage_position(self, close, unrealized_pnl) -> float:
        commission = 0
        clear_condition = unrealized_pnl < -self.loss_threshold or unrealized_pnl > self.win_threshold
        if self.position < 0 and clear_condition:
            self.avg_buy = (self.avg_buy * self.turnover_buy + close * (self.turnover_sell - self.turnover_buy))/self.turnover_sell
            self.position = 0 
            self.turnover_buy = self.turnover_sell
            commission = self.comm_rate * (self.turnover_sell - self.turnover_buy) * close

        if self.position > 0 and clear_condition:
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

            comm_fee = self._made_trade(high, low, buy1, buy2, sell1, sell2)
            value, unrealized_pnl, realized_pnl = self._calculate_values(close)
            comm_clearance = self._manage_position(close, unrealized_pnl)
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

