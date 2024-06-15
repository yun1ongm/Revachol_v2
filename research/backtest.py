import pandas as pd
import numpy as np


class BacktestFramework:

    def initialize_portfolio_variables(self, kdf: pd.DataFrame) -> pd.DataFrame:
        portfolio = kdf[["open", "high", "low", "close", "volume_U"]]
        portfolio["value"] = np.zeros(len(portfolio))
        portfolio["signal"] = np.zeros(len(portfolio))
        portfolio["position"] = np.zeros(len(portfolio))
        portfolio["entry_price"] = np.zeros(len(portfolio))
        portfolio["stop_loss"] = np.zeros(len(portfolio))
        portfolio["take_profit"] = np.zeros(len(portfolio))
        portfolio["unrealized_pnl"] = np.zeros(len(portfolio))
        portfolio["realized_pnl"] = np.zeros(len(portfolio))
        portfolio["commission"] = np.zeros(len(portfolio))

        return portfolio

    def record_values_sltp(
        self,
        portfolio,
        index,
        value,
        signal,
        position,
        entry_price,
        stop_loss,
        take_profit,
        unrealized_pnl,
        realized_pnl,
        commission,
    ) -> pd.DataFrame:
        portfolio["value"].at[index] = value
        portfolio["signal"].at[index] = signal
        portfolio["position"].at[index] = position
        portfolio["entry_price"].at[index] = entry_price
        portfolio["stop_loss"].at[index] = stop_loss
        portfolio["take_profit"].at[index] = take_profit
        portfolio["unrealized_pnl"].at[index] = unrealized_pnl
        portfolio["realized_pnl"].at[index] = realized_pnl
        portfolio["commission"].at[index] = commission

        return portfolio

    def record_values(
        self,
        portfolio,
        index,
        value,
        signal,
        position,
        entry_price,
        unrealized_pnl,
        realized_pnl,
        commission,
    ) -> pd.DataFrame:
        portfolio["value"].at[index] = value
        portfolio["signal"].at[index] = signal
        portfolio["position"].at[index] = position
        portfolio["entry_price"].at[index] = entry_price
        portfolio["unrealized_pnl"].at[index] = unrealized_pnl
        portfolio["realized_pnl"].at[index] = realized_pnl
        portfolio["commission"].at[index] = commission

        return portfolio

    def calculate_performance(self, result) -> dict:
        trades = {
            "total": 0,
            "win": 0,
            "loss": 0,
            "cumulative_win": 0,
            "cumulative_loss": 0,
        }

        for pnl in result["realized_pnl"]:
            if pnl > 0:
                trades["total"] += 1
                trades["win"] += 1
                trades["cumulative_win"] += pnl
            elif pnl < 0:
                trades["total"] += 1
                trades["loss"] += 1
                trades["cumulative_loss"] += pnl

        final_value = result["value"][-1] + result["unrealized_pnl"][-1]
        initial_value = result["value"][0]
        net_value = final_value - initial_value
        max_drawdown = np.max(np.maximum.accumulate(result["value"]) - result["value"])
        avg_trade_pnl = net_value / (trades["total"] + 0.0001)
        win_ratio = trades["win"] / (trades["total"] + 0.0001)
        avg_winning = trades["cumulative_win"] / (trades["win"] + 0.0001)
        avg_losing = trades["cumulative_loss"] / (trades["loss"] + 0.0001)
        single_avg_wlr = -avg_winning / (avg_losing + 0.0001)
        ret = net_value / initial_value
        comm_ratio = result["commission"].sum() / initial_value
        score = win_ratio * net_value / (max_drawdown + 0.0001)

        sigma_sum = np.sum(
            (pnl - avg_trade_pnl) ** 2 for pnl in result["realized_pnl"] if pnl != 0
        )
        sigma = np.sqrt(sigma_sum / (trades["total"] + 0.0001))
        t_sharpe = net_value / sigma

        performances = {
            "net_value": net_value,
            "win_ratio": win_ratio,
            "single_avg_wlr": single_avg_wlr,
            "total_trades": trades["total"],
            "return": ret,
            "max_drawdown": max_drawdown,
            "t_sharpe": t_sharpe,
            "commission": comm_ratio,
            "score": score,
        }

        return performances
