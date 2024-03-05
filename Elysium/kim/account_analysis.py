import okx.Account as Account
from binance.um_futures import UMFutures
import logging
import pandas as pd
import sys
temp_path = "/Users/rivachol/Desktop/Rivachol_v2/"
sys.path.append(temp_path)
import contek_timbersaw as timbersaw

class TradeAnalyzer:
    logger = logging.getLogger(__name__)
    
    def __init__(self, exchange: str, symbol: str):
        self.symbol = symbol
        if exchange == 'binance':
            from config.binance_api import bn_key, bn_secret
            self.client =UMFutures(key= bn_key, secret = bn_secret)  
            self.trades = self._bn_fetch_trades()

        
        elif exchange == 'okx':
            flag = "1"
            from config.okx_api import okx_key, okx_secret
            self.client = Account.AccountAPI(okx_key, okx_secret, flag=flag) 
    
    def _bn_fetch_trades(self) -> pd.DataFrame:
        response = self.client.get_account_trades(symbol=self.symbol, recvWindow=6000)
        res_df = pd.DataFrame(response)
        self.logger.info('Fetched', len(res_df), 'trades')  
        return res_df

    def _okx_fetch_trades(self) -> pd.DataFrame:
        response = self.client.get_positions_history(instId = "BTC-USDT-SWAP")
        res_df = pd.DataFrame(response)
        self.logger.info('Fetched', len(res_df), 'trades')  
        return res_df
    
    def analyze_trades(self, trades: pd.DataFrame):
        total_pnl = 0
        fees = 0
        wins = 0
        losses = 0

        for index,trade in trades.iterrows():
            pnl = float(trade['realizedPnl'])
            fee = float(trade['commission'])
            total_pnl += pnl
            fees += fee
            if pnl > 0:
                wins += 1
            elif pnl < 0:
                losses += 1
        self.logger.info(f"PNL:{total_pnl} Commision:{fees} wins:{wins} losses:{losses}")

        win_rate = wins / (wins + losses) if (wins + losses) > 0 else 0

        return {
            'total_pnl': total_pnl,
            'win_rate': win_rate,
            'fees': fees
        }

if __name__ == '__main__':
    timbersaw.setup()
    ta = TradeAnalyzer('binance', 'BTCUSDT')
    result = ta.analyze_trades(ta.trades)

