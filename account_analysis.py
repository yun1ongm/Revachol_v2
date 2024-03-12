import okx.Account as Account
from binance.um_futures import UMFutures
import logging
import pandas as pd
import yaml

class TradeAnalyzer:
    logger = logging.getLogger(__name__)

    def __init__(self, exchange: str, c_symbol: str):
        self.symbol_mapping = self._generate_mapping(c_symbol, exchange)
        self.symbol = self.symbol_mapping[c_symbol]
        config = self._read_config()
        if exchange == 'binance':
            self.client =UMFutures(key= config["bn_api"]['key'], secret = config["bn_api"]['secret'])  
            self.trades = self._bn_fetch_trades()

        elif exchange == 'okx':
            flag = "1"
            self.client = Account.AccountAPI(api_key=config["okx_api"]['key'],
                                             api_secret_key=config["okx_api"]['secret'], 
                                             passphrase= config["okx_api"]['passphrase'], flag=flag) 
            self.trades = self._okx_fetch_trades()

    def _generate_mapping(self, symbol: str, exchange: str):
        mapping = {}        
        base = symbol.split('USD')[0]
        if exchange == 'binance':
            mapping[symbol] = base + 'USDT'
        elif exchange == 'okx':
            mapping[symbol] = base + '-USDT-SWAP'
        return mapping
    
    def _read_config(self, rel_path = "config.yaml") -> dict:
        try:
            with open(main_path + rel_path, 'r') as stream:
                config = yaml.safe_load(stream)
        except FileNotFoundError:
            self.logger.error('Config file not found')
            sys.exit(1)
        return config
    
    def _bn_fetch_trades(self) -> pd.DataFrame:
        try:
            response = self.client.get_account_trades(symbol=self.symbol, recvWindow=6000)
            if len(response) > 0:
                res_df = pd.DataFrame(response)
                self.logger.info(f'Fetched {len(res_df)} trades')   
            else:
                self.logger.info('No trades found')
                res_df = pd.DataFrame() 
        except Exception as e:
            self.logger.error(f'Error fetching trades: {e}')
            res_df = pd.DataFrame()  
        return res_df

    def _okx_fetch_trades(self) -> pd.DataFrame:
        try:
            response = self.client.get_positions_history(instId = self.symbol)
            if len(response) > 0:
                res_df = pd.DataFrame(response)
                self.logger.info(f'Fetched {len(res_df)} trades')  
            else:
                self.logger.info('No trades found')
                res_df = pd.DataFrame() 
        except Exception as e:
            self.logger.error(f'Error fetching trades: {e}')
            res_df = pd.DataFrame()  
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
    import sys
    main_path = "/Users/rivachol/Desktop/Rivachol_v2/"
    sys.path.append(main_path)
    import contek_timbersaw as timbersaw
    timbersaw.setup()
    ta = TradeAnalyzer('binance', 'BTCUSD')
    result = ta.analyze_trades(ta.trades)

