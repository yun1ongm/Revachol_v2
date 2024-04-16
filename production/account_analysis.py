import okx.Account as Account
from binance.um_futures import UMFutures
import logging
import pandas as pd
import yaml
import sys
main_path = "/Users/rivachol/Desktop/Rivachol_v2/"
sys.path.append(main_path)
import contek_timbersaw as timbersaw
import warnings
warnings.filterwarnings("ignore")

class TradeAnalyzer:
    logger = logging.getLogger(__name__)

    def __init__(self, exchange: str):
        config = self._read_config()
        self.exchange = exchange
        if exchange == 'binance':
            self.client =UMFutures(key= config["bn_api"]['key'], secret = config["bn_api"]['secret'])  

        elif exchange == 'okx':
            flag = "0"
            self.client = Account.AccountAPI(api_key=config["okx_api"]['key'],
                                             api_secret_key=config["okx_api"]['secret'], 
                                             passphrase= config["okx_api"]['passphrase'], 
                                             flag=flag,
                                             debug=False) 
        else:
            self.logger.error('Exchange not supported')
            sys.exit(1)

    def _read_config(self, rel_path = "config.yaml") -> dict:
        try:
            with open(main_path + rel_path, 'r') as stream:
                config = yaml.safe_load(stream)
        except FileNotFoundError:
            self.logger.error('Config file not found')
            sys.exit(1)
        return config
    
    def _bn_fetch_trades(self, symbol) -> pd.DataFrame:
        try:
            response = self.client.get_account_trades(symbol = symbol, recvWindow=6000)
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

    def _okx_fetch_trades(self, symbol) -> pd.DataFrame:
        try:
            response = self.client.get_positions_history(instId=symbol)
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
    
    def _generate_mapping(self, c_symbol: str):
        mapping = {}        
        base = c_symbol.split('USD')[0]
        if self.exchange == 'binance':
            mapping[c_symbol] = base + 'USDT'
        elif self.exchange == 'okx':
            mapping[c_symbol] = base + '-USDT-SWAP'
        return mapping
    
        
    def get_account_balance(self) -> pd.DataFrame:
        if self.exchange == 'binance':
            response = self.client.balance(recvWindow=6000)
            res_df = pd.DataFrame(response)
            balance = float(res_df[res_df['asset'] == 'USDT']['balance']) + float(res_df[res_df['asset'] == 'USDT']['crossUnPnl'])
            self.logger.info(f'Fetched balance: {balance}')
        elif self.exchange == 'okx':
            response = self.client.get_account_balance()
            balance = float(response['data'][0]["totalEq"])
            self.logger.info(f'Fetched balance: {balance}')
        else:
            balance = 0
            self.logger.error('Get account failed.')
        return balance
    
    
    def analyze_by_symbol(self, c_symbol: str) -> dict:
        symbol_mapping = self._generate_mapping(c_symbol)
        symbol = symbol_mapping[c_symbol]

        if self.exchange == 'binance':
            trades = self._bn_fetch_trades(symbol)
        elif self.exchange == 'okx':
            trades = self._okx_fetch_trades(symbol)
        else: 
            trades = pd.DataFrame()
            self.logger.error('Get trades failed.')

        total_pnl = 0
        fees = 0

        for index,trade in trades.iterrows():
            pnl = float(trade['realizedPnl'])
            fee = float(trade['commission'])
            total_pnl += pnl
            fees += fee
  
        self.logger.info(f"PNL:{total_pnl} Commision:{fees}")

        return {
            'total_pnl': total_pnl,
            'fees': fees
        }
    
    def show_equity_curve(self):
        final_value = self.get_account_balance()



if __name__ == '__main__':
    timbersaw.setup()
    ta = TradeAnalyzer('binance')
    result = ta.analyze_by_symbol('BTCUSD')
    print(result)
    balance = ta.get_account_balance()
    print(balance)

