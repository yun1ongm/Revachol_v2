import logging
import warnings
warnings.filterwarnings("ignore")
import time
import sys
main_path = "/Users/rivachol/Desktop/Rivachol_v2"
sys.path.append(main_path)
from binance.um_futures import UMFutures
from binance.error import ClientError
import contek_timbersaw as timbersaw
import pandas as pd
import pandas_ta as pta
import yaml
from datetime import datetime, timedelta
from retry import retry

class ExecPopinjay:
    """
    Executor 

    """
    executor = "exec_Popinjay"
    equity = 2000
    leverage = 5 
    columns = [
            "opentime",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "closetime",
            "volume_U",
            "num_trade",
            "taker_buy",
            "taker_buy_volume_U",
            "ignore",
            ]
    params = {'atr_len': 13, 'threshold': 60, 'lot_k': 1.0}
    logger = logging.getLogger(executor)

    def __init__(self, symbol, timeframe) -> None:
        config = self._read_config()
        self.client = self._connect_api(key=config["bn_api"]["key"], secret=config["bn_api"]["secret"])
        self.symbol = symbol
        self.timeframe = timeframe
        self.interval = 3
        if self.symbol == "BTCUSDC":
            self.digit = 1
            min_sizer = 0.002
        if self.symbol == "ETHUSDC":
            self.digit = 2
            min_sizer = 0.007
        else:
            self.digit = 3
            min_sizer = 0.005

        self.lot = min_sizer
        self.thd = self.lot * self.params['threshold']
        self.kdf = self.get_candle()

    def _read_config(self, rel_path = "/config.yaml") -> dict:
        try:
            with open(main_path + rel_path, 'r') as stream:
                config = yaml.safe_load(stream)
        except FileNotFoundError:
            self.logger.error('Config file not found')
            sys.exit(1)
        return config

    @retry(tries=3, delay=1)  
    def _connect_api(self, key, secret) -> UMFutures:
        """connect binance client with apikey and apisecret"""
        client = UMFutures(key=key, secret=secret, timeout=3)

        return client

    def get_candle(self) -> pd.DataFrame:       
        klines = self.client.continuous_klines(self.symbol, "PERPETUAL", self.timeframe, limit=240)
        klines.pop() # remove unfinished candle
        kdf = pd.DataFrame(
            klines,
            columns=self.columns,
        )
        kdf = self._convert_kdf_datatype(kdf)
        self.logger.info(f"Initial candle data: {kdf.closetime[-1]}")
        return kdf
    
    def _convert_kdf_datatype(self, kdf) -> pd.DataFrame:
        kdf.opentime = [
            datetime.utcfromtimestamp(int(x) / 1000.0) for x in kdf.opentime
        ]
        kdf.open = kdf.open.astype("float")
        kdf.high = kdf.high.astype("float")
        kdf.low = kdf.low.astype("float")
        kdf.close = kdf.close.astype("float")
        kdf.volume = kdf.volume.astype("float")
        kdf.closetime = [
            datetime.utcfromtimestamp(int(x) / 1000.0) for x in kdf.closetime
        ]
        kdf.volume_U = kdf.volume_U.astype("float")
        kdf.num_trade = kdf.num_trade.astype("int")
        kdf.taker_buy = kdf.taker_buy.astype("float")
        kdf.taker_buy_volume_U = kdf.taker_buy_volume_U.astype("float")
        kdf.ignore = kdf.ignore.astype("float")
        kdf.set_index("opentime", inplace=True)

        return kdf

    @retry(tries=1, delay=1)
    def update_klines(self) -> bool:
        latest_ohlcv = self.client.continuous_klines(
            self.symbol, "PERPETUAL", self.timeframe, limit=2
        )
        unfin_candle = latest_ohlcv.pop()
        latest_kdf = pd.DataFrame(
            latest_ohlcv,
            columns=self.columns,
        )
        latest_kdf = self._convert_kdf_datatype(latest_kdf)

        if latest_kdf.index[-1] == self.kdf.index[-1]:
            self.logger.info(f"Candle price: {float(unfin_candle[4])}.")
            return False
        else:
            self.kdf = pd.concat([self.kdf, latest_kdf])
            if len(self.kdf) > 60*12:
                self.kdf = self.get_candle()
                self.logger.warning(f"Market bot refresh candle data")
            self.logger.info(
                f"Candle close time: {self.kdf.closetime[-1]} Updated Close: {self.kdf.close[-1]} Volume(U): {round(float(self.kdf.volume_U[-1])/1000000,2)}mil"
            )
            return True
    def _check_time_match(self) -> bool:
        """check if the time of the candle matches the current time"""
        now = datetime.utcnow().replace(second=0, microsecond=0)
        candle_time = self.kdf.index[-1].replace(second=0, microsecond=0)
        if now - timedelta(minutes=1) == candle_time:
            return True
        else:
            return False

    def generate_maker_price(self, kdf:pd.DataFrame) -> dict:       
        kdf["atr"] = pta.atr(kdf["high"], kdf["low"], kdf["close"], length = self.params['atr_len'], mamode = "EMA")
        kdf['buy1'] = kdf['close'].shift(1) - kdf['atr'].shift(1) 
        kdf['sell1'] = kdf['close'].shift(1) + kdf['atr'].shift(1) 
        kdf['buy2'] = kdf['close'].shift(1) - kdf['atr'].shift(1) * 2
        kdf['sell2'] = kdf['close'].shift(1) + kdf['atr'].shift(1) * 2
        return kdf[['high', 'low', 'close', 'volume_U', 'buy1', 'sell1', 'buy2', 'sell2']]

    def _rounding_price(self, maker_price_df) -> tuple:
        buy1 = round(maker_price_df['buy1'][-1], self.digit)
        sell1 = round(maker_price_df['sell1'][-1], self.digit)
        buy2 = round(maker_price_df['buy2'][-1], self.digit)
        sell2 = round(maker_price_df['sell2'][-1], self.digit)
        return buy1, sell1, buy2, sell2
    
    @retry(tries=1, delay=1)       
    def _send_batch_order(self, lot, maker_price_df) -> list:
        """send buy and sell orders based on the maker price dataframe
        Args:
            lot (float): lot size
            maker_price_df (pd.DataFrame): dataframe includes buy1, sell1, buy2, sell2
        Returns:
            response (list): response from binance api
            """
        buy1, sell1, buy2, sell2 = self._rounding_price(maker_price_df)
        self.logger.info(f"Buy1: {buy1} Sell1: {sell1} Buy2: {buy2} Sell2: {sell2}")
        try:
            batchOrders = [
                {
                    "symbol":self.symbol,
                    "side": "SELL",
                    "type": "LIMIT",
                    "quantity": f"{lot}",
                    "timeInForce": "GTX",
                    "reduceOnly": "false",
                    "price": f"{sell1}"
                },
                {
                    "symbol":self.symbol,
                    "side": "SELL",
                    "type": "LIMIT",
                    "quantity": f"{lot*2}",
                    "timeInForce": "GTX",
                    "reduceOnly": "false",
                    "price": f"{sell2}"
                },
                {
                    "symbol":self.symbol,
                    "side": "BUY",
                    "type": "LIMIT",
                    "quantity": f"{lot}",
                    "timeInForce": "GTX",
                    "reduceOnly": "false",
                    "price": f"{buy1}"
                },
                {
                    "symbol":self.symbol,
                    "side": "BUY",
                    "type": "LIMIT",
                    "quantity": f"{lot*2}",
                    "timeInForce": "GTX",
                    "reduceOnly": "false",
                    "price": f"{buy2}"
                },
                ]
            response = self.client.new_batch_order(batchOrders)
            return response
        except ClientError as error:
            self.logger.error(error)
            
    @retry(tries=1, delay=1)  
    def _cancel_open_orders(self) -> None:
        orders = pd.DataFrame(self.client.get_all_orders(symbol=self.symbol))
        orders_unfin = 0
        if not orders.empty:
            open_orders = orders.query('status == ["NEW", "PARTIALLY_FILLED"]')
            for orderId in open_orders["orderId"]:
                orders_unfin += 1
                try:
                    self.client.cancel_order(symbol=self.symbol, orderId=orderId)
                except ClientError as error:
                    self.logger.error(error)
        self.logger.info(f"Cancelled {orders_unfin} open orders.")

    def _fetch_notional(self) -> float:
        try:
            positions = pd.DataFrame(self.client.get_position_risk(recvWindow=6000))
            notional = positions.query("symbol == @self.symbol").loc[:, "notional"]
            notionalAmt = float(notional)
            return notionalAmt
        except ClientError as error:
            self.logger.error(error)
    
    def _check_threshold(self) -> bool:
        """ check if the position is over the threshold"""
        notionalAmt = self._fetch_notional()
        ticker = float(self.client.ticker_price(symbol=self.symbol)['price'])
        if notionalAmt < -self.thd * ticker:
            self._close_position()
            self.logger.critical(f"***Position is over the threshold. Close the position***")
            return True
        elif notionalAmt > self.thd * ticker:
            self._close_position()
            self.logger.critical(f"***Position is over the threshold. Close the position***")
            return True
        else:
            return False
    
    def _close_position(self) -> None:
        try:
            positions = pd.DataFrame(self.client.get_position_risk(recvWindow=6000))
            positionAmt = positions.query("symbol == @self.symbol").loc[:, "positionAmt"]
            position = float(positionAmt)
            if position > 0:
                self.client.new_order(
                    symbol=self.symbol,
                    side="SELL",
                    type="MARKET",
                    quantity=abs(position),
                )
            elif position < 0:
                self.client.new_order(
                    symbol=self.symbol,
                    side="BUY",
                    type="MARKET",
                    quantity=abs(position),
                )
        except ClientError as error:
            self.logger.error(error)
        
    def task(self) -> bool:
        """main task of the executor
        1. cancel open orders
        2. check threshold
        3. update klines
        4. generate maker price
        5. send orders
        """
        self._cancel_open_orders()
        flag = self._check_threshold()
        if not flag:
            maker_price_df = self.generate_maker_price(self.kdf)
            response = self._send_batch_order(self.lot, maker_price_df)
            # buy1, sell1, buy2, sell2 = self._rounding_price(maker_price_df)
            # self.logger.info(f"close: {self.kdf.close[-1]} buy1: {buy1} sell1: {sell1} buy2: {buy2} sell2: {sell2}")
            return True
        return False
        
        
    def run(self) -> None:
        while True:
            if self._check_time_match():
                time.sleep(self.interval)
            else:
                flag = self.update_klines()
                if flag:
                    compelete = self.task()
                    if compelete:
                        self.logger.info(f"Task completed. Waiting for next task.\n------------------")
                    else:
                        self.logger.error(f"Task failed. Waiting for next task.\n------------------")
                    time.sleep(self.interval) 
                else:
                    time.sleep(self.interval/3)

if __name__ == "__main__":
    timbersaw.setup()
    executor = ExecPopinjay(symbol = "ETHUSDC", timeframe = "1m")
    executor.run()
