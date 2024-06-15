import logging
from typing import Dict, List, Optional

import orjson
import requests
import yaml
from requests import Response

from contek_pyutils.retry import retry_and_log_exception
from contek_pyutils.singleton import Singleton

__all__ = ["SymbolApiClient"]

logger = logging.getLogger(__name__)


class SymbolApiClient(metaclass=Singleton):
    ADDRESS_MAPPING_ENDPOINT = "/get_address_map"
    SYMBOL_LIBRARY_ENDPOINT = "/get_symbol_library"
    C_SYMBOLS_ENDPOINT = "/get_c_symbols"
    INSTRUMENT_INFO_ENDPOINT = "/get_instrument_info"

    def __init__(
        self,
        host: str = "3.108.172.139",
        port: int = 18964,
    ):
        self.base_url = f"http://{host}:{port}"

    @retry_and_log_exception(calls=30)
    def _request_data(self, endpoint: str, params: Optional[dict] = None) -> Response:
        url = self.base_url + endpoint
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            return response
        else:
            logger.warning(msg=(response, response.reason, url, params))
            raise requests.exceptions.HTTPError(response)

    def get_address_mapping(self, chain: str, address: Optional[str] = None) -> Dict[str, str]:
        """
        This function returns the subject of the specified address
        on the blockchain by calling the symbol server API.

        :param chain: specify the blockchain to be queried
        :param address: specifies the address to be queried
        :return:
            If an address is specified,
            only the result of a single address are returned,
            otherwise all results for the specified blockchain are returned
        """
        params = dict(chain=chain, address=address)
        data = self._request_data(self.ADDRESS_MAPPING_ENDPOINT, params=params)
        address_mapping = orjson.loads(data.content)
        return address_mapping

    def get_symbol_library(
        self,
        exchange: str,
        symbol_type: str,
        quote: str,
        only_active: bool = True,
    ) -> List[str]:
        """
        Get symbol_library by exchange, symbol_type and quote.

        :param exchange: binance, bybit, okex, coinbase, deribit, upbit
        :param symbol_type: LINEAR_PERP, INVERSE_PERP, SPOT
        :param quote: USDT, USD, KRW, USDC
        :param only_active: Whether only symbols in active are needed, True or False
        :return: List of c_symbols.
        """
        params = dict(
            exchange=exchange,
            symbol_type=symbol_type,
            quote=quote,
            only_active=only_active,
        )
        data = self._request_data(self.SYMBOL_LIBRARY_ENDPOINT, params=params)
        library = orjson.loads(data.content)["c_symbols"]
        return library

    def get_c_symbols(
        self,
        symbol_key: str,
        only_active: bool = True,
        database: Optional[str] = None,
        table: Optional[str] = None,
    ) -> List[str]:
        """
        The purpose of this function is to get the c_symbols in the symbol_library
        and universe according to the symbol_key,
        separate library or the intersection of library and universe are both supported.
        Please refer: https://github.com/contek-io/contek-pyutils/blob/master/contek_pyutils/symbol.py

        :param symbol_key: An easy-to-understand syntax to specify wanted c_symbols
        :param only_active: Whether only symbols in active are needed
        :param database: Must be specified if only_active is True
        :param table: Must be specified if only_active is True
        :return: List of c_symbols.

        Valid parameter combination examples:
        (symbol_key="OWL_TRADE_UNIVERSE", only_active=False)

        (symbol_key="OWL_TRADE_UNIVERSE", only_active=True,
         database="binance_futures", table="candle_usdt")

        (symbol_key="OWL_TRADE_UNIVERSE & binance_spot_candle_usdt", only_active=False)

        (symbol_key="(OWL_TRADE_UNIVERSE % usd_to_krw) & upbit_candle_krw", only_active=False)

        (symbol_key="(OWL_TRADE_UNIVERSE % usd_to_krw) & upbit_candle_krw",
         only_active=True, database="upbit", table="candle_krw")
        ...
        """
        if only_active:
            assert isinstance(database, str) and isinstance(
                table, str
            ), "Database and table must be specified if only_active is True"
            params = dict(symbol_key=symbol_key, only_active=True, database=database, table=table)
        else:
            params = dict(symbol_key=symbol_key, only_active=False)
        data = self._request_data(self.C_SYMBOLS_ENDPOINT, params)
        c_symbols = orjson.loads(data.content)["c_symbols"]
        return c_symbols

    def get_instrument_info(self, exchange: str, symbol_type: str, quote: str) -> List[dict]:
        """
        Get instrument_info by exchange, symbol_type and quote.
        :param exchange: binance, bybit, okex, coinbase, deribit, upbit...
        :param symbol_type: LINEAR_PERP, INVERSE_PERP, SPOT
        :param quote: USD, USDT, KRW, USDC
        :return: List of instrument_info.
        """
        data = self._request_data(self.INSTRUMENT_INFO_ENDPOINT, params=dict(exchange=exchange))
        instrument_info = yaml.safe_load(data.content)["markets"]
        needed_instrument_info = list(
            filter(
                lambda x: x["type"] == symbol_type and x["quote"] == quote,
                instrument_info,
            )
        )
        return needed_instrument_info
