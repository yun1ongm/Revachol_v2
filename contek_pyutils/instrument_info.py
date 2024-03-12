from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Dict, Optional, Set, Tuple

import nest_asyncio
import yaml

from contek_pyutils.config import load_yaml
from contek_pyutils.env import configs_repo_token, is_interactive
from contek_pyutils.func.core import none_or
from contek_pyutils.github import release_from_github
from contek_pyutils.singleton import Singleton

if is_interactive():
    nest_asyncio.apply()


class SymbolType(Enum):
    LINEAR_PERP = "LINEAR_PERP"
    INVERSE_PERP = "INVERSE_PERP"
    LINEAR_QUANTO = "LINEAR_QUANTO"
    SPOT = "SPOT"


@dataclass(frozen=True, slots=True)
class ExchangeSymbol:
    quote: str = field(hash=True)
    name: str = field(hash=True)
    exchange: str = field(hash=True)
    type: SymbolType = field(hash=True)
    canonical_name: str = field(compare=False, hash=False)
    margin: str = field(compare=False, hash=False)
    base: str = field(compare=False, hash=False)


class InstrumentInfo(metaclass=Singleton):
    def __init__(self):
        self._load_from_github = True
        self._github_token = ""
        self._github_org = ""
        self._instrument_info_repo, self._instrument_info_release = "", ""
        self._arg_instrument_info: str | Path = ""

    async def init(
        self,
        load_instrument_info: str | Path | None = None,
        *,
        token: Optional[str] = None,
        org: str = "contek-io",
    ):
        load_instrument_info = none_or(load_instrument_info, "configs_instrument_info@latest")
        load_instrument_info = os.path.expanduser(load_instrument_info)
        load_from_github = not os.path.exists(load_instrument_info) or "@" in load_instrument_info
        self._load_from_github = load_from_github
        if load_from_github:
            self._instrument_info_repo, self._instrument_info_release = (
                load_instrument_info.split("@") if "@" in load_instrument_info else (load_instrument_info, "latest")
            )
            self._github_token = token if token is not None else configs_repo_token()
            self._github_org = org
        else:
            if isinstance(load_instrument_info, str):
                load_instrument_info = Path(os.path.expanduser(load_instrument_info))
            if not load_instrument_info.is_dir():  # type: ignore
                raise ValueError("Instrument Info should be a directory")
        self._arg_instrument_info = load_instrument_info
        await self._load_configs()

    @classmethod
    async def create(
        cls,
        load_instrument_info: str | Path | None = None,
        *,
        token: Optional[str] = None,
        org: str = "contek-io",
    ) -> InstrumentInfo:
        instance = cls()
        await instance.init(load_instrument_info, token=token, org=org)
        return instance

    async def reload_configs(self):
        try:
            await self._load_configs()
            return True
        except Exception as e:
            logging.error(f"reload instrument info failed with exception {e}")
            return False

    async def _load_configs(self):
        if self._load_from_github:
            mapping_config_generator = map(
                lambda dir_and_content: (
                    dir_and_content[0][:-4],
                    yaml.safe_load(dir_and_content[1])["markets"],
                ),
                filter(
                    lambda dir_and_content: dir_and_content[0].endswith(".yml"),
                    (
                        await release_from_github(
                            self._github_token,
                            self._instrument_info_repo,
                            self._github_org,
                            self._instrument_info_release,
                        )
                    ).items(),
                ),
            )
        else:
            assert isinstance(self._arg_instrument_info, Path)
            mapping_config_generator = map(
                lambda e: (e.stem, load_yaml(e)["markets"]),
                filter(lambda c: c.suffix == ".yml", self._arg_instrument_info.iterdir()),
            )
        # Exchange -> Set[ExchangeSymbol]
        self.__instruments: Dict[str, Set[ExchangeSymbol]] = {}
        for exchange, symbols in mapping_config_generator:
            exchange_symbols_set = set()
            for raw_symbol_config in symbols:
                canonical_name = raw_symbol_config["canonical_symbol"]
                exch_name = raw_symbol_config["canonical_symbol"]
                symbol_type = SymbolType[raw_symbol_config["type"]]
                quote = raw_symbol_config["quote"]
                base = raw_symbol_config["base"]
                margin = raw_symbol_config["margin"]
                exch_symbol = ExchangeSymbol(
                    base=base,
                    quote=quote,
                    type=symbol_type,
                    margin=margin,
                    canonical_name=canonical_name,
                    name=exch_name,
                    exchange=exchange,
                )
                if exch_symbol in exchange_symbols_set:
                    raise ValueError(f"Duplicated exchange symbol of {exch_symbol}")
                else:
                    exchange_symbols_set.add(exch_symbol)
                self.__instruments[exchange] = exchange_symbols_set
        self.__generate_canonical_symbol_mapping()

    @property
    def instruments(self):
        return self.__instruments

    def __generate_canonical_symbol_mapping(self):
        # Exchange, Canonical name, SymbolType, Quote -> ExchangeSymbol
        self.__canonical_symbol_mapping: Dict[Tuple[str, str, SymbolType, str], ExchangeSymbol] = {}
        for exchange, exch_symbol_set in self.__instruments.items():
            for exch_symbol in exch_symbol_set:
                mapping_key = (
                    exchange,
                    exch_symbol.canonical_name,
                    exch_symbol.type,
                    exch_symbol.quote,
                )
                if mapping_key in self.__canonical_symbol_mapping:
                    raise ValueError(
                        f"Duplicated mapping from {mapping_key} "
                        f"to {(self.__canonical_symbol_mapping[mapping_key], exch_symbol)}"
                    )
                else:
                    self.__canonical_symbol_mapping[mapping_key] = exch_symbol

    @property
    def canonical_symbol_mapping(self):
        return self.__canonical_symbol_mapping
