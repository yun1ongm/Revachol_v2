from __future__ import annotations

import asyncio
import inspect
import itertools
import os
from dataclasses import dataclass, field
from functools import reduce, singledispatchmethod
from typing import (
    Callable,
    ClassVar,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
)

import nest_asyncio
import yaml
from expression import Nothing, Option, Some, effect
from expression.core.option import of_optional

from contek_pyutils.config import load_yaml
from contek_pyutils.coro import exec_coroutines
from contek_pyutils.env import configs_repo_token, deploy_env, is_interactive
from contek_pyutils.func.core import none_or, none_or_else
from contek_pyutils.github import dir_from_github, file_from_github, load_dir
from contek_pyutils.instrument_info import InstrumentInfo, SymbolType
from contek_pyutils.singleton import Memoized, Singleton

__all__ = ["CanonicalSymbol", "Universe", "SymbolLibrary"]


if is_interactive():
    nest_asyncio.apply()


@dataclass(frozen=True, order=True, slots=True)
class CanonicalSymbol(metaclass=Memoized):
    canonical: str = field(hash=False, compare=False)
    _id: int = field(hash=True, compare=True)

    def __init__(self, canonical: str, cid_generator: Optional[Iterator[int]] = None):
        if cid_generator is None:
            raise ValueError(f"{canonical} is invalid")
        object.__setattr__(self, "canonical", canonical)
        object.__setattr__(self, "_id", next(cid_generator))

    def __str__(self):
        return self.canonical

    def __repr__(self):
        return f"CanonicalSymbol({self.canonical},{self._id})"


class SymbolLibrary(metaclass=Singleton):
    def __init__(self, content=None):
        if content is None:
            raise ValueError("SymbolLibrary has not been initialized yet")
        self.content = content

    @classmethod
    async def create(
        cls,
        load_from: str | None = None,
        *,
        token: str | None = None,
        repo_owner: str | None = None,
    ):
        load_from = os.path.expanduser(none_or(load_from, "configs_symbol_library"))
        repo_owner = none_or(repo_owner, "contek-io")
        raw_symbol_library: dict = {}
        tasks_to_complete = []
        if not os.path.exists(load_from):
            repo, branch = load_from.split("@") if "@" in load_from else (load_from, "master")

            async def f(r, b):
                nonlocal raw_symbol_library
                raw_symbol_library = await dir_from_github(
                    none_or_else(token, configs_repo_token),
                    repo=r,
                    owner=repo_owner,
                    branch=b,
                )

            tasks_to_complete.append(f(repo, branch))
        else:
            raw_symbol_library = load_dir(load_from)
        await exec_coroutines(tasks_to_complete)
        content = {}
        for d, ts in raw_symbol_library.items():
            if isinstance(ts, dict):
                for t, symbols in filter(lambda xy: xy[0].endswith(".yml"), ts.items()):
                    content.setdefault(d, {})[t[:-4]] = yaml.full_load(symbols)
            elif d.endswith(".yml"):
                content[d[:-4]] = yaml.full_load(ts)
        return cls(content)

    def is_active(self, db, table) -> Callable[[CanonicalSymbol], bool]:
        return lambda x: self.content[db][table][x.canonical].get("active", True)


@dataclass(frozen=True, slots=True)
class Universe(metaclass=Memoized):
    name: str = field(compare=True)
    abbr: Option[str] = field(default=Nothing, compare=False, hash=False)

    # CanonicalSymbol -> id in the universe,
    symbols: Dict[CanonicalSymbol, int] = field(default_factory=dict, compare=False, hash=False)
    __supported_ops: ClassVar[Set[str]] = {"(", ")", "&", "|", "%", "-", "[", "]", ","}

    __proxy_str_methods: ClassVar[dict[str, Callable]] = dict(
        filter(
            lambda x: "_" not in x[0],
            inspect.getmembers(str, predicate=inspect.isroutine),
        )
    ) | {"to": str.replace, "add": str.__add__}

    @classmethod
    def __call_proxy_method(cls, method, *args):
        str_method = cls.__proxy_str_methods[method]

        def m(u: Universe):
            new_names = filter(
                CanonicalSymbol.is_init,
                (str_method(c.canonical, *args) for c in u.symbols.keys()),
            )
            return cls(
                name=None,
                abbr=cls._get_abbr(u.abbr, Some(method + str(args)), "%"),
                symbols_list=new_names,
            )

        return m

    def __init__(
        self,
        name: Optional[str] = None,
        abbr: Optional[Option[str]] = None,
        symbols_list: Optional[Iterable[CanonicalSymbol | str]] = None,
    ):
        if abbr is None or symbols_list is None:
            raise ValueError(f"Universe {name} not exists")
        if not symbols_list:
            raise ValueError(f"Invalid empty {name} universe")
        symbols_list = sorted(
            map(
                lambda x: x if isinstance(x, CanonicalSymbol) else CanonicalSymbol(x),
                symbols_list,
            )
        )
        if name is None:
            name = "_".join(map(lambda x: str(x), symbols_list))
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "abbr", abbr)
        object.__setattr__(self, "symbols", {cs: index for index, cs in enumerate(symbols_list)})

    @singledispatchmethod
    @classmethod
    def get(cls, x):
        raise NotImplementedError(f"Cannot get universe from {x}")

    @get.register
    @classmethod
    def _(cls, symbols_list: list):
        canonical_symbols = list(map(lambda s: CanonicalSymbol(s), symbols_list))
        canonical_symbols.sort()
        name = "_".join(map(lambda x: x.canonical, canonical_symbols))
        return cls(name, Nothing, canonical_symbols)

    @get.register
    @classmethod
    def _(cls, token: str):
        if any(map(lambda x: x in cls.__supported_ops, token)):

            def f(op):
                def g(x):
                    to_replace = f" {op} "
                    return x.replace(op, to_replace)

                return g

            universe_expr = reduce(lambda u, h: h(u), [f(op) for op in cls.__supported_ops], token).split()

            def get_sub_expr():
                i = 0
                while i < len(universe_expr):
                    if universe_expr[i] == "[":
                        start = i
                        end = universe_expr.index("]", start)
                        yield list(filter(lambda x: x != ",", universe_expr[start + 1 : end]))
                        i = end + 1
                    else:
                        yield universe_expr[i]
                        i += 1

            def format_expr(expr):
                if isinstance(expr, list):
                    return f"Universe.get({expr})"
                elif isinstance(expr, str):
                    if expr in cls.__supported_ops:
                        return expr
                    else:
                        return f"Universe.get('{expr}')"
                else:
                    raise ValueError(f"Unknown expr {expr} with type {type(expr)}")

            return eval(" ".join(map(format_expr, get_sub_expr())))

        elif cls.is_init(token):
            return cls(token)
        elif CanonicalSymbol.is_init(token):
            return CanonicalSymbol(token)
        elif hasattr(cls, token):
            return getattr(cls, token)
        elif any(lambda x: x in cls.__proxy_str_methods for x in token.split("_")):
            expr = token.split("_")
            if expr[0] in cls.__proxy_str_methods:
                return cls.__call_proxy_method(expr[0], *map(str.upper, expr[1:]))
            elif expr[1] in cls.__proxy_str_methods and len(expr) == 3:
                return cls.__call_proxy_method(expr[1], expr[0].upper(), expr[2].upper())

        raise ValueError(f"Unknown token {token}")

    def __iter__(self):
        return self.symbols.__iter__()

    def pop(self, symbol, default):
        self.symbols.pop(symbol, default)

    def __len__(self) -> int:
        return len(self.symbols)

    def __getitem__(self, item: CanonicalSymbol) -> int:
        return self.symbols[item]

    def get_id(self, symbol: CanonicalSymbol) -> Option[int]:
        return of_optional(self.symbols.get(symbol))

    def __delitem__(self, key):
        del self.symbols[key]

    def __contains__(self, key):
        return key in self.symbols

    def __and__(self, other):
        return self.intersection(other).value

    def __or__(self, other):
        return self.union(other)

    def __str__(self):
        return self.abbr.default_value(self.name)

    @staticmethod
    @effect.option[str]()
    def _get_abbr(self_abbr_opt: Option[str], other_abbr_opt: Option[str], conj: str):
        self_abbr = yield from self_abbr_opt
        other_abbr = yield from other_abbr_opt
        return f"({self_abbr} {conj} {other_abbr})"

    def intersection(self, other: Universe) -> Option[Universe]:
        res_symbols = self.symbols.keys() & other.symbols.keys()
        is_superset_of_other = len(res_symbols) == len(other)
        is_subset_of_other = len(res_symbols) == len(self)
        if is_superset_of_other:
            return Some(other)
        elif is_subset_of_other:
            return Some(self)
        else:
            if not res_symbols:
                return Nothing
            else:
                return Some(Universe(None, self._get_abbr(self.abbr, other.abbr, "x"), res_symbols))

    def __mod__(self, mapper):
        return mapper(self)

    def __sub__(self, other):
        if not isinstance(other, (Universe, CanonicalSymbol)):
            raise ValueError("Sub different types")
        if isinstance(other, Universe):
            return self.difference(other).value
        elif isinstance(other, CanonicalSymbol):
            return self.difference(Universe(other.canonical, Some(str(other)), [other])).value

    def difference(self, other: Universe) -> Option[Universe]:
        self_symbols = set(self.symbols.keys())
        other_symbols = set(other.symbols.keys())
        res_symbols = self_symbols - other_symbols
        if len(res_symbols) == len(self_symbols):
            return Some(self)
        elif len(res_symbols) == 0:
            return Nothing
        else:
            return Some(Universe(None, self._get_abbr(self.abbr, other.abbr, "-"), res_symbols))

    def union(self, other: Universe) -> Universe:
        res_symbols = self.symbols | other.symbols
        if len(res_symbols) == len(self):
            return self
        elif len(res_symbols) == len(other):
            return other
        else:
            other_abbr = other.abbr
            return Universe(None, self._get_abbr(self.abbr, other_abbr, "+"), res_symbols)

    def non_active(
        self, esqs: List[Tuple[str, SymbolType, str]], instrument_info: InstrumentInfo
    ) -> Dict[CanonicalSymbol, int]:
        def is_non_active(x: CanonicalSymbol) -> bool:
            mapping_keys = ((esq[0], x.canonical, esq[1], esq[2]) for esq in esqs)
            return any(k not in instrument_info.canonical_symbol_mapping for k in mapping_keys)

        return {c: self.symbols[c] for c in filter(is_non_active, self.symbols.keys())}

    @classmethod
    async def load_raw_universes(
        cls,
        load_universes_from: str | None = None,
        load_symbol_library_from: str | None = None,
        *,
        universe_file_name: str | None = None,
        token: str | None = None,
        repo_owner: str | None = None,
    ):
        load_universes_from = os.path.expanduser(
            none_or(load_universes_from, f"configs_{deploy_env()}_universe@master")
        )
        load_symbol_library_from = os.path.expanduser(
            none_or(load_symbol_library_from, "configs_symbol_library@master")
        )
        universe_file_name = none_or(universe_file_name, "universes.yml")
        repo_owner = none_or(repo_owner, "contek-io")
        raw_predefined_universes = {}
        tasks_to_complete = []
        if not os.path.exists(load_universes_from) or "@" in load_universes_from:
            repo, branch = (
                load_universes_from.split("@") if "@" in load_universes_from else (load_universes_from, "master")
            )

            async def f(r, b, universe_file):
                nonlocal raw_predefined_universes
                raw_predefined_universes = yaml.full_load(
                    await file_from_github(
                        none_or_else(token, configs_repo_token),
                        repo=r,
                        owner=repo_owner,
                        branch=b,
                        path=universe_file,
                    )
                )

            tasks_to_complete.append(f(repo, branch, universe_file_name))
        else:
            raw_predefined_universes = load_yaml(load_universes_from)

        def add_universe(name, syms):
            if name in raw_predefined_universes:
                raise ValueError(f"Duplicated universe {name}")
            raw_predefined_universes[name] = syms

        tasks_to_complete.append(SymbolLibrary.create(load_symbol_library_from, token=token, repo_owner=repo_owner))
        await exec_coroutines(tasks_to_complete)
        raw_symbol_library = SymbolLibrary().content

        for d, ts in raw_symbol_library.items():
            if d == "all_symbols":
                add_universe(d, list(ts.keys()))
            else:
                for t, symbols in ts.items():
                    add_universe(f"{d}_{t}", list(symbols.keys()))

        return raw_predefined_universes

    @classmethod
    def create_from_raw(cls, raw_predefined_universes, cid_from: Sequence[str] = ("all_symbols",)):
        cid_counter = itertools.count(0, 1)
        cid_universes = {
            name: cls(
                name,
                Some(name),
                [CanonicalSymbol(cs, cid_counter) for cs in raw_predefined_universes[name]],
            )
            for name in cid_from
        }
        other_predefined_universes = {
            name: cls(name, Some(name), symbols)
            for name, symbols in filter(lambda ns: ns[0] not in cid_from, raw_predefined_universes.items())
        }
        return cid_universes | other_predefined_universes

    @classmethod
    async def fetch_universe_and_symbols(
        cls,
        load_universes_from: str | None = None,
        load_symbol_library_from: str | None = None,
        *,
        universe_file_name: str | None = None,
        token: str | None = None,
        repo_owner: str | None = None,
        cid_from: Sequence[str] = ("all_symbols",),
    ):
        raw_predefined_universes = await cls.load_raw_universes(
            load_universes_from,
            load_symbol_library_from,
            universe_file_name=universe_file_name,
            token=token,
            repo_owner=repo_owner,
        )
        return cls.create_from_raw(raw_predefined_universes, cid_from)


if __name__ == "__main__":
    asyncio.run(
        Universe.fetch_universe_and_symbols(
            load_universes_from="~/cfg/configs_canary_universe/universes.yml",
            load_symbol_library_from="~/cfg/configs_symbol_library",
            cid_from=("all_symbols",),
        )
    )
    u2 = Universe.get("(OWL_TRADE_UNIVERSE % usd_to_krw) & upbit_candle_krw")
    u1 = Universe.get("(upbit_candle_krw % krw_to_usd) & OWL_TRADE_UNIVERSE")
    assert len(u2) == len(u1)
    print(u1.abbr)
    print(u2.abbr)

    u3 = Universe.get("((OWL_TRADE_UNIVERSE % usd_to_krw) & upbit_candle_krw) % krw_to_usd")
    print(u3.name)
    print(u1.name)
    assert u3 == u1
    print(u2.name)
    print(len(Universe.get("OWL_TRADE_UNIVERSE % usd_to_krw % krw_to_usd")))
    print(len(Universe.get("upbit_candle_krw % krw_to_usd % usd_to_krw")))
    print(len(Universe.get("upbit_candle_krw % krw_to_usd")))
    print(Universe.get("upbit_candle_krw % removesuffix_krw % add_krw").abbr)
    print(SymbolLibrary().is_active("upbit", "candle_krw")(CanonicalSymbol("BTCKRW")))
