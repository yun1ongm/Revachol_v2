import types
import typing
from typing import Any


def origin_is_union(tp: type[Any] | None) -> bool:
    return tp is typing.Union or tp is types.UnionType  # noqa: E721
