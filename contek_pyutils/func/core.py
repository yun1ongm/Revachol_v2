from typing import Callable, Optional, TypeVar

T = TypeVar("T")


def none_or(var: Optional[T], default: T) -> T:
    return default if var is None else var


def none_or_else(var: Optional[T], default_f: Callable[[], T]) -> T:
    return default_f() if var is None else var
