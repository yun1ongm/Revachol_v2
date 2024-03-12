from typing import Iterable

from expression import Option, effect


@effect.option()
def join_opt(xss: Option[Option]):
    xs = yield from xss
    x = yield from xs
    return x


@effect.option()
def combine_maybes(ms: Iterable[Option]):
    res = []
    for m in ms:
        x = yield from m
        res.append(x)
    return res
