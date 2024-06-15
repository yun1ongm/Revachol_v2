from dataclasses import dataclass
from typing import Generic, Tuple, TypeVar, Union, cast

import pandas as pd
from expression import Error, Ok, pipe
from expression.collections import Block, block
from expression.core.typing import SupportsValidation
from expression.extra.parser import (
    _A,
    Parser,
    ParseResult,
    Remaining,
    choice,
    opt,
    parse_digit,
    pchar,
    pfloat,
    pstring,
    sequence,
)

_TBound = TypeVar("_TBound")


def parse_n_times(parser: Parser[_A], input: Remaining, n: int) -> ParseResult[Block[_A]]:
    if n <= 0:
        return Ok[Tuple[Block[_A], Remaining], str]((block.empty, input))

    first_result = parser.run(input)
    match first_result:
        case Ok((first_value, input_after_first_parse)):
            subsequent_result = parse_n_times(parser, input_after_first_parse, n - 1)
            match subsequent_result:
                case Ok((subsequent_values, remaining_input)):
                    values = subsequent_values.cons(cast(_A, first_value))
                    return Ok[Tuple[Block[_A], Remaining], str]((values, remaining_input))
                case Error(err):
                    return Error(err)
                case _:
                    return Error("parser error")
        case Error(err):
            return Error(err)
        case _:
            return Error("parser error")


def times(parser: Parser[_A], n: int) -> Parser[Block[_A]]:
    def run(input: Remaining) -> ParseResult[Block[_A]]:
        result = parse_n_times(parser, input, n)
        match result:
            case Ok((values, remaining_input)):
                return Ok((values, remaining_input))
            case Error(err):
                return Error(err)
            case _:
                return Error("parser error")

    return Parser(run, f"times_{n}({parser})")


def ndigit(n: int) -> Parser:
    return times(parse_digit, n).map(lambda x: "".join(x))


@dataclass
class Interval(SupportsValidation["Interval[_TBound]"], Generic[_TBound]):
    is_lower_bound_closed: bool
    is_upper_bound_closed: bool
    lower_bound: _TBound
    upper_bound: _TBound

    def __contains__(self, num: _TBound) -> bool:
        return (
            self.lower_bound < num < self.upper_bound
            or (num == self.lower_bound and self.is_lower_bound_closed)
            or (num == self.upper_bound and self.is_upper_bound_closed)
        )

    def __lt__(self, other):
        return self.upper_bound < other.lower_bound or (
            self.upper_bound == other.lower_bound
            and (not self.is_upper_bound_closed or not other.is_lower_bound_closed)
        )

    def __str__(self):
        return (
            f"Interval( {'[' if self.is_lower_bound_closed else '('}{self.lower_bound}, "
            f"{self.upper_bound}{']' if self.is_upper_bound_closed else ')'} )"
        )

    def __repr__(self):
        return self.__str__()


float_parser: Parser[str | float] = choice(
    block.of_seq(
        (
            pstring("inf"),
            pstring("+inf"),
            pstring("-inf"),
            pfloat,
        )
    )
).map(float)


def __float_str_to_timestamp(num_str: str) -> pd.Timestamp:
    match num_str:
        case "-inf":
            return pd.Timestamp.min.tz_localize(tz="utc")
        case "inf" | "+inf":
            return pd.Timestamp.max.tz_localize(tz="utc")
        case non_inf:
            return pd.Timestamp(non_inf, tz="utc")


timestamp_parser: Parser[str | pd.Timestamp] = choice(
    block.of_seq(
        (
            pstring("+inf"),
            pstring("-inf"),
            pstring("inf"),
            pipe(
                block.of_seq(
                    (
                        ndigit(4),
                        pchar("-"),
                        ndigit(2),
                        pchar("-"),
                        ndigit(2),
                        pchar(" "),
                        ndigit(2),
                        pchar(":"),
                        ndigit(2),
                        pchar(":"),
                        ndigit(2),
                    )
                ),
                sequence,
            ).map("".join),
        )
    )
).map(__float_str_to_timestamp)


def __make_interval_parser(bound_parser: Parser[str | _TBound]) -> Parser[str | Interval[_TBound]]:
    return sequence(
        block.of_seq(
            (
                pchar("(").or_else(pchar("[")).map(lambda x: x == "["),
                bound_parser,
                pchar(",").and_then(opt(pchar(" "))).ignore(),
                bound_parser,
                pchar(")").or_else(pchar("]")).map(lambda x: x == "]"),
            )
        )
    ).map(lambda x: tuple(x))


float_interval_parser = __make_interval_parser(float_parser)
timestamp_interval_parser = __make_interval_parser(timestamp_parser)


def parse_interval(
    input_str: str, interval_parser: Parser[str | Interval[_TBound]]
) -> Union[Ok[Interval[_TBound]], Error]:
    match interval_parser(input_str):
        case Ok((is_lower_bound_closed, lower_bound, _, upper_bound, is_upper_bound_closed)):
            return Ok(
                Interval(
                    is_lower_bound_closed,
                    is_upper_bound_closed,
                    lower_bound,
                    upper_bound,
                )
            )
        case Error(msg):
            return Error(msg)
        case _:
            return Error("interval parser error")
