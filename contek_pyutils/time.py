from datetime import datetime
from typing import Union

import pandas as pd
import pytz
from pytimeparse import parse


def to_epoch_millis(time: Union[datetime, pd.Timestamp]) -> int:
    return int(time.timestamp() * 1000)


def now_epoch_millis() -> int:
    return to_epoch_millis(pd.Timestamp.utcnow())


def millis_to_timestamp(time_millis: int) -> pd.Timestamp:
    return pd.to_datetime(time_millis, unit="ms", utc=True)


def millis_to_datetime(time_millis: int) -> datetime:
    return datetime.fromtimestamp(time_millis / 1000.0, tz=pytz.utc)


def round_timestamp(timestamp: pd.Timestamp, interval: str) -> pd.Timestamp:
    if interval[-1] == "m":
        interval = interval.replace("m", "min")
    return timestamp.round(interval)


def interval_to_millis(interval: str) -> int | float:
    parse_result = parse(interval)
    if parse_result is not None:
        return parse_result * 1000
    else:
        raise ValueError("Invalid interval: " + interval)


def interval_to_seconds(interval: str) -> int | float:
    parse_result = parse(interval)
    if parse_result is not None:
        return parse_result
    else:
        raise ValueError("Invalid interval: " + interval)
