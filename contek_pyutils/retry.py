import logging
from functools import wraps
from typing import Callable

from ratelimit import limits, sleep_and_retry
from tenacity import retry
from tenacity.stop import stop_after_attempt
from tenacity.wait import wait_fixed


def retry_and_log_exception(
    calls: int,
    period: int = 1,
    max_attempt_number: int = 30,
    wait: float = 2.0,
) -> Callable:
    """
    A decorator factory function that packs commonly used three request decorators
    into one and logs the exceptions raised in the decorated function

    :param calls: The maximum number of calls allowed within the specified period
    :param period: The period (in seconds) for which the limit applies
    :param max_attempt_number: The maximum number of attempts to execute the decorated
    function in case of failure
    :param wait: The time (in seconds) to wait before making another attempt
    :return: The decorated function
    """

    def decorator(f):
        @wraps(f)
        @retry(stop=stop_after_attempt(max_attempt_number), wait=wait_fixed(wait))
        @sleep_and_retry
        @limits(calls=calls, period=period)
        def wrapper(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except Exception:
                logger = logging.getLogger(f.__module__)
                logger.exception(f"Exception occurred in {f.__name__}")
                raise

        return wrapper

    return decorator
