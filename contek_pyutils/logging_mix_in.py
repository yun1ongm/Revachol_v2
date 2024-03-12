import logging
from abc import ABC, abstractmethod
from typing import Callable


class LoggingMixIn(ABC):
    def __init__(self, name=None):
        self.__setup(name=None)

    def __setup(self, name=None):
        self._logger = logging.getLogger(name)
        self.debug = self._log(logging.DEBUG)
        self.info = self._log(logging.INFO)
        self.error = self._log(logging.ERROR)
        self.critical = self._log(logging.CRITICAL)
        self.exception = self._log(logging.ERROR, exc_info=True)
        self.warning = self._log(logging.WARNING)

    def _log(self, level: int, exc_info=False):
        def log_0(content: str, *, stlevel=0):
            self._logger.log(
                level,
                f"{self.log_fmt(content)}",
                exc_info=exc_info,
                stacklevel=stlevel + 2,
            )

        return log_0

    @abstractmethod
    def log_fmt(self, content: str) -> str:
        raise NotImplementedError


class NamedLogger(LoggingMixIn):
    def __init__(self, fmt: str):
        LoggingMixIn.__init__(self)
        self._fmt = fmt

    def log_fmt(self, content: str) -> str:
        return self._fmt.format(content)


def inject_logger(fmt: str):
    def decorator(func: Callable):
        # noinspection PyUnresolvedReferences
        func.__contek_inject_logger__ = NamedLogger(fmt)

        def wrapper(*args, **kwargs):
            if kwargs.get("logger") is not None:
                # noinspection PyUnresolvedReferences
                func.__contek_inject_logger__.error("kwargs already has logger parameter")
                raise IndexError("kwargs already has logger parameter")
            # noinspection PyUnresolvedReferences
            kwargs["logger"] = func.__contek_inject_logger__
            return func(*args, **kwargs)

        return wrapper

    return decorator
