import asyncio
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Callable

from contek_pyutils.logging_mix_in import LoggingMixIn


class Loadable(ABC):
    def __init__(self, load_when_init: bool = False):
        if load_when_init:
            asyncio.run(self.load())

    @abstractmethod
    async def load(self):
        raise NotImplementedError(f"{self.__class__.__name__}.load()")


class Reloadable(Loadable, LoggingMixIn, ABC):
    def __init__(self, load_when_init: bool = False):
        LoggingMixIn.__init__(self)
        Loadable.__init__(self, load_when_init)
        self._reload_version: int = 0
        self._last_update_time: str = "Uninitialized"

    def log_fmt(self, content: str):
        return f"[{self.__class__.__name__}] - {content}"

    @property
    def version(self):
        return self._reload_version

    @property
    def last_update_time(self):
        return self._last_update_time

    async def reload(self):
        await self.load()

    async def recurring_reload(self, interval_in_s: int, callback: Callable = lambda e: None):
        class_name = self.__class__.__name__
        while True:
            self.info(f"Reload {class_name} with interval: {interval_in_s}")
            try:
                await self.reload()
                self._reload_version += 1
                self._last_update_time = str(datetime.utcnow())
                callback(self)
            except Exception:
                self.exception("Reload error")
                self.info(f"Reload {class_name} failed")
            else:
                self.info(f"Done reloading {class_name}")
            finally:
                await asyncio.sleep(interval_in_s)
