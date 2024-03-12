import os
from multiprocessing.resource_tracker import unregister
from multiprocessing.shared_memory import SharedMemory


class ContekSharedMemory(SharedMemory):
    def __init__(self, name=None, create=False, size=0, mode=None, persist=False):
        self._mode = 0o660
        if mode is not None:
            self._mode = mode
        umask = os.umask(0)
        super().__init__(name, create, size)
        os.umask(umask)

        if persist:
            unregister(self._name, "shared_memory")  # type: ignore
