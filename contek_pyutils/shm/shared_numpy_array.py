import pickle
from typing import Optional

import numpy as np

from contek_pyutils.shm.shared_memory import ContekSharedMemory


class SharedNumpyArray:
    """
    Wraps a numpy array so that it can be shared quickly among processes,
    avoiding unnecessary copying and (de)serializing.
    """

    def __init__(
        self,
        name=None,
        array: Optional[np.ndarray] = None,
        mode=None,
        persist: bool = False,
    ):
        """
        Creates the shared memory and copies the array therein
        """
        # create the shared memory location of the same size of the array
        if array is not None:
            meta_size = len(self.create_meta(array))
            data_size = array.nbytes
            size = 8 + meta_size + data_size
        else:
            size = 0

        self._shared = ContekSharedMemory(name, create=array is not None, size=size, mode=mode, persist=persist)

        if array is not None:
            self._dtype, self._shape = array.dtype, array.shape
            self.write(array)
        self.name = self._shared.name

    @staticmethod
    def create_meta(array) -> bytes:
        return pickle.dumps((array.dtype, array.shape))

    @staticmethod
    def load_meta(bs):
        return pickle.loads(bs)

    def write(self, array: np.ndarray):
        # save data type and shape, necessary to read the data correctly
        meta = self.create_meta(array)
        meta_size = len(meta)
        meta_size_bytes = meta_size.to_bytes(8, byteorder="little", signed=False)
        self._shared.buf[:8] = meta_size_bytes
        self._shared.buf[8 : 8 + meta_size] = meta
        # create a new numpy array that uses the shared memory we created.
        # at first, it is filled with zeros
        res = np.ndarray(self._shape, dtype=self._dtype, buffer=self._shared.buf[8 + meta_size :])

        # copy data from the array to the shared memory. numpy will
        # take care of copying everything in the correct format
        res[:] = array[:]

    def read(self):
        """
        Reads the array from the shared memory without unnecessary copying.
        """
        meta_size = int.from_bytes(self._shared.buf[:8], byteorder="little", signed=False)
        meta = self._shared.buf[8 : 8 + meta_size]

        self._dtype, self._shape = self.load_meta(meta)

        # simply create an array of the correct shape and type,
        # using the shared memory location we created earlier
        return np.ndarray(self._shape, self._dtype, buffer=self._shared.buf[8 + meta_size :])

    def copy(self):
        """
        Returns a new copy of the array stored in shared memory.
        """
        return np.copy(self.read())

    def unlink(self):
        """
        Releases the allocated memory. Call when finished using the data,
        or when the data was copied somewhere else.
        """
        self._shared.unlink()


if __name__ == "__main__":
    try:
        SharedNumpyArray("aaa")
    except FileNotFoundError:
        print("not found")
    c = SharedNumpyArray("aaa", np.zeros(10))
    d = SharedNumpyArray("aaa", mode=0o400)
    print(d.read())
    # c.unlink()
