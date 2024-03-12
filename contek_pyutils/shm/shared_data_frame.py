import pickle
from typing import Optional

import pandas as pd

from contek_pyutils.shm.shared_memory import ContekSharedMemory
from contek_pyutils.shm.shared_numpy_array import SharedNumpyArray


class SharedPandasDataFrame:
    """
    Wraps a pandas dataframe so that it can be shared quickly among processes,
    avoiding unnecessary copying and (de)serializing.
    """

    def __init__(self, name=None, df: Optional[pd.DataFrame] = None, mode=None, persist=False):
        """
        Creates the shared memory and copies the dataframe therein
        """
        if df is not None:
            self._values = SharedNumpyArray(name, df.values, mode)
            self._index = df.index
            self._columns = df.columns
            inc_bytes = self.inc_to_bytes(df)
            self._inc_shm = ContekSharedMemory(
                self.index_and_columns_file_name,
                create=True,
                size=len(inc_bytes),
                mode=mode,
                persist=persist,
            )
            self._inc_shm.buf[:] = inc_bytes
        else:
            self._values = SharedNumpyArray(name, mode=mode)
            self._inc_shm = ContekSharedMemory(self.index_and_columns_file_name, mode=mode, persist=persist)
            self._index, self._columns = pickle.loads(self._inc_shm.buf.tobytes())

    @property
    def name(self):
        return self._values.name

    @staticmethod
    def inc_to_bytes(df):
        return pickle.dumps((df.index.to_numpy(), df.columns.to_numpy()))

    @property
    def index_and_columns_file_name(self):
        return f"{self.name}_inc"

    def read(self):
        """
        Reads the dataframe from the shared memory
        without unnecessary copying.
        """
        return pd.DataFrame(self._values.read(), index=self._index, columns=self._columns)

    def copy(self):
        """
        Returns a new copy of the dataframe stored in shared memory.
        """
        return pd.DataFrame(self._values.copy(), index=self._index, columns=self._columns)

    def unlink(self):
        """
        Releases the allocated memory. Call when finished using the data,
        or when the data was copied somewhere else.
        """
        self._values.unlink()
        self._inc_shm.unlink()


if __name__ == "__main__":
    index = pd.date_range("1/1/2000", periods=4, freq="T")
    series = pd.Series([0.0, None, 2.0, 3.0], index=index)
    df = pd.DataFrame({"s": series})
    try:
        SharedPandasDataFrame("bbb")
    except FileNotFoundError:
        print("not found")
    c = SharedPandasDataFrame("bbb", df)
    d = SharedPandasDataFrame("bbb", mode=0o400)
    print(d.read())
    c.unlink()
