from __future__ import annotations

import numbers
from typing import Mapping, Sequence

import numpy as np
import pandas as pd


class Contek2DArray(np.ndarray):
    def __new__(cls, columns: Mapping | Sequence, a: np.ndarray):
        obj = np.asarray(a).view(cls)
        if len(obj.shape) != 2:
            raise ValueError(f"We need a 2D Array not {len(obj.shape)}D")
        cls.set_column(obj, columns)
        return obj

    @property
    def columns(self):
        return self.__column

    @property
    def list_columns(self):
        return [key for (key, idx) in sorted(list(self.__column.items()), key=lambda xy: xy[1])]

    def rename(self, names: dict):
        self.__column: dict = {names.get(name, name): idx for name, idx in self.__column.items()}

    def __enable_column_name(f):
        def g(self, item, *args, **kwargs):
            if (
                self.__column is not None
                and isinstance(item, tuple)
                and len(item) == 2
                and len(self.shape) == 2
                and len(self.__column) == self.shape[1]
            ):
                column_idx = item[1]
                if isinstance(column_idx, (np.ndarray, list)):
                    for i in range(len(column_idx)):
                        idx = column_idx[i]
                        if not isinstance(idx, (numbers.Number, bool)):
                            column_idx[i] = self.__column[idx]
                elif isinstance(column_idx, numbers.Number):
                    pass
                elif column_idx is Ellipsis:
                    pass
                elif isinstance(column_idx, slice):
                    if not isinstance(column_idx.start, numbers.Number) and column_idx.start is not None:
                        column_idx = slice(
                            self.__column[column_idx.start],
                            column_idx.stop,
                            column_idx.step,
                        )
                    if not isinstance(column_idx.stop, numbers.Number) and column_idx.stop is not None:
                        column_idx = slice(
                            column_idx.start,
                            self.__column[column_idx.stop],
                            column_idx.step,
                        )
                else:
                    column_idx = self.__column[column_idx]
                item = (item[0], column_idx)
            return f(self, item, *args, **kwargs)

        return g

    def __array_finalize__(self, obj):
        # From an explicit constructor
        if obj is None:
            return
        self.__column = getattr(obj, f"_{self.__class__.__name__}__column", None)

    def set_column(self, columns: Mapping | Sequence):
        if len(columns) != self.shape[1]:
            raise ValueError(f"Missing column required {self.shape[1]}, but {len(columns)}")
        if isinstance(columns, Sequence):
            columns = {k: idx for idx, k in enumerate(columns)}
        elif not isinstance(columns, Mapping):
            raise ValueError(f"Invalid column type {columns}")
        self.__column = columns

    __getitem__ = __enable_column_name(np.ndarray.__getitem__)
    __setitem__ = __enable_column_name(np.ndarray.__setitem__)

    def __setstate__(self, state):
        self.__column = state[-1]  # Set the info attribute
        # Call the parent's __setstate__ with the other tuple elements.
        super(Contek2DArray, self).__setstate__(state[0:-1])

    def __reduce__(self):
        # Get the parent's __reduce__ tuple
        pickled_state = super(Contek2DArray, self).__reduce__()
        # Create our own tuple to pass to __setstate__
        new_state = pickled_state[2] + (self.__column,)
        # Return a tuple that replaces the parent's __setstate__ tuple with our own
        return pickled_state[0], pickled_state[1], new_state

    def to_df(self) -> pd.DataFrame:
        return pd.DataFrame(self, columns=self.list_columns)

    @classmethod
    def from_df(cls, df: pd.DataFrame) -> Contek2DArray:
        return cls(list(df.columns), df.to_numpy())


# if __name__ == "__main__":
#     arr = np.array([[1, 2, 3], [4, 5, 6]])
#     y = Contek2DArray(["a", "b", "c"], np.array([[1, 2, 3], [4, 5, 6]]))
#
#     x = Contek2DArray({"a": 1, "b": 0, "c": 2}, np.array([[1, 2, 3], [4, 5, 6]]))
#     df = x.to_df()
#     print(df)
#     print(x[0, ["a", "b"]])
#     print(x[0, 1])
#     print(x[0, ...])
#     print(x[0, 1:])
#     print(x[0, "a":])
#     print(x[0, "c"])
#     x[0, ["a", "b"]] = 0
#     print(x)
#     x[0, "c"] = 10
#     print(x)
#     x[0, ...] = 100
#     print(x)
#     x[0, :] = 100222
#     print(np.isinf(x))
#     print(x)
#     x.rename({"a": "xxx"})
#     print(x[0, "xxx"])
#     from contek_op_lib.ts_change import TsChange
#
#     op = TsChange({"ii_size": 3})
#     y = op.generate_checked(0, input0=x)
#     print(type(y))
#
#     print("__________________________")
#     z = np.clip(y, 0, 10)
#     print(z.__dict__)
