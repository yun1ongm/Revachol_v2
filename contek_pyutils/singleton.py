import abc
import inspect
import os
import pickle as pkl
import weakref
from abc import ABC
from pathlib import Path
from typing import Type, TypeVar
from weakref import WeakValueDictionary

__all__ = ["Singleton", "Memoized"]


class DumpMixIn(ABC):
    _instance: None

    def load_instance(self, file: Path):
        with open(file, "rb") as f:
            self._instance = pkl.load(f)

    def dump_instance(self, file: Path):
        with open(file, "wb") as f:
            pkl.dump(self._instance, f)


T = TypeVar("T")


class Singleton(type, DumpMixIn):
    _instance = None

    def __call__(cls: Type[T], *args, **kwargs) -> T:
        if cls._instance is None:
            instance = super(Singleton, cls).__call__(*args, **kwargs)
            use_weakref = getattr(cls, "_SINGLETON_WEAKREF", None)
            env_var = f"{cls.__name__}_WEAKREF"
            if use_weakref is None:
                use_weakref = os.getenv(env_var)
                use_weakref = True if use_weakref == "1" else False
            if use_weakref:
                cls._instance = weakref.ref(instance)
            else:
                cls._instance = instance
        return cls._instance

    def clear_instance(cls):
        cls._instance = None

    def is_init(cls):
        return cls._instance is not None


class Memoized(type, DumpMixIn):
    _instance = None

    # depends on ordered dictionary
    def __call__(cls: Type[T], *args, **kwargs) -> T:
        args_name = inspect.getfullargspec(cls.__init__).args[1]
        memoized_value = args[0] if args else kwargs[args_name]
        if cls._instance is None:
            cls._initialize()
        instance = cls._instance.get(memoized_value) if memoized_value is not None else None
        if instance is None:
            try:
                instance = super().__call__(*args, **kwargs)
            except TypeError as e:
                raise TypeError(f"Failed with args {args}, kwargs {kwargs}") from e
            cls._instance[memoized_value] = instance
        return instance

    def _initialize(cls):
        use_weakref = getattr(cls, "_MEMOIZED_WEAKREF", None)
        env_var = f"{cls.__name__}_WEAKREF"
        if use_weakref is None:
            use_weakref = os.getenv(env_var)
            use_weakref = True if use_weakref == "1" else False
        if use_weakref:
            cls._instance = WeakValueDictionary()
        else:
            cls._instance = {}

    def clear_instances(cls, key=None):
        if cls._instance is not None:
            if key is None:
                cls._instance.clear()
            else:
                cls._instance.pop(key, None)

    def is_init(cls, key):
        if cls._instance is not None:
            return key in cls._instance
        else:
            return False


class SingletonABC(Singleton, abc.ABCMeta):
    pass


class MemoizedABC(Memoized, abc.ABCMeta):
    pass


if __name__ == "__main__":

    class A(metaclass=Memoized):
        def __init__(self, x):
            self.x = x

    class B(metaclass=Memoized):
        def __init__(self, x):
            self.x = x

    assert id(A(1)) == id(A(1))
    assert id(B(x=1)) == id(B(x=1))
    assert id(A(2)) != id(A(1))
    assert id(B(x=2)) != id(B(x=1))

    class C(metaclass=Singleton):
        def __init__(self, x):
            self.x = x

    class D(metaclass=Singleton):
        def __init__(self, x):
            self.x = x

    c1 = C(1)
    assert id(c1) == id(C(2))
    assert id(c1) != id(D(1))
    assert id(c1) != id(D(1))
    assert id(C()) == id(c1)
